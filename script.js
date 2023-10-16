const { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectsCommand, StorageClass } = require("@aws-sdk/client-s3");
const { mkdirSync, existsSync, createReadStream, createWriteStream, rmSync } = require("fs");
const archiver = require("archiver");
const zlib = require("zlib");
const csv = require("csv-parser");

// Set your AWS region and credentials
const region = process.env.REGION;
const accessKeyId = process.env.ACCESS_KEY;
const secretAccessKey = process.env.SECRET_KEY;

// Set the S3 buckets, manifest file, pattern, and dry run flag
const manifestBucketName = process.env.MANIFEST_BUCKET;
const fileBucketName = process.env.FILE_BUCKET;
const manifestName = process.env.MANIFEST;
const patternsStr = JSON.parse(process.env.PATTERNS);
const patterns = patternsStr.map(p => new RegExp(p));
const dryRun = process.env.DRY_RUN !== "false";
const useLocalManifest = process.env.USE_LOCAL_MANIFEST === "true";

function now() {
  return new Date().toDateString() + ' ' + new Date().toTimeString()
}

async function processManifest() {
  try {
    // Initialize the S3 clients
    const s3Client = new S3Client({ region, credentials: { accessKeyId, secretAccessKey } });

    // Step 1: Get the S3 inventory manifest
    let manifest;

    if (useLocalManifest) {
      console.log(`[${now()}] Using local manifest: ${manifestName}`);

      manifest = require(`${manifestName}`).files;
    } else {
      console.log(`[${now()}] Downloading manifest: ${manifestName}`);

      const getManifestParams = { Bucket: manifestBucketName, Key: manifestName };
      const manifestData = await s3Client.send(new GetObjectCommand(getManifestParams));
      manifest = JSON.parse(manifestData.Body.toString());
    }

    for (const { key: manifestKey } of manifest) {
      const manifestFileParts = manifestKey.split('/');
      const manifestFileName = manifestFileParts[manifestFileParts.length - 1].replace('.gz', '');

      if (!existsSync(manifestFileName)) {
        console.log(`[${now()}] Local manifest file not found, downloading to ${manifestFileName}`);

        const getManifestFileParams = { Bucket: manifestBucketName, Key: manifestKey };
        const manifestFileDataGz = await s3Client.send(new GetObjectCommand(getManifestFileParams));

        await new Promise((resolve, reject) => {
          const manifestFileWrite = createWriteStream(manifestFileName, { flags: 'a' })
            .on('error', (err) => reject(err))
            .on('finish', () => resolve());

          manifestFileDataGz.Body
            .pipe(zlib.createGunzip())
            .pipe(manifestFileWrite)
        })
      }
    }

    let archives = [];
    let archivePromises = [];
    let csvPromises = [];
    let archivedKeys = {};
    let processedCount = 0;

    // Make sure directory for local download exists
    for (let patternStr of patternsStr) {
      let dir = patternStr.replace(/\//g, '_');

      if (!existsSync(dir)) {
        mkdirSync(dir);
      }

      if (!archivedKeys.hasOwnProperty(patternStr)) {
        archivedKeys[patternStr] = []
      }

      console.log(`[${now()}] Pattern to match: ${patternStr}`);
    }

    // Step 2: Process each file in the manifest
    for (const { key: manifestKey } of manifest) {
      const manifestFileParts = manifestKey.split('/');
      const manifestFileName = manifestFileParts[manifestFileParts.length - 1].replace('.gz', '');

      csvPromises.push(new Promise(async (resolve) => {
        console.log(`[${now()}] Processing manifest file: ${manifestKey}`);

        createReadStream(manifestFileName, {encoding: 'UTF-8'})
          .pipe(csv())
          .on("data", async (data) => {
            for (let idx in patternsStr) {
              const patternStr = patternsStr[idx];
              const pattern = patterns[idx];

              const keys = Object.keys(data);
              const fileKey = data[keys[2]];

              if (pattern.test(fileKey)) {
                if (dryRun) {
                  console.log(`[${now()}][DryRun]. Pattern matched in file: ` + fileKey);
                  return;
                }

                archivedKeys[patternStr].push({Key: fileKey});
              }
            }
          })
          .on('end', () => {
            console.log(`[${now()}] All data in csv read: ` + manifestFileName);

            resolve();
          });
      }));
    }

    await Promise.all(csvPromises);

    for (const patternStr of Object.keys(archivedKeys)) {
      const dir = patternStr.replace(/\//g, '_');
      const archiveFile = `${dir}/output-${patternStr.replace(/\//g, '_')}.zip`;
      const archive = archiver("zip", {zlib: { level: 9 }});
      const archiveStream = createWriteStream(archiveFile, { flags: "a", highWaterMark: 1024 * 1024 });

      archive.pipe(archiveStream);
      archive.on('progress', (data) => {
        processedCount = data.entries.processed;

        console.log(`[${now()}][${patternStr}] Archive progress: ` + processedCount + '/' + archivedKeys[patternStr].length);
      });
      archive.on('finish', () => {
        console.log(`[${now()}][${patternStr}] Total archived: "` + patternStr + '" - ' + processedCount + '/' + archivedKeys[patternStr].length);

        if (processedCount !== archivedKeys[patternStr].length) {
          throw new Error('Mismatch processed archived vs planned: ' + processedCount + ' vs ' + archivedKeys[patternStr].length);
        }
      });

      archives.push({
        archive: archive,
        archiveFile: archiveFile,
        patternStr: patternStr
      });

      console.log(patternStr, archivedKeys[patternStr]);
      console.log(`[${now()}][${patternStr}] Processing matched results: ` + archivedKeys[patternStr].length);

      for (const key of archivedKeys[patternStr]) {
        const fileNameParts = key.Key.split('/');
        const fileName = fileNameParts[fileNameParts.length - 1];

        if (!existsSync(dir + '/' + fileName)) {
          const getFileParams = { Bucket: fileBucketName, Key: key.Key };
          const fileData = await s3Client.send(new GetObjectCommand(getFileParams));

          await new Promise((resolve, reject) => {
            const fileWrite = createWriteStream(dir + '/' + fileName, { flags: 'a' })
              .on('error', (err) => reject(err))
              .on('finish', () => resolve());

            fileData.Body.pipe(fileWrite)
          })
        }

        archivePromises.push(new Promise((resolve) => {
          archive.file(dir + '/' + fileName, { name: fileName });

          resolve();
        }));
      }
    }

    await Promise.all(archivePromises);

    for (const { archive } of archives) {
      archive.finalize();
    }

    console.log(`[${now()}] Manifest processing completed.`);

    for (const { archiveFile, patternStr } of archives) {
      const archiveKey = patternStr + '.zip';
      const archiveReadStream = createReadStream(archiveFile);
      const putObjectParams = {
        Bucket: manifestBucketName,
        Key: archiveKey,
        Body: archiveReadStream,
        StorageClass: StorageClass.GLACIER
      };

      if (archivedKeys[patternStr].length === 0) {
        console.log(`[${now()}][${patternStr}] No archived keys. Exiting..`);
        return;
      }

      // Step 6: Stream the archive into AWS S3
      console.log(`[${now()}][${patternStr}] Uploading archive ${archiveFile} to ${manifestBucketName}/${archiveKey}`);

      try {
        if (dryRun) {
          console.log(`[${now()}][${patternStr}] [DryRun] Uploading archive.`, putObjectParams);
        } else {
          await s3Client.send(new PutObjectCommand(putObjectParams));
        }

        console.log(`[${now()}][${patternStr}] Archive uploaded successfully for ${archiveFile}`);
      } catch (err) {
        console.error(`[${now()}][${patternStr}] Error uploading archive:`, err);

        throw err;
      }
    }

    for (const patternStr of Object.keys(archivedKeys)) {
      // Step 7: Delete the archived objects
      console.log(`[${now()}][${patternStr}] Deleting ${archivedKeys[patternStr].length} archived keys from ${fileBucketName}`);

      const size = 1000;
      const chunkedKeys = [];

      for (let i = 0; i < archivedKeys[patternStr].length; i += size) {
        chunkedKeys.push(archivedKeys[patternStr].slice(i, i + size));
      }

      for (const keys of chunkedKeys) {
        const deleteObjectsParams = {
          Bucket: fileBucketName,
          Delete: {
            Objects: keys
          }
        };

        try {
          if (dryRun) {
            console.log(`[${now()}][${patternStr}][DryRun] Deleting archived keys.`, deleteObjectsParams);
          } else {
            await s3Client.send(new DeleteObjectsCommand(deleteObjectsParams));

            console.log(`[${now()}][${patternStr}] Deleted ${keys.length} keys`);
          }
        } catch (err) {
          console.error(`[${now()}][${patternStr}] Error deleting archived keys:`, err);

          throw err;
        }

        console.log(`[${now()}][${patternStr}] Deleting local files`);

        for (const key of archivedKeys) {
          const fileNameParts = key.Key.split('/');
          const fileName = fileNameParts[fileNameParts.length - 1];

          try {
            rmSync(dir + '/' + fileName);
          } catch (err) {
            // console.error('Failed to delete from local dir: ' + dir + '/' + fileName, err)
          }
        }
      }

    }
  } catch (error) {
    console.error("Error processing manifest:", error);

    throw error;
  }
}

// Run the script
processManifest();
