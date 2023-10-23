# s3-archiver
Very fast files archive in Amazon S3 to zip in parallel

# License

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![NPM](https://badge.fury.io/js/@suhendra%2Fs3-zip.svg)](https://www.npmjs.com/package/@suhendra/s3-zip)

### Sample Command
```
ACCESS_KEY=<s3-access-key>
SECRET_KEY=<s3-secret>
REGION=<s3-region>
MANIFEST_BUCKET=<s3-bucket>
FILE_BUCKET=<s3-bucket>
MANIFEST="$PWD/manifest.json"
PATTERNS='["requests/2021/07/31"]' 
node script.js
```

### What This Script Does

1. Download remote AWS S3's `manifest.json` file, or use local's based on env variable `USE_LOCAL_MANIFEST`
2. Download the child manifest files locally, or use local's based on env variable `USE_LOCAL_MANIFEST`
3. Iterate through all manifest files to look for matches according to the patterns in env variable `PATTERNS`
4. Iterate through patterns, then through it's matches
5. Download the matches locally then pipe it to zip file
6. Upload the zip file to S3
7. Remove the archived keys in S3, then remove the locally downloaded files

### Env Variables

| Variable | Description |
| --- | --- |
| `ACCESS_KEY` | AWS S3 access key |
| `SECRET_KEY` | AWS S3 secret key |
| `REGION` | AWS S3 region |
| `MANIFEST_BUCKET` | AWS S3 bucket of the manifest files |
| `FILE_BUCKET` | AWS S3 bucket of the files that will be archived  |
| `MANIFEST` | Manifest file name to be downloaded, or looked up locally  |
| `USE_LOCAL_MANIFEST` | If `true`, then will use local manifest file without downloading. Default to `false`  |
| `PATTERNS` | Regex patterns to match the files that will be archived, use JSON syntax, e.g `["a", "b"]`  |
