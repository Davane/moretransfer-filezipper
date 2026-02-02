#!/usr/bin/env sh
# First upload the test files to R2 (dev bucket) so the worker can zip them:
#   ./scripts/put-object.sh moretransfer-dev 2026-02-01/tr_361d1b7c0/8b19__test-file-3.txt ./resources/test-file-1.txt
#   ./scripts/put-object.sh moretransfer-dev test-demo/tr_resources/test-file-2.txt ./resources/test-file-2.txt
#   ./scripts/put-object.sh moretransfer-dev test-demo/tr_resources/test-file-3.txt ./resources/test-file-3.txt
# Then start the worker: npm run dev:no-auth
# Then run this script: ./scripts/test-enqueue.sh

curl -s -X POST "http://127.0.0.1:8787/compress-files" \
  -H "content-type: application/json" \
  -d '{
  "transferId": "tr_361d1b7c0",
  "objectPrefix": "2026-02-01/tr_361d1b7c0",
  "createdBy": "test-user",
  "files": [
    {"key": "2026-02-01/tr_361d1b7c0/8b19__test-file-1.txt", "relativePath": "my-folder/test-file-1.txt"},
    {"key": "2026-02-01/tr_361d1b7c0/8b19__test-file-2.txt", "relativePath": "my-folder/inner-folder/test-file-2.txt"},
    {"key": "2026-02-01/tr_361d1b7c0/8b19__test-file-3.txt", "relativePath": "my-folder/inner-folder/further-inner-folder/test-file-3.txt"}
  ]
}'
