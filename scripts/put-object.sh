#!/usr/bin/env sh
# Put objects into R2 for testing.
# Use --local to avoid the wrangler remote "cacheExpiry" type error and to use the same
# local R2 storage as `wrangler dev`, so the worker can see these files.
BUCKET="$1"
UPLOAD_KEY="$2"
FILE_PATH="$3"

# example (remote): wrangler r2 object put moretransfer-dev/2026-02-01/tr_361d1b7c0/8b19__test-file-1.txt --file=./resources/test-file-1.txt
# example (local):  wrangler r2 object put moretransfer-dev/2026-02-01/tr_361d1b7c0/8b19__test-file-2.txt --file=./resources/test-file-2.txt --local
wrangler r2 object put "${BUCKET}/${UPLOAD_KEY}" --file="$FILE_PATH" --local
