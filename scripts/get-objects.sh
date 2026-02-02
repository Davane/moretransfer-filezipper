#!/usr/bin/env sh
BUCKET="$1"
OBJECT_KEY="$2"
DESTINATION_PATH="$3"

# example: 
# - wrangler r2 object get  moretransfer-dev/2026-02-01/tr_361d1b7c0/8b19__test-file-1.txt --file=./.downloads/bundle.zip
# - wrangler r2 object get moretransfer-dev/bundle/2025-08-20/tr_361d1b7c-f36b-4f75-8b19-54c8ffa3b160/moretransfer_bundle.zip --file=./.downloads/bundle.zip
wrangler r2 object get ${BUCKET}/$OBJECT_KEY --file $DESTINATION_PATH --env dev

