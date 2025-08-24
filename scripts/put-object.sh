# Put them in your local SOURCE bucket under a shared prefix
$BUCKET=$1
$UPLOAD_KEY=$2
$FILE_PATH=$3

# example: 
# - wrangler r2 object put moretransfer-dev/2025-08-20/tr_361d1b7c-f36b-4f75-8b19-54c8ffa3b160/8b19-54c8ffa3b160__test-file-1.txt --file=./resources/test-file-1.txt
wrangler r2 object put ${BUCKET}/$UPLOAD_KEY --file=$FILE_PATH --env dev
