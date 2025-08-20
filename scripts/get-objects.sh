
$BUCKET=$1
$OBJECT_KEY=$2
$DESTINATION_PATH=$3

# example: 
# - wrangler r2 object get moretransfer-dev/2025-08-20/tr_123/test-file-1.txt --file=./.downloads/test-file-1.txt
wrangler r2 object get ${BUCKET}/$OBJECT_KEY --file $DESTINATION_PATH --env dev

