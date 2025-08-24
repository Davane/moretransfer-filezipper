
$BUCKET=$1
$OBJECT_KEY=$2
$DESTINATION_PATH=$3

# example: 
# - wrangler r2 object get bundle/2025-08-20/tr_361d1b7c-f36b-4f75-8b19-54c8ffa3b160/moretransfer_bundle.zip --file=./.downloads/bundle.zip
wrangler r2 object get ${BUCKET}/$OBJECT_KEY --file $DESTINATION_PATH --env dev

