
$BUCKET=$1
$OBJECT_KEY=$2

wrangler r2 object delete $BUCKET $OBJECT_KEY  --env dev

