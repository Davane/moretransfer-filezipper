curl -s -X POST "http://127.0.0.1:8787/compress-files" \
  -H "content-type: application/json" \
  -d '{"transferId":"tr_12345", "objectPrefix":"2025-08-20/tr_123","createdBy":"test-user"}'