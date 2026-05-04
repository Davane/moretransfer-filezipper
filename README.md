# File Zipper

A Cloudflare Worker that creates ZIP archives from files stored in Cloudflare R2 buckets. This service processes ZIP creation jobs asynchronously using Cloudflare Queues and includes features like duplicate prevention, **ZIP64 store-only streaming**, and multipart uploads.

## Features

- **Asynchronous Processing**: Uses Cloudflare Queues for background ZIP creation
- **ZIP64 store-only streaming**: Custom writer emits valid ZIP64 (large files, archives over 4 GiB) with **STORE** (method 0) — no deflate; suitable for video/media. Every write is awaited through an R2 multipart sink so **backpressure** avoids Worker OOM on large jobs
- **Duplicate Prevention**: Uses Durable Objects to prevent concurrent ZIP creation for the same prefix
- **Multipart Uploads**: Fixed-size parts (default 32 MiB) to R2; non-final parts meet the 5 MiB minimum
- **Configurable Limits**: `MAX_FILES` and `MAX_ZIP_BYTES` (defaults and `wrangler.toml` support bundles up to **64 GiB**)

## Architecture

The service consists of:

- **HTTP Producer**: Accepts ZIP job requests via POST to `/compress-files`
- **Queue Consumer**: Processes ZIP jobs in the background
- **Durable Object**: Manages locks to prevent duplicate work
- **R2 Integration**: Reads from source bucket, writes bundled ZIP to output bucket

## Prerequisites

- Cloudflare account with Workers and R2 enabled
- Two R2 buckets (or one bucket with distinct prefixes)
- Wrangler CLI installed

## Configuration

### Environment Variables

Set these in your `wrangler.toml` or via the Cloudflare dashboard:

- `SOURCE_BUCKET` / `OUTPUT_BUCKET`: R2 bindings (see `wrangler.toml`)
- `ZIP_OUTPUT_PREFIX`: Prefix for generated ZIP paths (e.g. `bundle`)
- `ZIP_OUTPUT_FILE_NAME`: Base name for the output object (e.g. `moretransfer_bundle` → `moretransfer_bundle.zip`)
- `MAX_FILES`: Maximum number of files per ZIP (default in code: **1000** if unset; env often sets `500`)
- `MAX_ZIP_BYTES`: Maximum total uncompressed bytes counted toward the ZIP (default in code and env: **64 GiB** / `68719476736`)
- `BASE_RETRY_DELAY_SECONDS`: Queue retry backoff base
- `SECRET_KEY`, `WEB_API_BASE_URL`: API integration

### Required Resources

- **R2 Buckets**: Source and output bindings
- **Queue**: For job processing (`QUEUE_WORKER_MAIN`)
- **Durable Object**: For locking (`ZipLocks` → `ZipLocksDO`)

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd moretransfer-worker
```

2. Install dependencies:

```bash
npm install
```

3. Configure your `wrangler.toml` with your bucket names and settings

4. Deploy to Cloudflare:

```bash
npm run deploy
```

## Usage

### Enqueue a ZIP Job

Send a **POST** to `/compress-files` with a JSON body matching `ZipJob`:

```json
{
  "transferId": "uuid",
  "objectPrefix": "path/to/files/",
  "zipOutputKey": "optional/custom/key.zip",
  "includeEmpty": true,
  "createdBy": "api",
  "files": [{ "key": "path/obj1", "relativePath": "folder/a.mxf" }]
}
```

**Parameters:**

- `transferId` (required): Transfer id for status callbacks
- `objectPrefix` (required): R2 prefix to list and zip
- `zipOutputKey` (optional): Custom output key in the output bucket
- `includeEmpty` (optional): Include zero-byte files (default: true)
- `createdBy` (optional): Audit field; stored in ZIP object `customMetadata`
- `files` (optional): Maps R2 keys to `relativePath` inside the ZIP (folder structure)

## How It Works

1. **Job Enqueue**: HTTP handler verifies HMAC (unless skipped in dev) and sends a message to the queue
2. **Lock Acquisition**: Worker acquires a per-prefix lock using the Durable Object
3. **File Discovery**: Lists all objects under the prefix from the source bucket (paginated)
4. **ZIP64 STORE pipeline**: For each object, streams `SOURCE_BUCKET.get()` body through `Zip64StoreWriter` → `R2MultipartSink` (awaited writes, ZIP64 data descriptors + central directory + EOCD)
5. **Completion**: Multipart upload completes; transfer status is updated to `ready` with `bundleObjectKey`
6. **Lock Release**: Lock is released in a `finally` block; failures abort multipart and retry with backoff

Output objects include `customMetadata.zipVersion` = `zip64-store-v1`.

## Development

### Local development

```bash
npm run dev
# or without HMAC for local testing:
npm run dev:no-auth
```

### Typecheck

```bash
npx tsc --noEmit
```

### Project structure

```
moretransfer-worker/
├── src/
│   ├── index.ts                 # fetch / queue / scheduled / ZipLocksDO
│   ├── lib/zip64/               # ZIP64 STORE writer, CRC32, R2 multipart sink
│   └── modules/
│       └── zipper.ts            # Queue consumer ZIP job orchestration
├── package.json
├── wrangler.toml
└── README.md
```

## Dependencies

- **@cloudflare/workers-types**: TypeScript types for Cloudflare Workers
- **wrangler**: Cloudflare Workers CLI

## Limitations

- **STORE only** (no compression, encryption, or symlinks in the custom writer)
- **MAX_FILES** / **MAX_ZIP_BYTES** enforced before streaming (see env)
- ZIP creation is asynchronous (queue latency)
- Very large jobs depend on R2 throughput and Worker limits (`cpu_ms` in `wrangler.toml`); validate in staging before production ramp-up

## Validating ZIP output (staging / production)

After generating a ZIP, download it and verify:

```bash
unzip -t output.zip
ditto -x -k output.zip test-output
7zz t output.zip
```

Suggested cases: single file under 4 GiB; single file over 4 GiB; many small files; deep paths / Unicode names; empty files when `includeEmpty` is true.

## Error Handling

- Failed jobs are retried with exponential backoff
- Dead letter queue for permanently failed messages (see `wrangler.toml`)
- Lock TTL via Durable Object alarm (~2 minutes)
- Multipart uploads are aborted on failure to avoid orphaned parts

## License

ISC
