# File Zipper

A Cloudflare Worker that creates ZIP archives from files stored in Cloudflare R2 buckets. This service processes ZIP creation jobs asynchronously using Cloudflare Queues and includes features like duplicate prevention, streaming compression, and multipart uploads.

## Features

- **Asynchronous Processing**: Uses Cloudflare Queues for background ZIP creation
- **Streaming Compression**: Never buffers entire files in memory using fflate
- **Duplicate Prevention**: Uses Durable Objects to prevent concurrent ZIP creation for the same prefix
- **Multipart Uploads**: Efficiently uploads large ZIPs to R2 using multipart uploads
- **Configurable Limits**: Set maximum file count and ZIP size limits
- **Manifest Generation**: Automatically includes a JSON manifest with file statistics

## Architecture

The service consists of:

- **HTTP Producer**: Accepts ZIP job requests via POST to `/enqueue-zip`
- **Queue Consumer**: Processes ZIP jobs in the background
- **Durable Object**: Manages locks to prevent duplicate work
- **R2 Integration**: Reads from source bucket, writes to output bucket

## Prerequisites

- Cloudflare account with Workers and R2 enabled
- Two R2 buckets (source and output)
- Wrangler CLI installed

## Configuration

### Environment Variables

Set these in your `wrangler.toml` or via the Cloudflare dashboard:

- `SOURCE_BUCKET`: R2 bucket containing files to zip
- `OUTPUT_BUCKET`: R2 bucket where ZIPs will be stored
- `ZIP_OUTPUT_PREFIX`: Prefix for generated ZIP files (default: "prebaked-zips/")
- `MAX_FILES`: Maximum number of files per ZIP (default: 5000)
- `MAX_ZIP_BYTES`: Maximum ZIP size in bytes (default: 500MB)

### Required Resources

- **R2 Buckets**: Source and output buckets
- **Queue**: For job processing (`zip-bundles`)
- **Durable Object**: For locking (`ZipLocksDO`)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd file-zipper
```

2. Install dependencies:
```bash
npm install
```

3. Configure your `wrangler.toml` with your bucket names and settings

4. Deploy to Cloudflare:
```bash
npm run wrangler:deploy
```

## Usage

### Enqueue a ZIP Job

Send a POST request to `/enqueue-zip` with the following JSON payload:

```json
{
  "prefix": "path/to/files/",
  "zipKey": "optional-custom-output-key.zip",
  "includeEmpty": true,
  "createdBy": "api"
}
```

**Parameters:**
- `prefix` (required): R2 prefix to collect files from
- `zipKey` (optional): Custom output key in the output bucket
- `includeEmpty` (optional): Include zero-byte files (default: true)
- `createdBy` (optional): Audit field for tracking

### Example Request

```bash
curl -X POST https://your-worker.your-subdomain.workers.dev/enqueue-zip \
  -H "Content-Type: application/json" \
  -d '{
    "prefix": "uploads/2024/01/",
    "zipKey": "january-uploads.zip",
    "createdBy": "scheduled-job"
  }'
```

## How It Works

1. **Job Enqueue**: HTTP endpoint accepts ZIP job requests and adds them to a queue
2. **Lock Acquisition**: Worker acquires a per-prefix lock using Durable Objects
3. **File Discovery**: Lists all objects with the specified prefix from source bucket
4. **Streaming ZIP Creation**: Creates ZIP using fflate's streaming API
5. **Multipart Upload**: Uploads ZIP to output bucket using multipart uploads
6. **Manifest Generation**: Adds a JSON manifest with file statistics
7. **Lock Release**: Releases the lock when complete

## Development

### Local Development

```bash
# Login to Cloudflare
npm run wrangler:login

# Deploy to Cloudflare
npm run wrangler:deploy
```

### Project Structure

```
file-zipper/
├── src/
│   └── index.ts          # Main worker code
├── package.json          # Dependencies and scripts
├── wrangler.toml         # Cloudflare configuration
└── README.md            # This file
```

## Dependencies

- **fflate**: Fast ZIP compression library
- **@cloudflare/workers-types**: TypeScript types for Cloudflare Workers
- **wrangler**: Cloudflare Workers CLI tool

## Limitations

- Maximum 5000 files per ZIP (configurable)
- Maximum 500MB ZIP size (configurable)
- Files must be in the same R2 bucket
- ZIP creation is asynchronous (not real-time)

## Error Handling

- Failed jobs are retried with exponential backoff
- Dead letter queue for permanently failed jobs
- Lock timeouts prevent stuck processes
- Comprehensive error logging

## License

ISC
