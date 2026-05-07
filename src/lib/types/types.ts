export interface Env {
  SOURCE_BUCKET: R2Bucket;
  OUTPUT_BUCKET: R2Bucket;

  // Queues
  QUEUE_WORKER_MAIN: Queue;

  // Durable Object
  ZipLocks: DurableObjectNamespace;
  JobManager: DurableObjectNamespace;
  ZipSemaphore: DurableObjectNamespace;
  ZipContainer: DurableObjectNamespace;

  // Environment variables
  SECRET_KEY: string;
  WEB_API_BASE_URL: string;
  ZIP_OUTPUT_PREFIX: string;
  ZIP_OUTPUT_FILE_NAME: string;
  MAX_FILES?: string;
  MAX_ZIP_BYTES?: string;
  BASE_RETRY_DELAY_SECONDS: number;
  SKIP_REQUEST_VERIFICATION: boolean | undefined;

  // ZIP v2 (containers)
  ZIP_USE_CONTAINERS?: boolean | undefined;
  ZIP_GLOBAL_CONCURRENCY?: string | undefined; // default 1
  ZIP_MANIFEST_PREFIX?: string | undefined; // default "manifests"
  ZIP_PART_SIZE_BYTES?: string | undefined; // default 134217728 (128 MiB)
  ZIP_MAX_PARTS_PER_TICK?: string | undefined; // default 8

  // Cloudflare Stream
  CLOUDFLARE_ACCOUNT_ID: string;
  CLOUDFLARE_STREAM_API_TOKEN: string;
  /** Set to "false" to ingest with public playback URLs (rollback). Omit or any other value = signed URLs required. */
  STREAM_REQUIRE_SIGNED_URLS: boolean | undefined;
}

export enum RequestPath {
  COMPRESS_FILES = "/compress-files",
  STREAM_INGEST = "/stream-ingest",
}

export type RequestCredentials = "include" | "omit" | "same-origin";

export interface ZipJob {
  transferId: string;
  objectPrefix: string; // R2 prefix to collect
  zipOutputKey?: string; // optional custom output key in OUTPUT_BUCKET
  includeEmpty?: boolean; // include zero-byte files (default true)
  createdBy?: string; // optional audit
  files?: Array<{ key: string; relativePath?: string }>; // file mappings for folder structure
}

export type ZipJobManifest = {
  version: "v1";
  jobId: string;
  transferId: string;
  outputKey: string;
  createdAtMs: number;
  includeEmpty: boolean;
  files: Array<{
    key: string;
    nameInZip: string;
    size: number;
  }>;
};

export type ZipV2TickMessageData = {
  jobId: string;
};

export interface StreamIngestJob {
  transferId: string;
  fileId: string;
  r2PresignedGetUrl: string;
  /**
   * Used to set Stream `creator` and `Upload-Creator` header.
   * Optional because transfers may have null userId (e.g. deleted user).
   */
  transferUserId?: string | null;
  /**
   * ISO string of transfers.expiresAt; used to set Stream scheduledDeletion.
   */
  transferExpiresAt?: string;
  meta: {
    transferId: string;
    fileId: string;
    filename?: string;
    mimeType?: string;
  };
}

export enum TransferStatus {
  PENDING = "pending", // currently uploading or about to start
  COMPRESSING = "compressing",
  READY_BUT_COMPRESSION_FAILED = "ready_but_compression_failed",
  READY = "ready",
  FAILED = "failed",
}

export interface TransferUpdateRequest {
  status: TransferStatus,
  bundleObjectKey?: string;
}

export enum QueueMessageType {
  ZIP = "zip",
  ZIP_V2_TICK = "zip_v2_tick",
  STREAM_INGEST = "stream_ingest",
}

interface ZipMessage {
  type: QueueMessageType.ZIP;
  data: ZipJob;
}

interface ZipV2TickMessage {
  type: QueueMessageType.ZIP_V2_TICK;
  data: ZipV2TickMessageData;
}

interface StreamIngestMessage {
  type: QueueMessageType.STREAM_INGEST;
  data: StreamIngestJob;
}

export type QueueMessage = ZipMessage | ZipV2TickMessage | StreamIngestMessage;
