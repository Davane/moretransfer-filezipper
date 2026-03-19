export interface Env {
  SOURCE_BUCKET: R2Bucket;
  OUTPUT_BUCKET: R2Bucket;

  // Queues
  QUEUE_WORKER_MAIN: Queue;

  // Durable Object
  ZipLocks: DurableObjectNamespace;

  // Environment variables
  SECRET_KEY: string;
  WEB_API_BASE_URL: string;
  ZIP_OUTPUT_PREFIX: string;
  ZIP_OUTPUT_FILE_NAME: string;
  MAX_FILES?: string;
  MAX_ZIP_BYTES?: string;
  BASE_RETRY_DELAY_SECONDS: number;
  SKIP_REQUEST_VERIFICATION: boolean | undefined;
}

export enum RequestPath {
  COMPRESS_FILES = "/compress-files",
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
}

interface ZipMessage {
  type: QueueMessageType.ZIP;
  data: ZipJob;
}

export type QueueMessage = ZipMessage;
