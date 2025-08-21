
export enum RequestPath {
  COMPRESS_FILES = "/compress-files",
}

export type RequestCredentials = "include" | "omit" | "same-origin";


// Payload the producer will enqueue
export interface ZipJob {
  transferId: string;
  objectPrefix: string; // R2 prefix to collect
  zipOutputKey?: string; // optional custom output key in OUTPUT_BUCKET
  includeEmpty?: boolean; // include zero-byte files (default true)
  createdBy?: string; // optional audit
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
