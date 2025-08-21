export enum RequestPath {
  COMPRESS_FILES = "/compress-files"
}

// Payload the producer will enqueue
export interface ZipJob {
  objectPrefix: string; // R2 prefix to collect
  zipOutputKey?: string; // optional custom output key in OUTPUT_BUCKET
  includeEmpty?: boolean; // include zero-byte files (default true)
  createdBy?: string; // optional audit
};
