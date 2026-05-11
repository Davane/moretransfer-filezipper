export const COMPANY_NAME = "moretransfer";

// ------------------------------------------------------------------------------
// Zip v2 constants
// ------------------------------------------------------------------------------

export const DEFAULT_PART_SIZE = 128 * 1024 * 1024; // 128 MiB
export const ZIP_V2_VERSION = "zip64-store-container-v1";
export const DEFAULT_NUMBER_OF_PARTS = 8;
export const DEFAULT_MAX_CONSECUTIVE_FAILURES = 8;
export const DEFAULT_RETRY_BASE_DELAY_SECONDS = 10;
export const CLEANUP_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours
export const DEFAULT_TICK_INTERVAL_MS = 10_000;
