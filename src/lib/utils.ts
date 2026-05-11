import { Checkpoint, ErrorKind } from "./types/types";

export function slugify(text: string) {
  return text
    .toString()
    .normalize("NFD") // split accented letters into base + accent
    .replace(/[\u0300-\u036f]/g, "") // remove accents
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-") // replace non-alphanumeric with dashes
    .replace(/^-+|-+$/g, ""); // remove leading/trailing dashes
}

export function createSafeUploadKey(transferId: string, filename: string, rootPrefix = "uploads") {
  const ext = filename.includes(".") ? filename.split(".").pop() : "";
  const date = new Date().toISOString().slice(0, 10);

  return `${rootPrefix}/${date}/${transferId}/${crypto.randomUUID()}${ext ? "." + ext : ""}`;
}

export async function fetchWithCredentials<T>(endpoint: string, options: RequestInit = {}) {
  const res = await fetch(endpoint, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  if (!res.ok) {
    const error = await res.text();
    throw new Error(`Failed to fetch: ${res.status} ${error}`);
  }

  return res.json() as T;
}

export function calculateExponentialBackoff(attempts: number, baseDelaySeconds: number) {
  return baseDelaySeconds ** attempts;
}

export function resolveNameInZip(key: string, objectPrefix: string, relativePath?: string): string {
  if (relativePath) {
    return relativePath;
  }

  // Remove leading and trailing slashes from the object prefix
  const currentObjectPrefix = objectPrefix.replace(/^\/+/, "").replace(/\/?$/, "/");
  const delimiterIndex = key.indexOf("__");
  const uploadedFileName = delimiterIndex >= 0 ? key.slice(delimiterIndex + 2) : "";

  return (
    uploadedFileName ||
    key.substring(currentObjectPrefix.length).replace(/^\/+/, "") ||
    key.split("/").pop() ||
    "file"
  );
}


// ------------------------------------------------------------------------------
// Zip v2 helper functions
// ------------------------------------------------------------------------------

export function jsonResponse(obj: unknown, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

export function nowMs() {
  return Date.now();
}

// bigint helpers intentionally omitted in v1 (we persist offsets as decimal strings).

export function checkpointSummary(cp: Checkpoint) {
  return {
    manifestKey: cp.manifestKey,
    outputKey: cp.outputKey,
    uploadId: cp.uploadId ? `${cp.uploadId.slice(0, 8)}...` : undefined,
    partSize: cp.partSize,
    nextPartNumber: cp.nextPartNumber,
    fileIndex: cp.fileIndex,
    zipOffset: cp.zipOffset,
    bytesWrittenTotal: cp.bytesWrittenTotal,
    filesDone: cp.filesDone,
    done: cp.done,
  };
}

export function clampInt(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n));
}

export function jitterMs(maxJitterMs = 1000) {
  return Math.floor(Math.random() * maxJitterMs);
}

export function computeBackoffSeconds(consecutiveFailures: number, baseSeconds: number) {
  // Exponential backoff with a reasonable cap.
  const exp = Math.pow(2, clampInt(consecutiveFailures - 1, 0, 10));
  return clampInt(Math.floor(baseSeconds * exp), baseSeconds, 10 * 60);
}

export function classifyContainerFailure(
  status: number,
  errText: string,
): { retryable: boolean; kind: ErrorKind } {
  const s = errText.toLowerCase();
  const isInvalidManifest =
    s.includes("manifest fetch failed") ||
    s.includes("entries fetch failed") ||
    s.includes("bad json");

  if (isInvalidManifest) {
    return { retryable: false, kind: "bad_manifest" };
  }

  if (s.includes("context deadline exceeded") || s.includes("timeout")) {
    return { retryable: true, kind: "container_timeout" };
  }

  const isTransient =
    s.includes("eof") || s.includes("connection reset") || s.includes("broken pipe");
  if (isTransient) {
    return { retryable: true, kind: "r2_eof" };
  }

  if (status === 429) {
    return { retryable: true, kind: "container_429" };
  }

  if (status >= 500) {
    return { retryable: true, kind: "container_5xx" };
  }

  if (status >= 400) {
    return { retryable: false, kind: "container_4xx" };
  }

  return { retryable: true, kind: "unknown" };
}

export function classifyFinalizeFailure(errText: string): { retryable: boolean; kind: ErrorKind } {
  // Finalization errors are usually transient storage/network issues; be conservative.
  const s = errText.toLowerCase();
  if (s.includes("missing multipart parts")) {
    return { retryable: false, kind: "unknown" };
  }
  // R2/S3 logical validation: retries will not fix mismatched part sizes or invalid assemblies.
  if (
    s.includes("same length") ||
    s.includes("non-trailing parts") ||
    s.includes("10048") ||
    s.includes("entitytoosmall") ||
    s.includes("invalidpart") ||
    s.includes("bad digest") ||
    s.includes("invalidpartorder")
  ) {
    return { retryable: false, kind: "unknown" };
  }
  if (s.includes("eof") || s.includes("connection reset") || s.includes("broken pipe")) {
    return { retryable: true, kind: "r2_eof" };
  }
  if (s.includes("timeout") || s.includes("deadline")) {
    return { retryable: true, kind: "container_timeout" };
  }
  if (s.includes("400") || s.includes("403") || s.includes("404")) {
    return { retryable: false, kind: "container_4xx" };
  }
  if (s.includes("429")) {
    return { retryable: true, kind: "container_429" };
  }
  return { retryable: true, kind: "r2_transient" };
}