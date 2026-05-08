import { Env, TransferStatus } from "../lib/types/types";
import { WebAPIService } from "./web-api-service";
import { toInt, toBool } from "./job-manifest";

type JobStatus = "PENDING" | "RUNNING" | "FINALIZING" | "DONE" | "FAILED" | "CANCELLED";

type UploadedPart = { partNumber: number; etag: string; sizeBytes: number };

type ErrorKind =
  | "container_5xx"
  | "container_429"
  | "container_4xx"
  | "container_timeout"
  | "r2_eof"
  | "r2_transient"
  | "bad_manifest"
  | "unknown";

type ZipEntryRow = {
  jobId: string;
  fileIndex: number;
  nameB64: string;
  crc32: number;
  compressedSize: string;
  uncompressedSize: string;
  localHeaderOffset: string;
  modTime: number;
  modDate: number;
};

type Checkpoint = {
  manifestKey: string;
  outputKey: string;
  uploadId?: string;
  partSize: number;
  nextPartNumber: number;
  fileIndex: number;
  zipOffset: string; // bigint as decimal string
  bytesWrittenTotal: string; // bigint as decimal string
  filesDone: number;
  done: boolean;
};

type JobStateRow = {
  jobId: string;
  transferId: string;
  status: JobStatus;
  createdAtMs: number;
  updatedAtMs: number;
  errorMessage?: string;
  consecutiveFailures: number;
  lastFailureAtMs?: number;
  nextRetryAtMs?: number;
  lastErrorKind?: ErrorKind;
  cleanupAtMs?: number;
  nextTickAtMs?: number;
};

type StartJobRequest = {
  jobId: string;
  transferId: string;
  manifestKey: string;
  outputKey: string;
};

type TickJobRequest = {
  jobId: string;
};

type TickJobResponse = {
  status: JobStatus;
  done: boolean;
  retryAfterSeconds?: number;
};

type RunChunkResponse = {
  uploadedParts: UploadedPart[];
  nextPartNumber: number;
  fileIndex: number;
  zipOffset: string;
  bytesWrittenTotal: string;
  filesDone: number;
  done: boolean;
};

const DEFAULT_PART_SIZE = 128 * 1024 * 1024; // 128 MiB
const ZIP_V2_VERSION = "zip64-store-container-v1";
const DEFAULT_NUMBER_OF_PARTS = 8;
const DEFAULT_MAX_CONSECUTIVE_FAILURES = 8;
const DEFAULT_RETRY_BASE_DELAY_SECONDS = 10;
const CLEANUP_TTL_MS = 24 * 60 * 60 * 1000;
const DEFAULT_TICK_INTERVAL_MS = 5_000;

function jsonResponse(obj: unknown, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function nowMs() {
  return Date.now();
}

// bigint helpers intentionally omitted in v1 (we persist offsets as decimal strings).

function checkpointSummary(cp: Checkpoint) {
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

async function clearAlarmIfSupported(storage: DurableObjectStorage) {
  // deleteAlarm exists in newer runtime APIs; keep best-effort compatibility.
  const anyStorage: any = storage as any;
  if (typeof anyStorage.deleteAlarm === "function") {
    await anyStorage.deleteAlarm();
  }
}

function clampInt(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n));
}

function jitterMs(maxJitterMs = 1000) {
  return Math.floor(Math.random() * maxJitterMs);
}

function computeBackoffSeconds(consecutiveFailures: number, baseSeconds: number) {
  // Exponential backoff with a reasonable cap.
  const exp = Math.pow(2, clampInt(consecutiveFailures - 1, 0, 10));
  return clampInt(Math.floor(baseSeconds * exp), baseSeconds, 10 * 60);
}

function classifyContainerFailure(
  status: number,
  errText: string,
): { retryable: boolean; kind: ErrorKind } {
  const s = errText.toLowerCase();
  if (
    s.includes("manifest fetch failed") ||
    s.includes("entries fetch failed") ||
    s.includes("bad json")
  ) {
    return { retryable: false, kind: "bad_manifest" };
  }
  if (s.includes("context deadline exceeded") || s.includes("timeout")) {
    return { retryable: true, kind: "container_timeout" };
  }
  if (s.includes("eof") || s.includes("connection reset") || s.includes("broken pipe")) {
    return { retryable: true, kind: "r2_eof" };
  }
  if (status === 429) return { retryable: true, kind: "container_429" };
  if (status >= 500) return { retryable: true, kind: "container_5xx" };
  if (status >= 400) return { retryable: false, kind: "container_4xx" };
  return { retryable: true, kind: "unknown" };
}

class ContainerRunError extends Error {
  constructor(
    public readonly status: number,
    public readonly errText: string,
  ) {
    super(`Container run failed: ${status} ${errText}`);
    this.name = "ContainerRunError";
  }
}

export class JobManagerDO {
  constructor(
    private readonly state: DurableObjectState,
    private readonly env: Env,
  ) {}

  async alarm(): Promise<void> {
    this.initIfNeeded();
    const sql = this.sql();
    const now = nowMs();

    // --------------------------------------------------------------------------
    // Cleanup: purge completed/failed job state after TTL
    // --------------------------------------------------------------------------
    const due = sql.exec(
      `SELECT jobId, status, cleanupAtMs
       FROM job_state
       WHERE cleanupAtMs IS NOT NULL
         AND cleanupAtMs <= ?
         AND status IN ('DONE','FAILED');`,
      now,
    );

    const dueRows = due.toArray?.() ?? [];
    for (const row of dueRows) {
      const r: any = row;
      const jobId = String(r.jobId);
      const status = String(r.status) as JobStatus;

      try {
        if (status === "FAILED") {
          // Best-effort abort of multipart upload (if we have uploadId/outputKey)
          const cp = this.getCheckpoint(jobId);
          if (cp?.uploadId) {
            try {
              const up = this.env.OUTPUT_BUCKET.resumeMultipartUpload(cp.outputKey, cp.uploadId);
              await up.abort();
              console.log(`[zip-v2] Cleanup aborted multipart upload.`, {
                jobId,
                outputKey: cp.outputKey,
                uploadId: `${cp.uploadId.slice(0, 8)}...`,
              });
            } catch (e) {
              console.warn(`[zip-v2] Cleanup failed to abort multipart upload (best-effort).`, {
                jobId,
                error: String((e as any)?.message ?? e),
              });
            }
          }
        }

        // Purge DO-local state
        sql.exec(`DELETE FROM uploaded_parts WHERE jobId = ?;`, jobId);
        sql.exec(`DELETE FROM zip_entries WHERE jobId = ?;`, jobId);
        sql.exec(`DELETE FROM checkpoint WHERE jobId = ?;`, jobId);
        sql.exec(`DELETE FROM job_state WHERE jobId = ?;`, jobId);

        console.log(`[zip-v2] Cleanup purged job state.`, { jobId, status });
      } catch (e) {
        console.error(`[zip-v2] Cleanup failed.`, {
          jobId,
          status,
          error: String((e as any)?.message ?? e),
        });
      }
    }

    // --------------------------------------------------------------------------
    // Tick scheduling: enqueue due ZIP_V2_TICK messages
    // --------------------------------------------------------------------------
    const dueTicks = sql.exec(
      `SELECT jobId
       FROM job_state
       WHERE cleanupAtMs IS NOT NULL;`,
    );
    const nextRow = (next.toArray?.() ?? [])[0] as any;
    const nextCleanupAtMs = nextRow?.nextCleanupAtMs as number | null | undefined;

    if (nextCleanupAtMs && Number.isFinite(nextCleanupAtMs)) {
      await this.state.storage.setAlarm(nextCleanupAtMs);
      console.log(`[zip-v2] Cleanup alarm scheduled.`, { nextCleanupAtMs });
    } else {
      await clearAlarmIfSupported(this.state.storage);
      console.log(`[zip-v2] Cleanup alarm cleared (no pending cleanups).`);
    }
  }

  async fetch(req: Request): Promise<Response> {
    this.initIfNeeded();
    const url = new URL(req.url);

    // Start the zip v2 job
    if (req.method === "POST" && url.pathname === "/start") {
      const body = await req.json();
      await this.handleStart(body as StartJobRequest);

      return jsonResponse({ ok: true });
    }

    // Handle tick requests
    if (req.method === "POST" && url.pathname === "/tick") {
      const body = await req.json();
      const jobId = (body as TickJobRequest).jobId;

      const result = await this.handleTick(jobId);

      return jsonResponse(result);
    }

    // Handle status requests to get the job status
    // if (req.method === "GET" && url.pathname === "/status") {
    //   const jobId = url.searchParams.get("jobId");
    //   if (!jobId) {
    //     return jsonResponse({ error: "missing jobId" }, 400);
    //   }

    //   const status = await this.getJobStatus(jobId);

    //   return jsonResponse(status);
    // }

    // Handle list entries requests
    if (url.pathname === "/entries" && req.method === "GET") {
      const jobId = url.searchParams.get("jobId");
      if (!jobId) {
        return jsonResponse({ error: "missing jobId" }, 400);
      }

      const entries = this.listZipEntries(jobId);

      return jsonResponse(entries);
    }

    // Handle upsert entry requests
    if (url.pathname === "/entries" && req.method === "POST") {
      const jobId = url.searchParams.get("jobId");
      const fileIndex = url.searchParams.get("fileIndex");
      if (!jobId || fileIndex === null) {
        return jsonResponse({ error: "missing jobId/fileIndex" }, 400);
      }

      const entry = (await req.json()) as Omit<ZipEntryRow, "jobId" | "fileIndex">;
      this.upsertZipEntry(jobId, Number(fileIndex), entry);

      return jsonResponse({ ok: true });
    }

    return new Response("not found", { status: 404 });
  }

  // --------------------------------------------------------------------------------
  // Handlers
  // --------------------------------------------------------------------------------

  private async handleStart(body: StartJobRequest) {
    const { jobId, transferId, manifestKey, outputKey } = body;

    const existing = this.getJobRow(jobId);
    if (existing) {
      console.log(`[zip-v2] Job already exists.`, {
        jobId,
        transferId,
        manifestKey,
        outputKey,
      });
      return;
    }

    const partSize = toInt(this.env.ZIP_PART_SIZE_BYTES, DEFAULT_PART_SIZE);

    this.upsertJob({
      jobId,
      transferId,
      status: "PENDING",
      createdAtMs: nowMs(),
      updatedAtMs: nowMs(),
      consecutiveFailures: 0,
      cleanupAtMs: undefined,
    } satisfies JobStateRow);

    this.upsertCheckpoint(jobId, {
      manifestKey,
      outputKey,
      uploadId: undefined,
      partSize,
      nextPartNumber: 1,
      fileIndex: 0,
      zipOffset: "0",
      bytesWrittenTotal: "0",
      filesDone: 0,
      done: false,
    });

    console.log(`[zip-v2] Job started.`, {
      jobId,
      transferId,
      manifestKey,
      outputKey,
    });
  }

  private async handleTick(jobId: string): Promise<TickJobResponse> {
    const tickStartMs = nowMs();
    const job = this.getJobRow(jobId);
    const checkpoint = this.getCheckpoint(jobId);
    if (!job || !checkpoint) {
      throw new Error(`No Job or Checkpoint found for job: ${jobId}`);
    }

    if (job.nextRetryAtMs && nowMs() < job.nextRetryAtMs) {
      const retryAfterSeconds = Math.max(1, Math.ceil((job.nextRetryAtMs - nowMs()) / 1000));
      console.log(`[zip-v2] Tick skipped (backoff).`, {
        jobId,
        transferId: job.transferId,
        retryAfterSeconds,
        consecutiveFailures: job.consecutiveFailures,
        lastErrorKind: job.lastErrorKind,
      });
      return { status: job.status, done: false, retryAfterSeconds };
    }

    console.log(`[zip-v2] Tick start.`, {
      jobId,
      jobStatus: job.status,
      transferId: job.transferId,
      tickStartMs,
      checkpoint: checkpointSummary(checkpoint),
    });

    if (job.status === "DONE" || job.status === "FAILED" || job.status === "CANCELLED") {
      console.log(`[zip-v2] Job already completed.`, {
        jobId,
        jobStatus: job.status,
      });
      return {
        status: job.status,
        done: job.status === "DONE",
      };
    }

    const useContainers = toBool(this.env.ZIP_USE_CONTAINERS, false);
    if (!useContainers) {
      throw new Error("ZIP v2 tick received but ZIP_USE_CONTAINERS is disabled");
    }

    const existingOut = await this.env.OUTPUT_BUCKET.head(checkpoint.outputKey);
    if (existingOut) {
      this.upsertJob({
        ...job,
        status: "DONE",
        updatedAtMs: nowMs(),
      });

      return { status: "DONE", done: true };
    }

    // Acquire the semaphore to prevent multiple jobs from running concurrently
    const semaphoreId = this.env.ZipSemaphore.idFromName("global");
    const semaphore = this.env.ZipSemaphore.get(semaphoreId);
    const desired = toInt(this.env.ZIP_GLOBAL_CONCURRENCY, 1);

    const acquireStartMs = nowMs();
    const acquiredResp = await semaphore.fetch("https://semaphore/acquire", {
      method: "POST",
      body: JSON.stringify({ jobId, limit: desired }),
    });
    const acquireDurationMs = nowMs() - acquireStartMs;

    if (!acquiredResp.ok) {
      const errText = await acquiredResp.text();
      console.error(`[zip-v2] Failed to acquire semaphore.`, {
        error: errText,
        status: acquiredResp.status,
        durationMs: acquireDurationMs,
        jobId,
        transferId: job.transferId,
        bundleObjectKey: checkpoint.outputKey,
        manifestKey: checkpoint.manifestKey,
      });

      // No token; caller should retry later.
      return {
        status: job.status,
        done: false,
      };
    }

    try {
      this.upsertJob({
        ...job,
        status: "RUNNING",
        updatedAtMs: nowMs(),
      });

      // Create a new multipart upload if it doesn't exist
      const maxParts = toInt(this.env.ZIP_MAX_PARTS_PER_TICK, DEFAULT_NUMBER_OF_PARTS);
      if (!checkpoint.uploadId) {
        const createUploadStartMs = nowMs();
        const upload = await this.env.OUTPUT_BUCKET.createMultipartUpload(checkpoint.outputKey, {
          customMetadata: {
            jobId,
            transferId: job.transferId,
            manifestKey: checkpoint.manifestKey,
            zipVersion: ZIP_V2_VERSION,
          },
        });
        const createUploadDurationMs = nowMs() - createUploadStartMs;
        checkpoint.uploadId = upload.uploadId;
        this.upsertCheckpoint(jobId, checkpoint);
        console.log(`[zip-v2] Multipart upload created.`, {
          jobId,
          transferId: job.transferId,
          outputKey: checkpoint.outputKey,
          uploadId: `${upload.uploadId.slice(0, 8)}...`,
          durationMs: createUploadDurationMs,
        });
      }

      // Run the container to upload the next parts
      const containerId = this.env.ZipContainer.idFromName(jobId);
      const containerStub = this.env.ZipContainer.get(containerId);

      // Forward the request to the container
      const runChunkStartMs = nowMs();
      const runResp = await containerStub.fetch("https://zip/runChunk", {
        method: "POST",
        body: JSON.stringify({
          jobId,
          manifestKey: checkpoint.manifestKey,
          outputKey: checkpoint.outputKey,
          uploadId: checkpoint.uploadId,
          partSize: checkpoint.partSize,
          nextPartNumber: checkpoint.nextPartNumber,
          fileIndex: checkpoint.fileIndex,
          zipOffset: checkpoint.zipOffset,
          maxParts,
        }),
      });
      const runChunkDurationMs = nowMs() - runChunkStartMs;

      if (!runResp.ok) {
        const errText = await runResp.text();
        console.error(`[zip-v2] Container run failed.`, {
          status: runResp.status,
          error: errText,
          durationMs: runChunkDurationMs,
          jobId,
          transferId: job.transferId,
          bundleObjectKey: checkpoint.outputKey,
          manifestKey: checkpoint.manifestKey,
          checkpoint: checkpointSummary(checkpoint),
        });
        throw new ContainerRunError(runResp.status, errText);
      }

      const run = (await runResp.json()) satisfies RunChunkResponse;

      console.log(`[zip-v2] runChunk ok.`, {
        jobId,
        transferId: job.transferId,
        durationMs: runChunkDurationMs,
        maxParts,
        uploadedPartsCount: run.uploadedParts?.length ?? 0,
        nextPartNumber: run.nextPartNumber,
        fileIndex: run.fileIndex,
        zipOffset: run.zipOffset,
        bytesWrittenTotal: run.bytesWrittenTotal,
        filesDone: run.filesDone,
        done: run.done,
      });

      if (job.consecutiveFailures > 0 || job.nextRetryAtMs || job.lastErrorKind) {
        this.upsertJob({
          ...job,
          consecutiveFailures: 0,
          nextRetryAtMs: undefined,
          lastFailureAtMs: undefined,
          lastErrorKind: undefined,
          updatedAtMs: nowMs(),
        });
      }

      if (run.uploadedParts?.length) {
        this.insertUploadedParts(jobId, run.uploadedParts);
      }

      this.upsertCheckpoint(jobId, {
        ...checkpoint,
        nextPartNumber: run.nextPartNumber,
        fileIndex: run.fileIndex,
        zipOffset: run.zipOffset,
        bytesWrittenTotal: run.bytesWrittenTotal,
        filesDone: run.filesDone,
        done: run.done,
      } satisfies Checkpoint);

      if (run.done) {
        // Container should have completed multipart; verify output exists and update status.
        const verifyStartMs = nowMs();
        const out = await this.env.OUTPUT_BUCKET.head(checkpoint.outputKey);
        const verifyDurationMs = nowMs() - verifyStartMs;
        if (!out) {
          throw new Error("Container reported done but output does not exist");
        }

        const cleanupAtMs = nowMs() + CLEANUP_TTL_MS;
        this.upsertJob({ ...job, status: "DONE", updatedAtMs: nowMs(), cleanupAtMs });
        await this.state.storage.setAlarm(cleanupAtMs);
        console.log(`[zip-v2] Cleanup scheduled (done).`, {
          jobId,
          transferId: job.transferId,
          cleanupAtMs,
        });

        // Best-effort status callback
        const webAPIService = new WebAPIService(this.env.SECRET_KEY, this.env.WEB_API_BASE_URL);
        await webAPIService
          .updateTransferStatus(job.transferId, {
            status: TransferStatus.READY,
            bundleObjectKey: checkpoint.outputKey,
          })
          .catch((e) =>
            console.warn(`[zip-v2] Failed to update transfer status`, {
              error: e,
              jobId,
              transferId: job.transferId,
              bundleObjectKey: checkpoint.outputKey,
              manifestKey: checkpoint.manifestKey,
            }),
          );

        console.log(`[zip-v2] Tick done (job complete).`, {
          jobId,
          transferId: job.transferId,
          outputKey: checkpoint.outputKey,
          outputBytes: out.size,
          verifyDurationMs,
          tickDurationMs: nowMs() - tickStartMs,
        });
        return { status: "DONE", done: true };
      }

      // Not done; caller should enqueue another tick.
      console.log(`[zip-v2] Tick done (job still running).`, {
        jobId,
        transferId: job.transferId,
        tickDurationMs: nowMs() - tickStartMs,
      });
      return { status: "RUNNING", done: false };
    } catch (err: any) {
      const errMsg = String(err?.message ?? err);
      const status = err instanceof ContainerRunError ? err.status : 500;
      const rawErrText = err instanceof ContainerRunError ? err.errText : errMsg;
      const { retryable, kind } = classifyContainerFailure(status, rawErrText);

      const consecutiveFailures = (job.consecutiveFailures ?? 0) + 1;
      const maxConsecutiveFailures = toInt(
        (this.env as any).ZIP_V2_MAX_CONSECUTIVE_FAILURES,
        DEFAULT_MAX_CONSECUTIVE_FAILURES,
      );
      const baseDelaySeconds = toInt(
        (this.env as any).ZIP_V2_RETRY_BASE_DELAY_SECONDS,
        DEFAULT_RETRY_BASE_DELAY_SECONDS,
      );

      if (retryable && consecutiveFailures < maxConsecutiveFailures) {
        const backoffSeconds = computeBackoffSeconds(consecutiveFailures, baseDelaySeconds);
        const nextRetryAtMs = nowMs() + backoffSeconds * 1000 + jitterMs(1000);
        this.upsertJob({
          ...job,
          status: "RUNNING",
          updatedAtMs: nowMs(),
          errorMessage: rawErrText,
          consecutiveFailures,
          lastFailureAtMs: nowMs(),
          nextRetryAtMs,
          lastErrorKind: kind,
        });

        console.warn(`[zip-v2] Tick failed (retrying).`, {
          jobId,
          transferId: job.transferId,
          consecutiveFailures,
          maxConsecutiveFailures,
          retryAfterSeconds: backoffSeconds,
          errorKind: kind,
          status,
          error: rawErrText,
        });

        return { status: "RUNNING", done: false, retryAfterSeconds: backoffSeconds };
      }

      console.error(`[zip-v2] job failed jobId=${jobId}`, {
        error: err,
        errorMessage: errMsg,
        errorKind: kind,
        retryable,
        consecutiveFailures,
        maxConsecutiveFailures,
        jobId,
        transferId: job.transferId,
        bundleObjectKey: checkpoint.outputKey,
        manifestKey: checkpoint.manifestKey,
        tickDurationMs: nowMs() - tickStartMs,
        checkpoint: checkpointSummary(checkpoint),
      });

      const cleanupAtMs = nowMs() + CLEANUP_TTL_MS;
      this.upsertJob({
        ...job,
        status: "FAILED",
        updatedAtMs: nowMs(),
        errorMessage: rawErrText,
        consecutiveFailures,
        lastFailureAtMs: nowMs(),
        nextRetryAtMs: undefined,
        lastErrorKind: kind,
        cleanupAtMs,
      });
      await this.state.storage.setAlarm(cleanupAtMs);
      console.log(`[zip-v2] Cleanup scheduled (failed).`, {
        jobId,
        transferId: job.transferId,
        cleanupAtMs,
      });

      // Best-effort failure callback
      const webAPIService = new WebAPIService(this.env.SECRET_KEY, this.env.WEB_API_BASE_URL);
      await webAPIService
        .updateTransferStatus(job.transferId, {
          status: TransferStatus.READY_BUT_COMPRESSION_FAILED,
        })
        .catch((e) =>
          console.warn(`[zip-v2] Failed to update transfer status`, {
            error: e,
            jobId,
            transferId: job.transferId,
            bundleObjectKey: checkpoint.outputKey,
            manifestKey: checkpoint.manifestKey,
          }),
        );

      return { status: "FAILED", done: false };
    } finally {
      const semaphoreId = this.env.ZipSemaphore.idFromName("global");
      await this.env.ZipSemaphore.get(semaphoreId).fetch("https://semaphore/release", {
        method: "POST",
        body: JSON.stringify({ jobId }),
      });
    }
  }

  // Used by the web API to get the job status
  // private async getJobStatus(jobId: string) {
  //   const job = this.getJobRow(jobId);
  //   const checkpoint = this.getCheckpoint(jobId);

  //   if (!job || !checkpoint) {
  //     return { jobId, exists: false };
  //   }

  //   return {
  //     jobId,
  //     transferId: job.transferId,
  //     status: job.status,
  //     errorMessage: job.errorMessage ?? null,
  //     manifestKey: checkpoint.manifestKey,
  //     outputKey: checkpoint.outputKey,
  //     uploadId: checkpoint.uploadId ?? null,
  //     partSize: checkpoint.partSize,
  //     nextPartNumber: checkpoint.nextPartNumber,
  //     fileIndex: checkpoint.fileIndex,
  //     zipOffset: checkpoint.zipOffset,
  //     bytesWrittenTotal: checkpoint.bytesWrittenTotal,
  //     filesDone: checkpoint.filesDone,
  //     done: checkpoint.done,
  //     updatedAtMs: job.updatedAtMs,
  //   };
  // }

  // --------------------------------------------------------------------------------
  // SQL helpers
  // --------------------------------------------------------------------------------
  private sql() {
    return this.state.storage.sql as any;
  }

  private initIfNeeded() {
    const sql = this.sql();
    sql.exec(
      `CREATE TABLE IF NOT EXISTS job_state (
        jobId TEXT PRIMARY KEY,
        transferId TEXT NOT NULL,
        status TEXT NOT NULL,
        createdAtMs INTEGER NOT NULL,
        updatedAtMs INTEGER NOT NULL,
        errorMessage TEXT,
        consecutiveFailures INTEGER NOT NULL DEFAULT 0,
        lastFailureAtMs INTEGER,
        nextRetryAtMs INTEGER,
        lastErrorKind TEXT,
        cleanupAtMs INTEGER
      );`,
    );

    // Backward-compatible migrations for existing environments.
    // (CREATE TABLE IF NOT EXISTS does not add columns.)
    for (const stmt of [
      `ALTER TABLE job_state ADD COLUMN consecutiveFailures INTEGER NOT NULL DEFAULT 0;`,
      `ALTER TABLE job_state ADD COLUMN lastFailureAtMs INTEGER;`,
      `ALTER TABLE job_state ADD COLUMN nextRetryAtMs INTEGER;`,
      `ALTER TABLE job_state ADD COLUMN lastErrorKind TEXT;`,
      `ALTER TABLE job_state ADD COLUMN cleanupAtMs INTEGER;`,
    ]) {
      try {
        sql.exec(stmt);
      } catch {
        // ignore: column already exists
      }
    }
    sql.exec(
      `CREATE TABLE IF NOT EXISTS checkpoint (
        jobId TEXT PRIMARY KEY,
        manifestKey TEXT NOT NULL,
        outputKey TEXT NOT NULL,
        uploadId TEXT,
        partSize INTEGER NOT NULL,
        nextPartNumber INTEGER NOT NULL,
        fileIndex INTEGER NOT NULL,
        zipOffset TEXT NOT NULL,
        bytesWrittenTotal TEXT NOT NULL,
        filesDone INTEGER NOT NULL,
        done INTEGER NOT NULL
      );`,
    );
    sql.exec(
      `CREATE TABLE IF NOT EXISTS uploaded_parts (
        jobId TEXT NOT NULL,
        partNumber INTEGER NOT NULL,
        etag TEXT NOT NULL,
        sizeBytes INTEGER NOT NULL,
        PRIMARY KEY (jobId, partNumber)
      );`,
    );

    sql.exec(
      `CREATE TABLE IF NOT EXISTS zip_entries (
        jobId TEXT NOT NULL,
        fileIndex INTEGER NOT NULL,
        nameB64 TEXT NOT NULL,
        crc32 INTEGER NOT NULL,
        compressedSize TEXT NOT NULL,
        uncompressedSize TEXT NOT NULL,
        localHeaderOffset TEXT NOT NULL,
        modTime INTEGER NOT NULL,
        modDate INTEGER NOT NULL,
        PRIMARY KEY (jobId, fileIndex)
      );`,
    );
  }

  private getJobRow(jobId: string): JobStateRow | null {
    const sql = this.sql();
    const rs = sql.exec(
      `SELECT jobId, transferId, status, createdAtMs, updatedAtMs, errorMessage,
              consecutiveFailures, lastFailureAtMs, nextRetryAtMs, lastErrorKind, cleanupAtMs
       FROM job_state WHERE jobId = ?;`,
      jobId,
    );
    const rows = rs.toArray?.() ?? [];
    const row = rows[0];
    if (!row) {
      return null;
    }

    const r: any = row;
    return {
      jobId: r.jobId,
      transferId: r.transferId,
      status: r.status,
      createdAtMs: r.createdAtMs,
      updatedAtMs: r.updatedAtMs,
      errorMessage: r.errorMessage ?? undefined,
      consecutiveFailures: r.consecutiveFailures ?? 0,
      lastFailureAtMs: r.lastFailureAtMs ?? undefined,
      nextRetryAtMs: r.nextRetryAtMs ?? undefined,
      lastErrorKind: (r.lastErrorKind ?? undefined) as any,
      cleanupAtMs: r.cleanupAtMs ?? undefined,
    } satisfies JobStateRow;
  }

  private getCheckpoint(jobId: string): Checkpoint | null {
    const sql = this.sql();
    const rs = sql.exec(
      `SELECT jobId, manifestKey, outputKey, uploadId, partSize, nextPartNumber, fileIndex, zipOffset, bytesWrittenTotal, filesDone, done
       FROM checkpoint WHERE jobId = ?;`,
      jobId,
    );

    const rows = rs.toArray?.() ?? [];
    const row = rows[0];
    if (!row) {
      return null;
    }

    const r: any = row;

    return {
      manifestKey: r.manifestKey,
      outputKey: r.outputKey,
      uploadId: r.uploadId ?? undefined,
      partSize: r.partSize,
      nextPartNumber: r.nextPartNumber,
      fileIndex: r.fileIndex,
      zipOffset: r.zipOffset,
      bytesWrittenTotal: r.bytesWrittenTotal,
      filesDone: r.filesDone,
      done: Boolean(r.done),
    };
  }

  private upsertJob(row: JobStateRow) {
    const sql = this.sql();
    sql.exec(
      `INSERT INTO job_state (
         jobId, transferId, status, createdAtMs, updatedAtMs, errorMessage,
         consecutiveFailures, lastFailureAtMs, nextRetryAtMs, lastErrorKind, cleanupAtMs
       )
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(jobId) DO UPDATE SET
         transferId=excluded.transferId,
         status=excluded.status,
         updatedAtMs=excluded.updatedAtMs,
         errorMessage=excluded.errorMessage,
         consecutiveFailures=excluded.consecutiveFailures,
         lastFailureAtMs=excluded.lastFailureAtMs,
         nextRetryAtMs=excluded.nextRetryAtMs,
         lastErrorKind=excluded.lastErrorKind,
         cleanupAtMs=excluded.cleanupAtMs;`,
      row.jobId,
      row.transferId,
      row.status,
      row.createdAtMs,
      row.updatedAtMs,
      row.errorMessage ?? null,
      row.consecutiveFailures ?? 0,
      row.lastFailureAtMs ?? null,
      row.nextRetryAtMs ?? null,
      row.lastErrorKind ?? null,
      row.cleanupAtMs ?? null,
    );
  }

  private upsertCheckpoint(jobId: string, cp: Checkpoint) {
    const sql = this.sql();
    sql.exec(
      `INSERT INTO checkpoint
        (jobId, manifestKey, outputKey, uploadId, partSize, nextPartNumber, fileIndex, zipOffset, bytesWrittenTotal, filesDone, done)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(jobId) DO UPDATE SET
         manifestKey=excluded.manifestKey,
         outputKey=excluded.outputKey,
         uploadId=excluded.uploadId,
         partSize=excluded.partSize,
         nextPartNumber=excluded.nextPartNumber,
         fileIndex=excluded.fileIndex,
         zipOffset=excluded.zipOffset,
         bytesWrittenTotal=excluded.bytesWrittenTotal,
         filesDone=excluded.filesDone,
         done=excluded.done;`,
      jobId,
      cp.manifestKey,
      cp.outputKey,
      cp.uploadId ?? null,
      cp.partSize,
      cp.nextPartNumber,
      cp.fileIndex,
      cp.zipOffset,
      cp.bytesWrittenTotal,
      cp.filesDone,
      cp.done ? 1 : 0,
    );
  }

  private insertUploadedParts(jobId: string, parts: UploadedPart[]) {
    const sql = this.sql();
    for (const p of parts) {
      sql.exec(
        `INSERT INTO uploaded_parts (jobId, partNumber, etag, sizeBytes)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(jobId, partNumber) DO UPDATE SET etag=excluded.etag, sizeBytes=excluded.sizeBytes;`,
        jobId,
        p.partNumber,
        p.etag,
        p.sizeBytes,
      );
    }
  }

  private upsertZipEntry(
    jobId: string,
    fileIndex: number,
    entry: Omit<ZipEntryRow, "jobId" | "fileIndex">,
  ) {
    const sql = this.sql();
    sql.exec(
      `INSERT INTO zip_entries
        (jobId, fileIndex, nameB64, crc32, compressedSize, uncompressedSize, localHeaderOffset, modTime, modDate)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(jobId, fileIndex) DO UPDATE SET
         nameB64=excluded.nameB64,
         crc32=excluded.crc32,
         compressedSize=excluded.compressedSize,
         uncompressedSize=excluded.uncompressedSize,
         localHeaderOffset=excluded.localHeaderOffset,
         modTime=excluded.modTime,
         modDate=excluded.modDate;`,
      jobId,
      fileIndex,
      entry.nameB64,
      entry.crc32,
      entry.compressedSize,
      entry.uncompressedSize,
      entry.localHeaderOffset,
      entry.modTime,
      entry.modDate,
    );
  }

  private listZipEntries(jobId: string): Array<Omit<ZipEntryRow, "jobId">> {
    const sql = this.sql();
    const rs = sql.exec(
      `SELECT fileIndex, nameB64, crc32, compressedSize, uncompressedSize, localHeaderOffset, modTime, modDate
       FROM zip_entries WHERE jobId = ? ORDER BY fileIndex ASC;`,
      jobId,
    );
    const rows = rs.toArray?.() ?? [];
    return rows as Array<Omit<ZipEntryRow, "jobId">>;
  }
}
