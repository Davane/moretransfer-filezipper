import { Env, TransferStatus, ZipJobManifest } from "../lib/types/types";
import { WebAPIService } from "./web-api-service";
import { toInt, toBool } from "./job-manifest";

type JobStatus = "PENDING" | "RUNNING" | "FINALIZING" | "DONE" | "FAILED" | "CANCELLED";

type UploadedPart = { partNumber: number; etag: string; sizeBytes: number };

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
};

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

export class JobManagerDO {
  constructor(
    private readonly state: DurableObjectState,
    private readonly env: Env,
  ) {}

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
        errorMessage TEXT
      );`,
    );
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

  async fetch(req: Request): Promise<Response> {
    this.initIfNeeded();
    const url = new URL(req.url);

    if (req.method === "POST" && url.pathname === "/start") {
      const body = (await req.json()) as {
        jobId: string;
        transferId: string;
        manifestKey: string;
        outputKey: string;
      };

      await this.handleStart(body);

      return jsonResponse({ ok: true });
    }

    // Handle tick requests
    if (req.method === "POST" && url.pathname === "/tick") {
      const body = (await req.json()) as { jobId: string };
      const result = await this.handleTick(body.jobId);

      return jsonResponse(result);
    }

    // Handle status requests
    if (req.method === "GET" && url.pathname === "/status") {
      const jobId = url.searchParams.get("jobId");
      if (!jobId) {
        return jsonResponse({ error: "missing jobId" }, 400);
      }

      const status = await this.getStatus(jobId);

      return jsonResponse(status);
    }

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

  private getJobRow(jobId: string): JobStateRow | null {
    const sql = this.sql();
    const rs = sql.exec(
      `SELECT jobId, transferId, status, createdAtMs, updatedAtMs, errorMessage FROM job_state WHERE jobId = ?;`,
      jobId,
    );
    const row = rs.one?.() ?? null;

    return row as JobStateRow | null;
  }

  private getCheckpoint(jobId: string): Checkpoint | null {
    const sql = this.sql();
    const rs = sql.exec(
      `SELECT jobId, manifestKey, outputKey, uploadId, partSize, nextPartNumber, fileIndex, zipOffset, bytesWrittenTotal, filesDone, done
       FROM checkpoint WHERE jobId = ?;`,
      jobId,
    );

    const row = rs.one?.() ?? null;
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
      `INSERT INTO job_state (jobId, transferId, status, createdAtMs, updatedAtMs, errorMessage)
       VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT(jobId) DO UPDATE SET
         transferId=excluded.transferId,
         status=excluded.status,
         updatedAtMs=excluded.updatedAtMs,
         errorMessage=excluded.errorMessage;`,
      row.jobId,
      row.transferId,
      row.status,
      row.createdAtMs,
      row.updatedAtMs,
      row.errorMessage ?? null,
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

  private listUploadedParts(jobId: string): UploadedPart[] {
    const sql = this.sql();
    const rs = sql.exec(
      `SELECT partNumber, etag, sizeBytes FROM uploaded_parts WHERE jobId = ? ORDER BY partNumber ASC;`,
      jobId,
    );
    const rows = rs.toArray?.() ?? [];
    return rows as UploadedPart[];
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

  // private async readManifest(manifestKey: string): Promise<ZipJobManifest> {
  //   const obj = await this.env.OUTPUT_BUCKET.get(manifestKey);
  //   if (!obj?.body) {
  //     throw new Error(`Missing manifest: ${manifestKey}`);
  //   }
  //   const text = await obj.text();
  //   return JSON.parse(text) as ZipJobManifest;
  // }

  private async handleStart(body: {
    jobId: string;
    transferId: string;
    manifestKey: string;
    outputKey: string;
  }) {
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

    const defaultPartSize = 128 * 1024 * 1024; // 128 MiB
    const partSize = toInt(this.env.ZIP_PART_SIZE_BYTES, defaultPartSize);

    this.upsertJob({
      jobId,
      transferId,
      status: "PENDING",
      createdAtMs: nowMs(),
      updatedAtMs: nowMs(),
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

  private async handleTick(jobId: string): Promise<{ status: JobStatus; done: boolean }> {
    const job = this.getJobRow(jobId);
    const checkpoint = this.getCheckpoint(jobId);
    if (!job || !checkpoint) {
      throw new Error(`Unknown job: ${jobId}`);
    }

    if (job.status === "DONE" || job.status === "FAILED" || job.status === "CANCELLED") {
      return { status: job.status, done: job.status === "DONE" };
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

    const semaphoreId = this.env.ZipSemaphore.idFromName("global");
    const semaphore = this.env.ZipSemaphore.get(semaphoreId);
    const desired = toInt(this.env.ZIP_GLOBAL_CONCURRENCY, 1);
    const acquiredResp = await semaphore.fetch("https://semaphore/acquire", {
      method: "POST",
      body: JSON.stringify({ jobId, limit: desired }),
    });

    if (!acquiredResp.ok) {
      const errText = await acquiredResp.text();
      console.error(`[zip-v2] Failed to acquire semaphore.`, {
        error: errText,
        status: acquiredResp.status,
        jobId,
        transferId: job.transferId,
        bundleObjectKey: checkpoint.outputKey,
        manifestKey: checkpoint.manifestKey,
      });
      // No token; caller should retry later.
      return { status: job.status, done: false };
    }

    try {
      this.upsertJob({ ...job, status: "RUNNING", updatedAtMs: nowMs() });

      const maxParts = toInt(this.env.ZIP_MAX_PARTS_PER_TICK, 8);
      if (!checkpoint.uploadId) {
        const upload = await this.env.OUTPUT_BUCKET.createMultipartUpload(checkpoint.outputKey, {
          customMetadata: {
            jobId,
            transferId: job.transferId,
            manifestKey: checkpoint.manifestKey,
            zipVersion: "zip64-store-container-v1",
          },
        });
        checkpoint.uploadId = upload.uploadId;
        this.upsertCheckpoint(jobId, checkpoint);
      }

      const containerId = this.env.ZipContainer.idFromName(jobId);
      const containerStub = this.env.ZipContainer.get(containerId);

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
      if (!runResp.ok) {
        const errText = await runResp.text();
        console.error(`[zip-v2] Container run failed.`, {
          status: runResp.status,
          error: errText,
          jobId,
          transferId: job.transferId,
          bundleObjectKey: checkpoint.outputKey,
          manifestKey: checkpoint.manifestKey,
        });
        throw new Error(`Container run failed: ${runResp.status} ${errText}`);
      }

      const run = (await runResp.json()) as {
        uploadedParts: UploadedPart[];
        nextPartNumber: number;
        fileIndex: number;
        zipOffset: string;
        bytesWrittenTotal: string;
        filesDone: number;
        done: boolean;
      };

      if (run.uploadedParts?.length) {
        this.insertUploadedParts(jobId, run.uploadedParts);
      }

      const updated: Checkpoint = {
        ...checkpoint,
        nextPartNumber: run.nextPartNumber,
        fileIndex: run.fileIndex,
        zipOffset: run.zipOffset,
        bytesWrittenTotal: run.bytesWrittenTotal,
        filesDone: run.filesDone,
        done: run.done,
      };
      this.upsertCheckpoint(jobId, updated);

      if (run.done) {
        // Container should have completed multipart; verify output exists and update status.
        const out = await this.env.OUTPUT_BUCKET.head(checkpoint.outputKey);
        if (!out) {
          throw new Error("Container reported done but output does not exist");
        }

        this.upsertJob({ ...job, status: "DONE", updatedAtMs: nowMs() });

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

        return { status: "DONE", done: true };
      }

      // Not done; caller should enqueue another tick.
      return { status: "RUNNING", done: false };
    } catch (err: any) {
      console.error(`[zip-v2] job failed jobId=${jobId}`, {
        error: err,
        jobId,
        transferId: job.transferId,
        bundleObjectKey: checkpoint.outputKey,
        manifestKey: checkpoint.manifestKey,
      });
      this.upsertJob({
        ...job,
        status: "FAILED",
        updatedAtMs: nowMs(),
        errorMessage: String(err?.message ?? err),
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

  private async getStatus(jobId: string) {
    const job = this.getJobRow(jobId);
    const checkpoint = this.getCheckpoint(jobId);

    if (!job || !checkpoint) {
      return { jobId, exists: false };
    }

    return {
      jobId,
      transferId: job.transferId,
      status: job.status,
      errorMessage: job.errorMessage ?? null,
      manifestKey: checkpoint.manifestKey,
      outputKey: checkpoint.outputKey,
      uploadId: checkpoint.uploadId ?? null,
      partSize: checkpoint.partSize,
      nextPartNumber: checkpoint.nextPartNumber,
      fileIndex: checkpoint.fileIndex,
      zipOffset: checkpoint.zipOffset,
      bytesWrittenTotal: checkpoint.bytesWrittenTotal,
      filesDone: checkpoint.filesDone,
      done: checkpoint.done,
      updatedAtMs: job.updatedAtMs,
    };
  }
}
