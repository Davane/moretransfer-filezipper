import { Zip, ZipPassThrough } from "fflate";
import { RequestPath, TransferStatus, TransferUpdateRequest, ZipJob } from "./lib/types/types";
import { verifyHmac } from "./lib/crypto";
import { WebAPIService } from "./lib/web-api-service";
import { calculateExponentialBackoff } from "./lib/utils";

export interface Env {
  SOURCE_BUCKET: R2Bucket;
  OUTPUT_BUCKET: R2Bucket;

  // Queues
  QUEUE_FILE_ZIPPER: Queue;

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

export default {
  /**
   * Simple HTTP producer(endpoint) to enqueue jobs
   * @param req - The incoming request
   * @param env - The environment variables
   */
  async fetch(req: Request, env: Env): Promise<Response> {
    if (req.method !== "POST") {
      return new Response(undefined, { status: 405 });
    }

    const url = new URL(req.url);
    console.log(`Executing worker for url ${url.pathname}`);

    if (url.pathname !== RequestPath.COMPRESS_FILES) {
      return new Response(undefined, { status: 404 });
    }

    if (!env.SKIP_REQUEST_VERIFICATION) {
      try {
        await verifyHmac(req, env.SECRET_KEY);
      } catch (error) {
        console.error("HMAC verification failed:", error);
        return new Response("Unauthorized", { status: 401 });
      }
    } else {
      console.warn("Skipping request verification");
    }

    const body = await req.json<ZipJob>();
    console.log("Compressing files:", JSON.stringify(body));

    if (!body.objectPrefix) {
      return new Response("Missing prefix", { status: 400 });
    }

    const job: ZipJob = {
      transferId: body.transferId,
      objectPrefix: body.objectPrefix,
      zipOutputKey: body.zipOutputKey,
      includeEmpty: body.includeEmpty ?? true,
      createdBy: body.createdBy ?? "api",
      files: body.files,
    };

    try {
      console.log("Sending job to queue:", JSON.stringify(job));
      await env.QUEUE_FILE_ZIPPER.send(job);
      console.log("Job queued", JSON.stringify(job));
    } catch (error) {
      console.error("Failed to enqueue job:", error);
      return new Response("Failed to enqueue job", { status: 500 });
    }

    return new Response("Enqueued", { status: 202 });
  },

  /**
   * Queue consumer: does the actual ZIP work in the background. This function
   * processes a batch of ZIP jobs from the queue.
   * @param batch - The batch of messages to process
   * @param env - The environment variables
   * @param ctx - The execution context
   */
  async queue(batch: MessageBatch<ZipJob>, env: Env, ctx: ExecutionContext) {
    const webAPIService = new WebAPIService(env.SECRET_KEY, env.WEB_API_BASE_URL);

    for (const msg of batch.messages) {
      console.log(`Processing message ${msg.id} from batch`, JSON.stringify(msg.body));
      const messagePayload = msg.body;

      let transferStatusPayload: TransferUpdateRequest = {
        status: TransferStatus.READY_BUT_COMPRESSION_FAILED,
      };

      try {
        const bundleObjectKey = await processZipJob(messagePayload, env);

        // Update the transfer status
        transferStatusPayload = {
          status: TransferStatus.READY,
          bundleObjectKey,
        };

        // Acknowledge the message has been processed
        msg.ack();
      } catch (err) {
        const delaySeconds = calculateExponentialBackoff(
          msg.attempts,
          env.BASE_RETRY_DELAY_SECONDS
        );
        console.error(
          `Compression job failed. Attempt: ${msg.attempts}. Retry in ${delaySeconds} seconds:`,
          err
        );

        msg.retry({ delaySeconds });
      } finally {
        if (!env.SKIP_REQUEST_VERIFICATION) {
          try {
            // Update the transfer status.
            await webAPIService.updateTransferStatus(
              messagePayload.transferId,
              transferStatusPayload
            );
          } catch (error) {
            // If failed we dont want to redo the bundling process.
            // Ignore errors here and handle when user attempts to download
            console.warn(`Failed to update transfer status ${messagePayload.transferId}:`, error);
          }
        } else {
          console.warn(`Skipping transfer status update for ${messagePayload.transferId}`);
        }
      }
    }
  },
};

// ------------------------------------------------------------------------------
// Helper functions
// ------------------------------------------------------------------------------

// ---- Durable Object for per-prefix locks (prevents duplicate work) ----
export class ZipLocksDO {
  state: DurableObjectState;
  constructor(state: DurableObjectState) {
    this.state = state;
  }

  async fetch(req: Request) {
    const url = new URL(req.url);

    if (url.pathname === "/lock") {
      console.log(`Locking object ${this.state.id} ...`);

      // Acquire a short lock if not already locked
      const existing = await this.state.storage.getAlarm();
      if (existing) {
        console.log(`Object Locked: ${this.state.id}`);
        return new Response("locked", { status: 423 });
      }

      // set a short TTL alarm (e.g., 2 minutes)
      await this.state.storage.setAlarm(Date.now() + 120_000);
      console.log(`Object Locked: ${this.state.id}`);

      return new Response("ok");
    }

    if (url.pathname === "/unlock") {
      console.log(`Unlocking object ${this.state.id} ...`);
      await this.state.storage.deleteAlarm();
      console.log(`Object unlocked: ${this.state.id}.`);
      return new Response("ok");
    }

    return new Response("not found", { status: 404 });
  }

  async alarm() {
    // Lock expires automatically when alarm fires
    console.log("Object automatically unlocked:", this.state.id);
    await this.state.storage.deleteAlarm();
  }
}

async function processZipJob(job: ZipJob, env: Env) {
  const currentObjectPrefix = cleanPrefix(job.objectPrefix);
  const defaultZipOutputKey = `${env.ZIP_OUTPUT_PREFIX}/${currentObjectPrefix.replace(
    /\/?$/,
    ""
  )}/${env.ZIP_OUTPUT_FILE_NAME}.zip`;

  const zipOutputKey = job.zipOutputKey ?? defaultZipOutputKey;
  const maxFiles = toInt(env.MAX_FILES, 100);
  const maxZipBytes = toInt(env.MAX_ZIP_BYTES, 3000 * 1024 * 1024); // 3GB

  // If a pre-baked ZIP already exists and is fresh enough, you could skip here.
  const existing = await env.OUTPUT_BUCKET.head(zipOutputKey);
  if (existing) {
    console.log(`ZIP already exists: ${zipOutputKey}, skipping...`);
    return zipOutputKey;
  }

  // Per-prefix lock so two workers don’t create the same ZIP simultaneously
  const id = env.ZipLocks.idFromName(currentObjectPrefix);
  const stub = env.ZipLocks.get(id);

  console.log(`Locking source object key [${currentObjectPrefix}]. Stub ID [${id}].`);
  const lockResp = await stub.fetch("https://lock/lock", { method: "POST" });
  if (!lockResp.ok) {
    console.error(`Already locked: ${currentObjectPrefix}.`);
    throw new Error(`Prefix is locked: ${currentObjectPrefix}`);
  }

  try {
    

    // Stream ZIP to R2 via multipart upload
    const mp = await env.OUTPUT_BUCKET.createMultipartUpload(zipOutputKey, {
      // You can set httpMetadata or customMetadata if you want
      customMetadata: { prefix: currentObjectPrefix, createdBy: job.createdBy ?? "worker" },
    });

    // Create a web-stream of ZIP bytes
    const { stream, addFile, finalize } = createZipStream();

    // Pipe that stream into R2 multipart parts of ~10–20MB each
    const uploader = pumpToMultipart(env.OUTPUT_BUCKET, mp, stream);

    // Walk all objects with the prefix and add them to the ZIP
    let totalFiles = 0;
    let totalBytes = 0;
    let listed: R2Objects = await env.SOURCE_BUCKET.list({
      prefix: currentObjectPrefix,
      limit: 500,
    });

    console.log(`Listing ${listed.objects.length} objects for prefix: ${currentObjectPrefix}`);
    if (listed.objects.length === 0) {
      console.log(`No objects found for prefix: ${currentObjectPrefix}`);
      throw new Error(`No objects found for prefix: ${currentObjectPrefix}`);
    }

    // Build lookup map for relativePath (used to preserve folder structure in ZIP)
    const filePathMap = new Map(
      (job.files ?? []).map((f) => [f.key, f.relativePath])
    );

    while (true) {
      for (const obj of listed.objects) {
        if (totalFiles >= maxFiles) {
          console.error(`File count limit hit: ${maxFiles}`);
          throw new Error(`File count limit hit: ${maxFiles}`);
        }

        if (!job.includeEmpty && obj.size === 0) {
          continue;
        }

        totalFiles++;
        totalBytes += obj.size;

        if (totalBytes > maxZipBytes) {
          console.error(`ZIP size limit hit: ${maxZipBytes} bytes. Current limit is ${totalBytes}`);
          throw new Error(`ZIP size limit hit: ${maxZipBytes} bytes`);
        }

        const key = obj.key;

        // Check if we have a relativePath from the file metadata (for folder uploads)
        const relativePath = filePathMap.get(key);

        let nameInZip: string;
        if (relativePath) {
          // Use the folder path from metadata to preserve folder structure
          nameInZip = relativePath;
        } else {
          // Fallback to existing logic for backward compatibility
          const delimiterIndex = key.indexOf("__");
          const uploadedFileName =
            delimiterIndex >= 0 ? key.slice(delimiterIndex + 2, key.length) : "";
          nameInZip =
            uploadedFileName || // Get the uploaded filename
            key.substring(currentObjectPrefix.length).replace(/^\/+/, "") || // or use the last part of the key
            obj.key.split("/").pop() || // or use the last part of the key
            "file"; // else just use file
        }

        const r = await env.SOURCE_BUCKET.get(key);
        if (!r?.body) {
          console.warn(`Object is missing from source bucket: ${key}`);
          continue; // skip missing
        }

        console.log(`Adding file to ZIP: ${nameInZip} ...`);

        // Add a file entry to ZIP as a streaming deflate
        await addFile(nameInZip, r.body);
      }

      if (!listed.truncated || !listed.cursor) {
        console.log(`No more objects to list for prefix: ${currentObjectPrefix}`);
        break;
      }

      listed = await env.SOURCE_BUCKET.list({
        prefix: currentObjectPrefix,
        limit: 500,
        cursor: listed.cursor,
      });
    }

    // Finalize the ZIP stream
    await finalize();

    // Wait for multipart upload to finish
    const output = await uploader;
    console.log("ZIP completed:", output.key, "ETag:", output.etag);
    return output.key;
  } finally {
    await stub.fetch("https://lock/unlock", { method: "POST" });
  }
}

/**
 * Returns a ZIP byte stream plus helpers to add streaming files, then finalize.
 * Uses fflate’s streaming API so we never buffer entire objects in memory.
 */
function createZipStream() {
  const ts = new TransformStream<Uint8Array, Uint8Array>();
  const writer = ts.writable.getWriter();

  const zip = new Zip((err, chunk, final) => {
    if (err) {
      writer.abort(err);
      return;
    }
    // backpressure-friendly
    writer.write(chunk).catch((e) => {
      // if consumer aborted
      console.warn("ZIP writer aborted:", e);
    });
    if (final) {
      writer.close();
    }
  });

  async function addFile(name: string, body: ReadableStream) {
    // STORE (no compression) → almost no CPU, CRC only
    const entry = new ZipPassThrough(name);
    zip.add(entry);

    const reader = body.getReader();
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        break;
      }
      entry.push(value); // raw bytes
    }
    entry.push(new Uint8Array(0), true); // end of file
    console.log(`Added (stored) file to ZIP: ${name}`);
  }

  async function finalize() {
    console.log("Finalizing ZIP ...");
    zip.end();
  }

  return { stream: ts.readable, addFile, finalize };
}

/**
 * Pipes a ReadableStream of bytes into R2 multipart parts.
 * Returns the result of finalizeMultipartUpload.
 */
async function pumpToMultipart(
  bucket: R2Bucket,
  multipartUpload: R2MultipartUpload,
  stream: ReadableStream<Uint8Array>
) {
  const reader = stream.getReader();

  // R2 rules: non-final parts must be equal size, >= 5 MiB
  const PART_SIZE = 16 * 1024 * 1024; // 16 MiB (>= 5 MiB)

  const etags: R2UploadedPart[] = [];
  let partNumber = 1;

  // Fixed-size buffer for the current part
  let buf = new Uint8Array(PART_SIZE);
  let offset = 0;

  async function flushFullBuffer() {
    // Upload an EXACTLY PART_SIZE slice
    const toSend = offset === PART_SIZE ? buf : buf.subarray(0, offset);
    if (toSend.byteLength === 0) {
      return;
    } // nothing to send

    const uploaded = await multipartUpload.uploadPart(partNumber++, toSend);
    etags.push(uploaded);

    // New buffer for the next part (avoid sharing mutating memory)
    buf = new Uint8Array(PART_SIZE);
    offset = 0;
  }

  while (true) {
    const { value, done } = await reader.read();
    if (done) {
      break;
    }
    if (!value || value.byteLength === 0) {
      continue;
    }

    let chunk = value;
    let i = 0;
    while (i < chunk.byteLength) {
      const take = Math.min(PART_SIZE - offset, chunk.byteLength - i);
      buf.set(chunk.subarray(i, i + take), offset);
      offset += take;
      i += take;

      // Only flush when we filled EXACTLY PART_SIZE bytes
      if (offset === PART_SIZE) {
        await flushFullBuffer();
      }
    }
  }

  // Final (possibly smaller) part
  if (offset > 0) {
    await flushFullBuffer(); // sends buf.subarray(0, offset)
  }

  return multipartUpload.complete(etags);
}

function cleanPrefix(p: string) {
  // Normalize to "prefix/" (no leading slash)
  return p.replace(/^\/+/, "").replace(/\/?$/, "/");
}

function toInt(s: string | undefined, defaultValue: number) {
  const n = s ? parseInt(s, 10) : NaN;
  return Number.isFinite(n) ? n : defaultValue;
}
