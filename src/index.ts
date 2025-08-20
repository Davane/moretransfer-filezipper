import { Zip, ZipDeflate } from "fflate";
import { ZipJob } from "./lib/types/types";
import { verifyHmac } from "./lib/utils";

export interface Env {
  SOURCE_BUCKET: R2Bucket;
  OUTPUT_BUCKET: R2Bucket;
  
  ZIP_OUTPUT_PREFIX: string;
  ZIP_OUTPUT_FILE_NAME: string;

  MAX_FILES?: string;
  MAX_ZIP_BYTES?: string;

  // Queues
  QUEUE_FILE_ZIPPER: Queue;

  // Durable Object
  ZipLocks: DurableObjectNamespace;

  // Environment variables
  HMAC_SECRET: string
}

export default {
  /**
   * Simple HTTP producer to enqueue jobs (optional – you can also enqueue from other workers)
   * @param req - The incoming request
   * @param env - The environment variables
   */
  async fetch(req: Request, env: Env): Promise<Response> {
    if (req.method === "POST" && new URL(req.url).pathname === "/compress-files") {

      try {
        // TODO: remove when testing from CLI
        await verifyHmac(req, env.HMAC_SECRET);
      } catch (error) {
        console.error("HMAC verification failed:", error);
        return new Response("Unauthorized", { status: 401 });
      }
      
      const body = await req.json<Partial<ZipJob>>();

      console.log("Compressing files:", JSON.stringify(body));
      if (!body.objectPrefix) {
        return new Response("Missing prefix", { status: 400 });
      }

      const job: ZipJob = {
        objectPrefix: body.objectPrefix,
        zipOutputKey: body.zipOutputKey,
        includeEmpty: body.includeEmpty ?? true,
        createdBy: body.createdBy ?? "api",
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
    }

    return new Response("OK");
  },

  /**
   * Queue consumer: does the actual ZIP work in the background. This function
   * processes a batch of ZIP jobs from the queue.
   * @param batch - The batch of messages to process
   * @param env - The environment variables
   * @param ctx - The execution context
   */
  async queue(batch: MessageBatch<ZipJob>, env: Env, ctx: ExecutionContext) {
    for (const msg of batch.messages) {
      console.log(`Processing message ${msg.id} from batch`, JSON.stringify(msg.body));

      try {
        await processZipJob(msg.body, env);
        // Acknowledge the message has been processed
        msg.ack();
      } catch (err) {
        // Let the queue retry with exponential backoff
        console.error("Zip job failed:", err);
        msg.retry();
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
  const zipOutputKey =
    job.zipOutputKey ?? `${env.ZIP_OUTPUT_PREFIX}/${currentObjectPrefix.replace(/\/?$/, "")}/${env.ZIP_OUTPUT_FILE_NAME}.zip`;
  const maxFiles = toInt(env.MAX_FILES, 100);
  const maxZipBytes = toInt(env.MAX_ZIP_BYTES, 3000 * 1024 * 1024); // 3GB

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
    // If a pre-baked ZIP already exists and is fresh enough, you could skip here.
    // const existing = await env.OUTPUT_BUCKET.head(zipOutputKey);
    // if (existing) {return;}

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
    let listed: R2Objects = await env.SOURCE_BUCKET.list({
      prefix: currentObjectPrefix,
      limit: 500,
    });
    let totalFiles = 0;
    let totalBytes = 0;

    console.log(`Listing ${listed.objects.length} objects for prefix: ${currentObjectPrefix}`);
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
        const nameInZip =
          key.substring(currentObjectPrefix.length).replace(/^\/+/, "") ||
          obj.key.split("/").pop() ||
          "file";

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

    // // Add a manifest file (optional)
    // const manifest = JSON.stringify(
    //   { prefix: currentObjectPrefix, totalFiles, totalBytes, createdAt: new Date().toISOString() },
    //   null,
    //   2
    // );
    // await addFile("manifest.json", new Blob([manifest]).stream());

    // Finalize the ZIP stream
    await finalize();

    // Wait for multipart upload to finish
    const output = await uploader;
    console.log("ZIP completed:", output.key, "ETag:", output.etag);
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
    writer.write(chunk);
    if (final) writer.close();
  });

  async function addFile(name: string, body: ReadableStream) {
    // Create a streaming deflate entry
    const entry = new ZipDeflate(name, { level: 6 }); // tweak level if desired
    zip.add(entry);

    const reader = body.getReader();
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        break;
      }
      // Pass raw bytes into the deflater; fflate compresses and emits to the zip callback
      entry.push(value);
    }
    // Signal end-of-file
    entry.push(new Uint8Array(0), true);
    console.log(`Added file to ZIP: ${name}`);
  }

  async function finalize() {
    console.log("Finalizing ZIP ...");

    // Optionally write a comment file or README, etc.
    // Add a short README
    // zip.add(new AsyncZipDeflate("README.txt"), strToU8("Generated by Cloudflare Worker\n"));
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
  mp: R2MultipartUpload,
  stream: ReadableStream<Uint8Array>
) {
  const reader = stream.getReader();
  const PART_SIZE = 16 * 1024 * 1024; // 16MB parts
  const etags: R2UploadedPart[] = [];
  let partNumber = 1;

  // Assemble parts
  let current: Uint8Array[] = [];
  let currentBytes = 0;

  async function flushPart(final = false) {
    if (currentBytes === 0 && !final) {
      return;
    }

    // Coalesce chunks to a single Uint8Array for the part
    let combined: Uint8Array;
    if (current.length === 1) {
      combined = current[0];
    } else {
      combined = new Uint8Array(currentBytes);
      let offset = 0;
      for (const c of current) {
        combined.set(c, offset);
        offset += c.byteLength;
      }
    }

    const uploaded = await mp.uploadPart(partNumber++, combined);
    etags.push(uploaded);
    current = [];
    currentBytes = 0;
  }

  while (true) {
    const { value, done } = await reader.read();
    if (done) {
      await flushPart(true);
      break;
    }

    if (value?.byteLength) {
      current.push(value);
      currentBytes += value.byteLength;
      if (currentBytes >= PART_SIZE) {
        await flushPart();
      }
    }
  }

  return mp.complete(etags);
}

function cleanPrefix(p: string) {
  // Normalize to "prefix/" (no leading slash)
  return p.replace(/^\/+/, "").replace(/\/?$/, "/");
}

function toInt(s: string | undefined, defaultValue: number) {
  const n = s ? parseInt(s, 10) : NaN;
  return Number.isFinite(n) ? n : defaultValue;
}
