import { Zip, AsyncZipDeflate, strToU8 } from "fflate";

export interface Env {
  SOURCE_BUCKET: R2Bucket;
  OUTPUT_BUCKET: R2Bucket;
  ZIP_OUTPUT_PREFIX: string;
  MAX_FILES?: string;
  MAX_ZIP_BYTES?: string;

  // Queues
  QUEUE_ZIP_BUNDLES: Queue;

  // Durable Object
  ZipLocks: DurableObjectNamespace;
}

// Payload the producer will enqueue
type ZipJob = {
  prefix: string;                  // R2 prefix to collect
  zipKey?: string;                 // optional custom output key in OUTPUT_BUCKET
  includeEmpty?: boolean;          // include zero-byte files (default true)
  createdBy?: string;              // optional audit
};

export default {
  // Simple HTTP producer to enqueue jobs (optional – you can also enqueue from other workers)
  async fetch(req: Request, env: Env): Promise<Response> {
    if (req.method === "POST" && new URL(req.url).pathname === "/enqueue-zip") {
      const body = (await req.json()) as Partial<ZipJob>;
      if (!body.prefix) return new Response("Missing prefix", { status: 400 });

      const job: ZipJob = {
        prefix: body.prefix,
        zipKey: body.zipKey,
        includeEmpty: body.includeEmpty ?? true,
        createdBy: body.createdBy ?? "api",
      };

      await env.QUEUE_ZIP_BUNDLES.send(job);
      return new Response("Enqueued", { status: 202 });
    }

    return new Response("OK");
  },

  // Queue consumer: does the actual ZIP work in the background
  async queue(batch: MessageBatch<ZipJob>, env: Env, ctx: ExecutionContext) {
    for (const msg of batch.messages) {
      try {
        await processZipJob(msg.body, env);
        msg.ack();
      } catch (err) {
        // Let the queue retry with exponential backoff
        console.error("Zip job failed:", err);
        msg.retry();
      }
    }
  },
};

// ---- Durable Object for per-prefix locks (prevents duplicate work) ----
export class ZipLocksDO {
  state: DurableObjectState;
  constructor(state: DurableObjectState) {
    this.state = state;
  }

  async fetch(req: Request) {
    const url = new URL(req.url);
    if (url.pathname === "/lock") {
      // Acquire a short lock if not already locked
      const existing = await this.state.storage.getAlarm();
      if (existing) return new Response("locked", { status: 423 });
      // set a short TTL alarm (e.g., 2 minutes)
      await this.state.storage.setAlarm(Date.now() + 120_000);
      return new Response("ok");
    }
    if (url.pathname === "/unlock") {
      await this.state.storage.deleteAlarm();
      return new Response("ok");
    }
    return new Response("not found", { status: 404 });
  }

  async alarm() {
    // Lock expires automatically when alarm fires
    await this.state.storage.deleteAlarm();
  }
}

async function processZipJob(job: ZipJob, env: Env) {
  const prefix = cleanPrefix(job.prefix);
  const zipKey = job.zipKey ?? `${env.ZIP_OUTPUT_PREFIX}${prefix.replace(/\/?$/, "")}.zip`;
  const maxFiles = toInt(env.MAX_FILES, 5000);
  const maxZipBytes = toInt(env.MAX_ZIP_BYTES, 500 * 1024 * 1024);

  // Per-prefix lock so two workers don’t create the same ZIP simultaneously
  const id = env.ZipLocks.idFromName(prefix);
  const stub = env.ZipLocks.get(id);
  const lockResp = await stub.fetch("https://lock/lock", { method: "POST" });
  if (!lockResp.ok) throw new Error(`Prefix is locked: ${prefix}`);

  try {
    // If a pre-baked ZIP already exists and is fresh enough, you could skip here.
    // const existing = await env.OUTPUT_BUCKET.head(zipKey);
    // if (existing) return;

    // Stream ZIP to R2 via multipart upload
    const mp = await env.OUTPUT_BUCKET.createMultipartUpload(zipKey, {
      // You can set httpMetadata or customMetadata if you want
      customMetadata: { prefix, createdBy: job.createdBy ?? "worker" },
    });

    // Create a web-stream of ZIP bytes
    const { stream, addFile, finalize } = createZipStream();

    // Pipe that stream into R2 multipart parts of ~10–20MB each
    const uploader = pumpToMultipart(env.OUTPUT_BUCKET, mp, stream);

    // Walk all objects with the prefix and add them to the ZIP
    let listed: R2Objects = await env.SOURCE_BUCKET.list({ prefix, limit: 1000, delimiter: undefined });
    let totalFiles = 0;
    let totalBytes = 0;

    while (true) {
      for (const obj of listed.objects) {
        if (totalFiles >= maxFiles) throw new Error(`File count limit hit: ${maxFiles}`);
        if (!job.includeEmpty && obj.size === 0) continue;

        totalFiles++;
        totalBytes += obj.size;

        if (totalBytes > maxZipBytes) throw new Error(`ZIP size limit hit: ${maxZipBytes} bytes`);

        const key = obj.key;
        const nameInZip = key.substring(prefix.length).replace(/^\/+/, "") || obj.key.split("/").pop() || "file";

        const r = await env.SOURCE_BUCKET.get(key);
        if (!r || !r.body) continue; // skip missing

        // Add a file entry to ZIP as a streaming deflate
        await addFile(nameInZip, r.body);
      }

      if (!listed.truncated || !listed.cursor) break;
      listed = await env.SOURCE_BUCKET.list({ prefix, limit: 1000, cursor: listed.cursor });
    }

    // Add a manifest file (optional)
    const manifest = JSON.stringify(
      { prefix, totalFiles, totalBytes, createdAt: new Date().toISOString() },
      null,
      2
    );
    await addFile("manifest.json", new Blob([manifest]).stream());

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
    const entry = new AsyncZipDeflate(name, { level: 6 }); // tweak level if desired
    zip.add(entry);

    const reader = body.getReader();
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      // Pass raw bytes into the deflater; fflate compresses and emits to the zip callback
      entry.push(value);
    }
    // Signal end-of-file
    entry.push(new Uint8Array(0), true);
  }

  async function finalize() {
    // Optionally write a comment file or README, etc.
    // Add a short README
    zip.add(new AsyncZipDeflate("README.txt"), strToU8("Generated by Cloudflare Worker\n"));
    zip.end();
  }

  return { stream: ts.readable, addFile, finalize };
}

/**
 * Pipes a ReadableStream of bytes into R2 multipart parts.
 * Returns the result of finalizeMultipartUpload.
 */
async function pumpToMultipart(bucket: R2Bucket, mp: R2MultipartUpload, stream: ReadableStream<Uint8Array>) {
  const reader = stream.getReader();

  const PART_SIZE = 16 * 1024 * 1024; // 16MB parts
  let partNumber = 1;
  const etags: R2UploadedPart[] = [];

  // Assemble parts
  let current: Uint8Array[] = [];
  let currentBytes = 0;

  async function flushPart(final = false) {
    if (currentBytes === 0 && !final) return;

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
    if (value && value.byteLength) {
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

function toInt(s: string | undefined, def: number) {
  const n = s ? parseInt(s, 10) : NaN;
  return Number.isFinite(n) ? n : def;
}
