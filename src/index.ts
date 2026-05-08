import {
  Env,
  QueueMessage,
  QueueMessageType,
  RequestPath,
  ZipJob,
  ZipV2TickMessageData,
} from "./lib/types/types";
import { verifyHmac } from "./lib/crypto";
import { WebAPIService } from "./modules/web-api-service";
import { Zipper } from "./modules/zipper";
import { CronHandler } from "./modules/cron";
import { StreamIngestor } from "./modules/stream-ingestor";
import { resolveOutputKey, writeZipManifest, toBool } from "./modules/job-manifest";
import { JobManagerDO } from "./modules/job-manager-do";
import { ZipSemaphoreDO } from "./modules/semaphore-do";
import { ZipContainerDO } from "./modules/zip-container";

export { ContainerProxy } from "@cloudflare/containers";

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

    if (![RequestPath.COMPRESS_FILES, RequestPath.STREAM_INGEST].includes(url.pathname as any)) {
      return new Response(undefined, { status: 404 });
    }

    if (env.SKIP_REQUEST_VERIFICATION) {
      console.warn("Skipping request verification");
    } else {
      try {
        await verifyHmac(req, env.SECRET_KEY);
      } catch (error: any) {
        console.error("HMAC verification failed:", error?.message, error);
        return new Response("Unauthorized", { status: 401 });
      }
    }

    try {
      if (url.pathname === RequestPath.COMPRESS_FILES) {
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

        const useV2 = toBool(env.ZIP_USE_CONTAINERS, false);
        if (!useV2) {
          const message: QueueMessage = { type: QueueMessageType.ZIP, data: job };
          console.log("Sending job to queue:", JSON.stringify(message));
          await env.QUEUE_WORKER_MAIN.send(message);
          console.log("Job queued", JSON.stringify(message));
        } else {
          // Stable ID so repeated triggers resume the same JobManagerDO state.
          // One ZIP v2 job per transfer.
          const jobId = job.transferId;
          const outputKey = resolveOutputKey(env, job);

          // Idempotent start: if the output already exists, do not restart work.
          const existingOut = await env.OUTPUT_BUCKET.head(outputKey);
          if (existingOut) {
            console.log(`[zip-v2] Output already exists; skipping start.`, {
              jobId,
              transferId: job.transferId,
              outputKey,
              outputBytes: existingOut.size,
            });
            return new Response("Enqueued", { status: 202 });
          }

          const { manifestKey } = await writeZipManifest({
            env,
            jobId,
            zipJob: job,
            outputKey,
          });

          // Start a new zip v2 job by forwarding to JobManagerDO
          const jobManagerId = env.JobManager.idFromName(jobId);
          const jobManager = env.JobManager.get(jobManagerId);
          await jobManager.fetch("https://job/start", {
            method: "POST",
            body: JSON.stringify({
              jobId,
              transferId: job.transferId,
              manifestKey,
              outputKey,
            }),
          });

          // Enqueue a tick message to the queue to start the zip v2 job
          const tick: ZipV2TickMessageData = { jobId };
          const message: QueueMessage = { 
            type: QueueMessageType.ZIP_V2_TICK, 
            data: tick 
          };
          console.log("Sending zip v2 tick to queue:", JSON.stringify(message));
          
          await env.QUEUE_WORKER_MAIN.send(message);
          console.log("Zip v2 job queued", JSON.stringify({ jobId, manifestKey, outputKey }));
        }
      } else if (url.pathname === RequestPath.STREAM_INGEST) {
        const body = await req.json<any>();
        console.log("Stream ingest request:", JSON.stringify(body));

        if (!body?.transferId || !body?.fileId || !body?.r2PresignedGetUrl) {
          return new Response("Missing required fields", { status: 400 });
        }

        const message: QueueMessage = { type: QueueMessageType.STREAM_INGEST, data: body };
        console.log("Sending stream ingest job to queue:", JSON.stringify(message));
        await env.QUEUE_WORKER_MAIN.send(message);
        console.log("Stream ingest job queued", JSON.stringify(message));
      }
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
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    const now = Date.now();
    console.log(`[scheduled] Cron triggered: "${event.cron}" at ${now}`);

    const cronHandler = new CronHandler(env);
    const webAPIService = new WebAPIService(env.SECRET_KEY, env.WEB_API_BASE_URL);

    switch (event.cron) {
      // Every 3 hours
      case "0 */3 * * *": {
        ctx.waitUntil(cronHandler.handleCleanupExpiredTransfersCron(webAPIService, now));
        break;
      }

      // Daily at 9 AM UTC
      case "0 9 * * *": {
        ctx.waitUntil(cronHandler.handleExpiryReminderCron(webAPIService, now));
        break;
      }

      // Every 30 minutes
      case "*/30 * * * *": {
        ctx.waitUntil(cronHandler.handleReviewCommentDigestCron(webAPIService, now));
        break;
      }

      default:
        console.warn(`[scheduled] Unhandled cron schedule: "${event.cron}"`);
    }
  },

  async queue(batch: MessageBatch<QueueMessage>, env: Env, ctx: ExecutionContext) {
    const webAPIService = new WebAPIService(env.SECRET_KEY, env.WEB_API_BASE_URL);

    for (const msg of batch.messages) {
      console.log(`Processing message ${msg.id} from batch`, JSON.stringify(msg.body));

      if (msg.body.type === QueueMessageType.ZIP) {
        await new Zipper(env).zip(msg, webAPIService);
      } else if (msg.body.type === QueueMessageType.ZIP_V2_TICK) {
        const { jobId } = msg.body.data;

        try {

          // Handle the zip v2 tick message by forwarding to JobManagerDO
          const id = env.JobManager.idFromName(jobId);
          const stub = env.JobManager.get(id);
          const resp = await stub.fetch("https://job/tick", {
            method: "POST",
            body: JSON.stringify({ jobId }),
          });

          if (!resp.ok) {
            throw new Error(`JobManager tick failed: ${resp.status} ${await resp.text()}`);
          }

          // Always ack on normal progress. JobManagerDO will schedule the next tick via alarm.
          // Only use queue retry for real failures/exceptions.
          await resp.json().catch(() => {});
          msg.ack();
        } catch (e) {
          console.error("[zip-v2] tick failed:", e);
          msg.retry({ delaySeconds: 30 });
        }
      } else if (msg.body.type === QueueMessageType.STREAM_INGEST) {
        await new StreamIngestor(env).ingest(msg);
      } else {
        console.error(`Unknown message type: ${(msg.body as any).type}`);
        msg.ack();
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

export { JobManagerDO, ZipSemaphoreDO, ZipContainerDO };
