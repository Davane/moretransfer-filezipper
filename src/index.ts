import {
  Env,
  QueueMessage,
  QueueMessageType,
  RequestPath,
  ZipJob,
} from "./lib/types/types";
import { verifyHmac } from "./lib/crypto";
import { WebAPIService } from "./modules/web-api-service";
import { Zipper } from "./modules/zipper";
import { CronHandler } from "./modules/cron";

// export type { Env } from "./lib/types/types"

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
      const message: QueueMessage = { type: QueueMessageType.ZIP, data: job };
      console.log("Sending job to queue:", JSON.stringify(message));
      
      await env.QUEUE_WORKER_MAIN.send(message);
      console.log("Job queued", JSON.stringify(message));
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

      default:
        console.warn(`[scheduled] Unhandled cron schedule: "${event.cron}"`);
    }
  },

  async queue(batch: MessageBatch<QueueMessage>, env: Env, ctx: ExecutionContext) {
    const webAPIService = new WebAPIService(env.SECRET_KEY, env.WEB_API_BASE_URL);

    for (const msg of batch.messages) {
      console.log(`Processing message ${msg.id} from batch`, JSON.stringify(msg.body));

      switch (msg.body.type) {
        case QueueMessageType.ZIP:
          await new Zipper(env).zip(msg, webAPIService);
          break;
        default:
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
