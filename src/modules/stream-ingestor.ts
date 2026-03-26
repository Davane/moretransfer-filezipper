import { Env, QueueMessage, StreamIngestJob } from "../lib/types/types";
import { calculateExponentialBackoff } from "../lib/utils";

type CloudflareApiResponse<T> = {
  success: boolean;
  errors?: Array<{ code?: number; message?: string }>;
  messages?: Array<{ code?: number; message?: string }>;
  result?: T;
};

type StreamVideo = {
  uid?: string;
  created?: string;
  modified?: string;
  readyToStream?: boolean;
  preview?: string;
  thumbnail?: string;
  playback?: {
    hls?: string;
    dash?: string;
  };
  status?: {
    state?: string;
    pctComplete?: string;
    errorReasonCode?: string;
    errorReasonText?: string;
  };
  meta?: any;
};

export class StreamIngestor {
  constructor(private readonly env: Env) {}

  async ingest(msg: Message<QueueMessage>) {
    const job = msg.body.data as StreamIngestJob;

    try {
      const result = await this.copyFromMediaUrlToStreamService(job);
      console.log(
        `[stream] Created/updated Stream video for file ${job.fileId}: uid=${result.uid} state=${result.status?.state}`,
      );
      msg.ack();
    } catch (err) {
      const delaySeconds = calculateExponentialBackoff(
        msg.attempts,
        this.env.BASE_RETRY_DELAY_SECONDS,
      );
      console.error(
        `[stream] Ingest job failed. Attempt: ${msg.attempts}. Retry in ${delaySeconds} seconds:`,
        err,
      );
      msg.retry({ delaySeconds });
    }
  }

  private async copyFromMediaUrlToStreamService(job: StreamIngestJob): Promise<StreamVideo> {
    if (!this.env.CLOUDFLARE_ACCOUNT_ID || !this.env.CLOUDFLARE_STREAM_API_TOKEN) {
      throw new Error("Missing CLOUDFLARE_ACCOUNT_ID or CLOUDFLARE_STREAM_API_TOKEN");
    }

    const url = `https://api.cloudflare.com/client/v4/accounts/${this.env.CLOUDFLARE_ACCOUNT_ID}/stream/copy`;
    const body = {
      url: job.r2PresignedGetUrl,
      meta: job.meta,
      requireSignedURLs: false,
    };

    const res = await fetch(url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${this.env.CLOUDFLARE_STREAM_API_TOKEN}`,
      },
      body: JSON.stringify(body),
    });

    const data = (await res.json().catch(() => ({}))) as CloudflareApiResponse<StreamVideo>;
    if (!res.ok || !data.success || !data.result) {
      const errMsg =
        data?.errors?.map((e) => e.message).filter(Boolean).join("; ") ||
        `HTTP ${res.status} ${res.statusText}`;
      throw new Error(`[stream] Cloudflare API error: ${errMsg}`);
    }

    return data.result;
  }
}

