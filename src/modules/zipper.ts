import {
  Env,
  QueueMessage,
  TransferStatus,
  TransferUpdateRequest,
  ZipJob,
} from "../lib/types/types";
import { WebAPIService } from "./web-api-service";
import { calculateExponentialBackoff } from "../lib/utils";
import { R2MultipartSink, Zip64StoreWriter } from "../lib/zip64";

const DEFAULT_MAX_FILES = 500;

/** 64 GiB — must stay in sync with wrangler MAX_ZIP_BYTES for large bundles. */
const DEFAULT_MAX_ZIP_BYTES = 64 * 1024 * 1024 * 1024; // 64 GiB

export class Zipper {
  constructor(private readonly env: Env) {}

  async zip(msg: Message<QueueMessage>, webAPIService: WebAPIService): Promise<void> {
    const messagePayload = msg.body.data as ZipJob;

    let transferStatusPayload: TransferUpdateRequest = {
      status: TransferStatus.READY_BUT_COMPRESSION_FAILED,
    };

    try {
      const bundleObjectKey = await this.processZipJob(messagePayload, this.env);

      transferStatusPayload = {
        status: TransferStatus.READY,
        bundleObjectKey,
      };

      msg.ack();
    } catch (err) {
      const delaySeconds = calculateExponentialBackoff(
        msg.attempts,
        this.env.BASE_RETRY_DELAY_SECONDS,
      );
      console.error(
        `Compression job failed. Attempt: ${msg.attempts}. Retry in ${delaySeconds} seconds:`,
        err,
      );

      msg.retry({ delaySeconds });
    } finally {
      if (this.env.SKIP_REQUEST_VERIFICATION) {
        console.warn(`Skipping transfer status update for ${messagePayload.transferId}`);
      } else {
        try {
          await webAPIService.updateTransferStatus(
            messagePayload.transferId,
            transferStatusPayload,
          );
        } catch (error) {
          console.warn(`Failed to update transfer status ${messagePayload.transferId}:`, error);
        }
      }
    }
  }

  private async processZipJob(job: ZipJob, env: Env) {
    const currentObjectPrefix = this.cleanPrefix(job.objectPrefix);
    const defaultZipOutputKey = `${env.ZIP_OUTPUT_PREFIX}/${currentObjectPrefix.replace(
      /\/?$/,
      "",
    )}/${env.ZIP_OUTPUT_FILE_NAME}.zip`;

    const zipOutputKey = job.zipOutputKey ?? defaultZipOutputKey;
    const maxFiles = this.toInt(env.MAX_FILES, DEFAULT_MAX_FILES);
    const maxZipBytes = this.toInt(env.MAX_ZIP_BYTES, DEFAULT_MAX_ZIP_BYTES);

    const existing = await env.OUTPUT_BUCKET.head(zipOutputKey);
    if (existing) {
      console.log(`ZIP already exists: ${zipOutputKey}, skipping...`);
      return zipOutputKey;
    }

    const id = env.ZipLocks.idFromName(currentObjectPrefix);
    const stub = env.ZipLocks.get(id);

    console.log(`Locking source object key [${currentObjectPrefix}]. Stub ID [${id}].`);
    const lockResp = await stub.fetch("https://lock/lock", { method: "POST" });
    if (!lockResp.ok) {
      console.error(`Already locked: ${currentObjectPrefix}.`);
      throw new Error(`Prefix is locked: ${currentObjectPrefix}`);
    }

    let multipartUpload: R2MultipartUpload | undefined;
    let zip: Zip64StoreWriter | undefined;

    try {
      multipartUpload = await env.OUTPUT_BUCKET.createMultipartUpload(zipOutputKey, {
        customMetadata: {
          prefix: currentObjectPrefix,
          createdBy: job.createdBy ?? "worker",
          zipVersion: "zip64-store-v1",
        },
      });

      const sink = new R2MultipartSink(multipartUpload, {
        partSize: 32 * 1024 * 1024,
      });
      zip = new Zip64StoreWriter(sink);

      const listed: R2Objects = await env.SOURCE_BUCKET.list({
        prefix: currentObjectPrefix,
        limit: 500,
      });

      console.log(`Listing ${listed.objects.length} objects for prefix: ${currentObjectPrefix}`);
      if (listed.objects.length === 0) {
        console.log(`No objects found for prefix: ${currentObjectPrefix}`);
        throw new Error(`No objects found for prefix: ${currentObjectPrefix}`);
      }

      const filePathMap = new Map((job.files ?? []).map((f) => [f.key, f.relativePath]));

      const totals = { totalFiles: 0, totalBytes: 0 };
      await this.streamPrefixObjectsIntoZip({
        env,
        zip,
        job,
        currentObjectPrefix,
        filePathMap,
        maxFiles,
        maxZipBytes,
        listed,
        totals,
      });

      const output = await zip.close();
      console.log("ZIP64 completed:", output.key, "ETag:", output.etag);
      return output.key;
    } catch (error) {
      console.error("ZIP64 job failed:", error);
      if (zip) {
        await zip.abort().catch(() => {});
      } else if (multipartUpload) {
        await multipartUpload.abort().catch(() => {});
      }
      throw error;
    } finally {
      await stub.fetch("https://lock/unlock", { method: "POST" });
    }
  }

  private cleanPrefix(p: string) {
    return p.replace(/^\/+/, "").replace(/\/?$/, "/");
  }

  private toInt(s: string | undefined, defaultValue: number) {
    const n = s ? Number.parseInt(s, 10) : Number.NaN;
    return Number.isFinite(n) ? n : defaultValue;
  }

  /** Paginates `list` under `currentObjectPrefix` and appends each object to the ZIP. */
  private async streamPrefixObjectsIntoZip(args: {
    env: Env;
    zip: Zip64StoreWriter;
    job: ZipJob;
    currentObjectPrefix: string;
    filePathMap: Map<string, string | undefined>;
    maxFiles: number;
    maxZipBytes: number;
    listed: R2Objects;
    totals: { totalFiles: number; totalBytes: number };
  }): Promise<void> {
    const {
      env,
      zip,
      job,
      currentObjectPrefix,
      filePathMap,
      maxFiles,
      maxZipBytes,
      totals,
    } = args;
    let listed = args.listed;

    while (true) {
      for (const obj of listed.objects) {
        await this.appendListedObjectIfPresent({
          env,
          zip,
          job,
          currentObjectPrefix,
          filePathMap,
          maxFiles,
          maxZipBytes,
          obj,
          totals,
        });
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
  }

  private async appendListedObjectIfPresent(args: {
    env: Env;
    zip: Zip64StoreWriter;
    job: ZipJob;
    currentObjectPrefix: string;
    filePathMap: Map<string, string | undefined>;
    maxFiles: number;
    maxZipBytes: number;
    obj: R2Object;
    totals: { totalFiles: number; totalBytes: number };
  }): Promise<void> {
    const {
      env,
      zip,
      job,
      currentObjectPrefix,
      filePathMap,
      maxFiles,
      maxZipBytes,
      obj,
      totals,
    } = args;

    if (totals.totalFiles >= maxFiles) {
      console.error(`File count limit hit: ${maxFiles}`);
      throw new Error(`File count limit hit: ${maxFiles}`);
    }

    if (!job.includeEmpty && obj.size === 0) {
      return;
    }

    totals.totalFiles++;
    totals.totalBytes += obj.size;

    if (totals.totalBytes > maxZipBytes) {
      console.error(
        `ZIP size limit hit: ${maxZipBytes} bytes. Current total is ${totals.totalBytes}`,
      );
      throw new Error(`ZIP size limit hit: ${maxZipBytes} bytes`);
    }

    const key = obj.key;
    const nameInZip = this.resolveNameInZip(key, currentObjectPrefix, filePathMap.get(key));

    const r = await env.SOURCE_BUCKET.get(key);
    if (!r?.body) {
      console.warn(`Object is missing from source bucket: ${key}`);
      return;
    }

    console.log(`Adding file to ZIP64: ${nameInZip} (${obj.size} bytes)`);
    await zip.addFile(nameInZip, r.body, obj.size);
  }

  private resolveNameInZip(
    key: string,
    currentObjectPrefix: string,
    relativePath: string | undefined,
  ): string {
    if (relativePath) {
      return relativePath;
    }
    const delimiterIndex = key.indexOf("__");
    const uploadedFileName =
      delimiterIndex >= 0 ? key.slice(delimiterIndex + 2, key.length) : "";
    return (
      uploadedFileName ||
      key.substring(currentObjectPrefix.length).replace(/^\/+/, "") ||
      key.split("/").pop() ||
      "file"
    );
  }
}
