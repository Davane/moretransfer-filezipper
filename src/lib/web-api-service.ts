import { COMPANY_NAME } from "./constants";
import { createHmacSha256Hex } from "./crypto";
import { TransferUpdateRequest } from "./types/types";
import { createSafeUploadKey, fetchWithCredentials, slugify } from "./utils";

export enum NextApiErrorCode {
  TransferSessionException = "TransferSessionException",
  RegisterFilesException = "RegisterFilesException",
  SignPartException = "SignPartException",
  CompleteFileUploadException = "CompleteFileUploadException",
  CompleteTransferUploadException = "CompleteTransferUploadException",
  DownloadFileException = "DownloadFileException",
}

class WebAPIService {
  private readonly _secret: string;
  private readonly _baseUrl: string;

  constructor(secret: string, baseUrl: string) {
    this._secret = secret;
    this._baseUrl = baseUrl;
  }

  async getHeaderSignature(body: Record<string, any>) {
    if (!this._secret) {
      throw new Error("Missing SECRET_KEY");
    }

    const ts = Date.now().toString();
    const rawJson = JSON.stringify(body);
    const message = `${ts}\n${rawJson}`;

    const sig = await createHmacSha256Hex(message, this._secret);

    return {
      "x-timestamp": ts,
      "x-signature": sig,
    };
  }

  async updateTransferStatus(transferId: string, request: TransferUpdateRequest) {
    const headers = await this.getHeaderSignature(request);
    const url = `${this._baseUrl}/api/transfers/${transferId}/compression-status`;
    console.log(`Updating transfer status for ${transferId}:`, url, JSON.stringify(request));

    try {
      const uploadMeta = await fetchWithCredentials<any>(url, {
        method: "PUT",
        body: JSON.stringify(request),
        headers,
      });
      return uploadMeta;
    } catch (error: any) {
      console.log(`Error updating transfer status for ${transferId}:`, error);
      throw error;
    }
  }

  /**
   * Get the payload for the file compression job
   */
  getEnqueueCompressionPayload(transfer: { fileName: string; id: string }) {
    const transferId = transfer.id;
    const objectFullPath = createSafeUploadKey(transferId, transfer.fileName ?? "");
    const objectPrefix = objectFullPath.substring(0, objectFullPath.lastIndexOf("/") + 1);

    const date = new Date().toISOString().slice(0, 10);
    const filename = transfer.fileName ?? "bundle";

    const outputKey = createSafeUploadKey(
      transferId,
      `${slugify(COMPANY_NAME)}_${filename}_${date}.zip`,
      "compressed"
    );

    const payload = {
      objectPrefix,
      zipOutputKey: outputKey,
      createdBy: "next-api",
    };

    console.info(`Enqueuing zip for transfer ${transferId}: ${JSON.stringify(payload)}`);

    return payload;
  }
}

export { WebAPIService };
