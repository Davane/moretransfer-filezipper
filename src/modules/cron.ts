import { Env } from "../lib/types/types";
import { WebAPIService } from "./web-api-service";

export class CronHandler {
  constructor(private readonly env: Env) {}

  async handleCleanupExpiredTransfersCron(webAPIService: WebAPIService, timestamp: number) {
    const body = { trigger: "scheduled", timestamp };

    return await webAPIService
      .sendCleanupExpiredTransfersRequest(body)
      .then(async (res) => {
        const text = await res.text();
        console.log(`[scheduled] Cleanup response: ${res.status}`, text);
      })
      .catch((err) => {
        console.error("[scheduled] Cleanup request failed:", err);
      });
  }
}
