import { Env } from "../lib/types/types";
import { WebAPIService } from "./web-api-service";

enum ReminderEmailType {
  TransferExpiry = "transfer-expiry-reminder",
}

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

  async handleExpiryReminderCron(webAPIService: WebAPIService, timestamp: number) {
    const body = {
      type: ReminderEmailType.TransferExpiry,
      trigger: "scheduled",
      timestamp,
    };

    return await webAPIService
      .sendEmailNotificationRequest(body)
      .then(async (res) => {
        const text = await res.text();
        console.log(`[scheduled] Expiry reminder response: ${res.status}`, text);
      })
      .catch((err) => {
        console.error("[scheduled] Expiry reminder request failed:", err);
      });
  }
}
