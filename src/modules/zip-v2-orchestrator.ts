/**
 * Pure ZIP v2 tick orchestration: `zipTickReduce` + command union.
 * No I/O, env, storage, or `fetch` — callers pass `ZipTickConfig` (incl. pre-sampled jitter).
 */

import type {
  Checkpoint,
  ErrorKind,
  JobStateRow,
  RunChunkResponse,
  TickJobResponse,
  UploadedPart,
} from "../lib/types/types";
import { computeBackoffSeconds } from "../lib/utils";

export type ZipTickState = {
  job: JobStateRow;
  checkpoint: Checkpoint;
};

/** Injected numbers only (caller supplies `jitterMsSample` from `jitterMs()` once per reduce call site). */
export type ZipTickConfig = {
  nowMs: number;
  maxConsecutiveFailures: number;
  retryBaseDelaySeconds: number;
  tickIntervalMs: number;
  cleanupTtlMs: number;
  jitterMsSample: number;
};

export type ZipTickCommand =
  | { type: "persist_job"; job: JobStateRow }
  | { type: "persist_checkpoint"; jobId: string; checkpoint: Checkpoint }
  | { type: "insert_uploaded_parts"; jobId: string; parts: UploadedPart[] }
  | { type: "reschedule_storage_alarm" }
  | {
      type: "notify_transfer_compression_failed";
      transferId: string;
      outputKey: string;
      manifestKey: string;
    };

export type ZipTickEvent =
  | { type: "tick_backoff_skips"; response: TickJobResponse }
  | {
      type: "apply_retryable_failure";
      holdStatus: "RUNNING" | "FINALIZING";
      errText: string;
      kind: ErrorKind;
    }
  | { type: "chunk_run_success"; run: RunChunkResponse }
  | { type: "schedule_next_tick_interval" }
  | { type: "output_head_exists_done" }
  | {
      type: "chunk_failed_terminal";
      errText: string;
      kind: ErrorKind;
      consecutiveFailures: number;
    };

export type ZipReduceResult = {
  next: ZipTickState;
  commands: ZipTickCommand[];
  /** When backoff was applied (same semantics as previous `applyRetryableBackoff` return). */
  retryBackoff?: { retryAfterSeconds: number };
  /** Caller must run existing terminal path (throw / failFinalize) without persisting retry row. */
  retryExhausted?: boolean;
  /** Early HTTP tick response (backoff skip); no commands. */
  tickResponse?: TickJobResponse;
};

export function zipTickReduce(
  prev: ZipTickState,
  event: ZipTickEvent,
  cfg: ZipTickConfig,
): ZipReduceResult {
  switch (event.type) {
    case "tick_backoff_skips":
      return {
        next: prev,
        commands: [],
        tickResponse: event.response,
      };

    case "apply_retryable_failure": {
      const { job, checkpoint } = prev;
      const consecutiveFailures = (job.consecutiveFailures ?? 0) + 1;
      if (consecutiveFailures >= cfg.maxConsecutiveFailures) {
        return { next: prev, commands: [], retryExhausted: true };
      }
      const backoffSeconds = computeBackoffSeconds(consecutiveFailures, cfg.retryBaseDelaySeconds);
      const backoffUntilMs = cfg.nowMs + backoffSeconds * 1000 + cfg.jitterMsSample;
      const nextActionAtMs = Math.max(cfg.nowMs + cfg.tickIntervalMs, backoffUntilMs);
      const nextJob: JobStateRow = {
        ...job,
        status: event.holdStatus,
        updatedAtMs: cfg.nowMs,
        errorMessage: event.errText,
        consecutiveFailures,
        lastFailureAtMs: cfg.nowMs,
        lastErrorKind: event.kind,
        nextActionAtMs,
      };
      return {
        next: { job: nextJob, checkpoint },
        commands: [{ type: "persist_job", job: nextJob }, { type: "reschedule_storage_alarm" }],
        retryBackoff: { retryAfterSeconds: backoffSeconds },
      };
    }

    case "schedule_next_tick_interval": {
      const { job, checkpoint } = prev;
      const nextActionAtMs = cfg.nowMs + Math.max(0, cfg.tickIntervalMs);
      const nextJob: JobStateRow = {
        ...job,
        updatedAtMs: cfg.nowMs,
        nextActionAtMs,
      };
      return {
        next: { job: nextJob, checkpoint },
        commands: [{ type: "persist_job", job: nextJob }, { type: "reschedule_storage_alarm" }],
      };
    }

    case "output_head_exists_done": {
      const { job, checkpoint } = prev;
      const nextJob: JobStateRow = {
        ...job,
        status: "DONE",
        updatedAtMs: cfg.nowMs,
        nextActionAtMs: undefined,
      };
      return {
        next: { job: nextJob, checkpoint },
        commands: [{ type: "persist_job", job: nextJob }, { type: "reschedule_storage_alarm" }],
      };
    }

    case "chunk_run_success": {
      const { job, checkpoint } = prev;
      const run = event.run;
      let j = job;
      if (j.consecutiveFailures > 0 || j.nextActionAtMs || j.lastErrorKind) {
        j = {
          ...j,
          consecutiveFailures: 0,
          nextActionAtMs: undefined,
          lastFailureAtMs: undefined,
          lastErrorKind: undefined,
          updatedAtMs: cfg.nowMs,
        };
      }
      const nextCheckpoint: Checkpoint = {
        ...checkpoint,
        nextPartNumber: run.nextPartNumber,
        fileIndex: run.fileIndex,
        zipOffset: run.zipOffset,
        bytesWrittenTotal: run.bytesWrittenTotal,
        filesDone: run.filesDone,
        done: run.done,
      };
      const nextJob: JobStateRow = {
        ...j,
        status: run.done ? "FINALIZING" : "RUNNING",
        updatedAtMs: cfg.nowMs,
      };
      const commands: ZipTickCommand[] = [];
      if (run.uploadedParts?.length) {
        commands.push({
          type: "insert_uploaded_parts",
          jobId: job.jobId,
          parts: run.uploadedParts,
        });
      }
      commands.push(
        { type: "persist_checkpoint", jobId: job.jobId, checkpoint: nextCheckpoint },
        { type: "persist_job", job: nextJob },
      );
      return {
        next: { job: nextJob, checkpoint: nextCheckpoint },
        commands,
      };
    }

    case "chunk_failed_terminal": {
      const { job, checkpoint } = prev;
      const cleanupAtMs = cfg.nowMs + cfg.cleanupTtlMs;
      const nextJob: JobStateRow = {
        ...job,
        status: "FAILED",
        updatedAtMs: cfg.nowMs,
        errorMessage: event.errText,
        consecutiveFailures: event.consecutiveFailures,
        lastFailureAtMs: cfg.nowMs,
        lastErrorKind: event.kind,
        cleanupAtMs,
        nextActionAtMs: undefined,
      };
      const commands: ZipTickCommand[] = [
        { type: "persist_job", job: nextJob },
        { type: "reschedule_storage_alarm" },
        {
          type: "notify_transfer_compression_failed",
          transferId: job.transferId,
          outputKey: checkpoint.outputKey,
          manifestKey: checkpoint.manifestKey,
        },
      ];
      return {
        next: { job: nextJob, checkpoint },
        commands,
      };
    }

  }
}
