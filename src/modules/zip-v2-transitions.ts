/**
 * ZIP v2 explicit transition table (option 1): pure lookup + effect list.
 * No I/O, env, storage, or fetch — callers execute `ZipEffect` in a fixed order.
 *
 * Backoff math uses `computeBackoffSeconds` from `../lib/utils` (single source of truth).
 */

import type {
  Checkpoint,
  ErrorKind,
  JobStateRow,
  JobStatus,
  RunChunkResponse,
  TickJobResponse,
  UploadedPart,
} from "../lib/types/types";
import { computeBackoffSeconds } from "../lib/utils";

/** Statuses that participate in the tick state machine (terminal rows are interpreter-only). */
export type ZipActiveStatus = "PENDING" | "RUNNING" | "FINALIZING";

export type ZipTableEvent =
  | { type: "output_exists" }
  | { type: "schedule_next_tick_interval" }
  | { type: "chunk_run_success"; run: RunChunkResponse }
  | {
      type: "retryable_backoff";
      holdStatus: "RUNNING" | "FINALIZING";
      errText: string;
      kind: ErrorKind;
    }
  | {
      type: "chunk_terminal_failure";
      errText: string;
      kind: ErrorKind;
      consecutiveFailures: number;
    };

/** Data-only side effects; `JobManagerDO` runs them in `executeZipEffects` order. */
export type ZipEffect =
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

/** Clock + caps injected by the interpreter (incl. one pre-sampled jitter for backoff). */
export type ZipTransitionClockConfig = {
  nowMs: number;
  maxConsecutiveFailures: number;
  retryBaseDelaySeconds: number;
  tickIntervalMs: number;
  cleanupTtlMs: number;
  jitterMsSample: number;
};

export type ZipTickState = {
  job: JobStateRow;
  checkpoint: Checkpoint;
};

export type ZipTransitionResult =
  | { kind: "effects"; effects: ZipEffect[]; tickResponse?: TickJobResponse }
  /** Consecutive failure cap exceeded — caller must terminal-fail or rethrow. */
  | { kind: "retry_exhausted" };

/** Human-readable matrix rows (documentation + exhaustive test keys). */
export const ZIP_TICK_TRANSITIONS = [
  {
    statuses: ["PENDING", "RUNNING"] as const,
    event: "output_exists" as const,
    doc: "R2 output already present before chunking → DONE + alarm.",
  },
  {
    statuses: ["PENDING", "RUNNING"] as const,
    event: "schedule_next_tick_interval" as const,
    doc: "Semaphore busy or post-chunk schedule → bump nextActionAtMs + alarm.",
  },
  {
    statuses: ["PENDING", "RUNNING"] as const,
    event: "chunk_run_success" as const,
    doc: "Container chunk ok → parts + checkpoint + job (RUNNING or FINALIZING).",
  },
  {
    statuses: ["PENDING", "RUNNING", "FINALIZING"] as const,
    event: "retryable_backoff" as const,
    doc: "Classified retryable error → increment failures, backoff nextActionAtMs + alarm (or exhaust).",
  },
  {
    statuses: ["PENDING", "RUNNING", "FINALIZING"] as const,
    event: "chunk_terminal_failure" as const,
    doc: "Terminal chunk/finalize budget path → FAILED + cleanup fields + alarm + notify.",
  },
] as const satisfies ReadonlyArray<{
  statuses: readonly ZipActiveStatus[];
  event: ZipTableEvent["type"];
  doc: string;
}>;

export type ZipTransitionTableRow = (typeof ZIP_TICK_TRANSITIONS)[number];
/** Alias aligned with the plan doc (“each transition row”). */
export type TransitionRow = ZipTransitionTableRow;

function assertActiveStatus(status: JobStatus): asserts status is ZipActiveStatus {
  if (status !== "PENDING" && status !== "RUNNING" && status !== "FINALIZING") {
    throw new Error(`lookupZipTransition: unsupported status ${status}`);
  }
}

function rowCovers(status: ZipActiveStatus, eventType: ZipTableEvent["type"]): boolean {
  return ZIP_TICK_TRANSITIONS.some(
    (r) => r.event === eventType && (r.statuses as readonly string[]).includes(status),
  );
}

/**
 * Declarative routing: `(persisted status, classified tick event)` → effects (+ optional tick response).
 * Unknown or illegal pairs throw — fail loud when the interpreter and table drift.
 */
export function lookupZipTransition(
  state: ZipTickState,
  status: JobStatus,
  event: ZipTableEvent,
  clock: ZipTransitionClockConfig,
): ZipTransitionResult {
  assertActiveStatus(status);
  const eventType = event.type;
  if (!rowCovers(status, eventType)) {
    throw new Error(`lookupZipTransition: no ZIP_TICK_TRANSITIONS row for (${status}, ${eventType})`);
  }

  const { job, checkpoint } = state;

  switch (event.type) {
    case "output_exists": {
      const nextJob: JobStateRow = {
        ...job,
        status: "DONE",
        updatedAtMs: clock.nowMs,
        nextActionAtMs: undefined,
      };
      return {
        kind: "effects",
        effects: [
          { type: "persist_job", job: nextJob },
          { type: "reschedule_storage_alarm" },
        ],
      };
    }

    case "schedule_next_tick_interval": {
      const nextActionAtMs = clock.nowMs + Math.max(0, clock.tickIntervalMs);
      const nextJob: JobStateRow = {
        ...job,
        updatedAtMs: clock.nowMs,
        nextActionAtMs,
      };
      return {
        kind: "effects",
        effects: [
          { type: "persist_job", job: nextJob },
          { type: "reschedule_storage_alarm" },
        ],
      };
    }

    case "chunk_run_success": {
      const { run } = event;
      let j = job;
      if (j.consecutiveFailures > 0 || j.nextActionAtMs || j.lastErrorKind) {
        j = {
          ...j,
          consecutiveFailures: 0,
          nextActionAtMs: undefined,
          lastFailureAtMs: undefined,
          lastErrorKind: undefined,
          updatedAtMs: clock.nowMs,
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
        updatedAtMs: clock.nowMs,
      };
      const effects: ZipEffect[] = [];
      const parts = run.uploadedParts ?? [];
      if (parts.length) {
        effects.push({ type: "insert_uploaded_parts", jobId: job.jobId, parts });
      }
      effects.push(
        { type: "persist_checkpoint", jobId: job.jobId, checkpoint: nextCheckpoint },
        { type: "persist_job", job: nextJob },
      );
      return { kind: "effects", effects };
    }

    case "retryable_backoff": {
      const consecutiveFailures = (job.consecutiveFailures ?? 0) + 1;
      if (consecutiveFailures >= clock.maxConsecutiveFailures) {
        return { kind: "retry_exhausted" };
      }
      const backoffSeconds = computeBackoffSeconds(consecutiveFailures, clock.retryBaseDelaySeconds);
      const backoffUntilMs = clock.nowMs + backoffSeconds * 1000 + clock.jitterMsSample;
      const nextActionAtMs = Math.max(clock.nowMs + clock.tickIntervalMs, backoffUntilMs);
      const nextJob: JobStateRow = {
        ...job,
        status: event.holdStatus,
        updatedAtMs: clock.nowMs,
        errorMessage: event.errText,
        consecutiveFailures,
        lastFailureAtMs: clock.nowMs,
        lastErrorKind: event.kind,
        nextActionAtMs,
      };
      return {
        kind: "effects",
        effects: [
          { type: "persist_job", job: nextJob },
          { type: "reschedule_storage_alarm" },
        ],
        tickResponse: {
          status: event.holdStatus,
          done: false,
          retryAfterSeconds: backoffSeconds,
        },
      };
    }

    case "chunk_terminal_failure": {
      const cleanupAtMs = clock.nowMs + clock.cleanupTtlMs;
      const nextJob: JobStateRow = {
        ...job,
        status: "FAILED",
        updatedAtMs: clock.nowMs,
        errorMessage: event.errText,
        consecutiveFailures: event.consecutiveFailures,
        lastFailureAtMs: clock.nowMs,
        lastErrorKind: event.kind,
        cleanupAtMs,
        nextActionAtMs: undefined,
      };
      return {
        kind: "effects",
        effects: [
          { type: "persist_job", job: nextJob },
          { type: "reschedule_storage_alarm" },
          {
            type: "notify_transfer_compression_failed",
            transferId: job.transferId,
            outputKey: checkpoint.outputKey,
            manifestKey: checkpoint.manifestKey,
          },
        ],
      };
    }

  }
}

/**
 * Executes `ZipEffect`s in a single documented order (persist data before alarm / web callbacks).
 */
export const ZIP_EFFECT_EXECUTION_ORDER_DOC = [
  "insert_uploaded_parts",
  "persist_checkpoint",
  "persist_job",
  "reschedule_storage_alarm",
  "notify_transfer_compression_failed",
] as const;

/** Flatten `ZIP_TICK_TRANSITIONS` for exhaustive tests (status × declared event type). */
export function zipTransitionDeclaredPairs(): Array<{
  status: ZipActiveStatus;
  eventType: ZipTableEvent["type"];
}> {
  const out: Array<{ status: ZipActiveStatus; eventType: ZipTableEvent["type"] }> = [];
  for (const row of ZIP_TICK_TRANSITIONS) {
    for (const s of row.statuses) {
      out.push({ status: s, eventType: row.event });
    }
  }
  return out;
}

/*
| status     | event                        | outcome |
|------------|------------------------------|---------|
| PENDING    | output_exists                | DONE + alarm |
| RUNNING    | output_exists                | DONE + alarm |
| PENDING    | schedule_next_tick_interval  | nextAction + alarm |
| RUNNING    | schedule_next_tick_interval  | nextAction + alarm |
| PENDING    | chunk_run_success            | parts + cp + job |
| RUNNING    | chunk_run_success            | parts + cp + job |
| PENDING    | retryable_backoff            | backoff row or retry_exhausted |
| RUNNING    | retryable_backoff            | backoff row or retry_exhausted |
| FINALIZING | retryable_backoff            | backoff row or retry_exhausted |
| PENDING    | chunk_terminal_failure       | FAILED + alarm + notify |
| RUNNING    | chunk_terminal_failure       | FAILED + alarm + notify |
| FINALIZING | chunk_terminal_failure       | FAILED + alarm + notify |
*/
