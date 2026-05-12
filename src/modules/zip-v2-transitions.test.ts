import { describe, expect, it } from "vitest";
import {
  lookupZipTransition,
  zipTransitionDeclaredPairs,
  ZIP_TICK_TRANSITIONS,
  type ZipTickState,
  type ZipTableEvent,
  type ZipTransitionClockConfig,
} from "./zip-v2-transitions";
import { computeBackoffSeconds } from "../lib/utils";
import type { Checkpoint, JobStateRow, RunChunkResponse } from "../lib/types/types";

function clock(over: Partial<ZipTransitionClockConfig> = {}): ZipTransitionClockConfig {
  return {
    nowMs: 1_000_000,
    maxConsecutiveFailures: 5,
    retryBaseDelaySeconds: 30,
    tickIntervalMs: 5_000,
    cleanupTtlMs: 86_400_000,
    jitterMsSample: 100,
    ...over,
  };
}

function state(job: Partial<JobStateRow> = {}, cp: Partial<Checkpoint> = {}): ZipTickState {
  const j: JobStateRow = {
    jobId: "j1",
    transferId: "t1",
    status: "RUNNING",
    createdAtMs: 1,
    updatedAtMs: 2,
    consecutiveFailures: 0,
    ...job,
  };
  const c: Checkpoint = {
    manifestKey: "m",
    outputKey: "o.zip",
    uploadId: "u1",
    partSize: 1,
    nextPartNumber: 1,
    fileIndex: 0,
    zipOffset: "0",
    bytesWrittenTotal: "0",
    filesDone: 0,
    done: false,
    ...cp,
  };
  return { job: j, checkpoint: c };
}

describe("ZIP_TICK_TRANSITIONS", () => {
  it("has stable row metadata for scanning", () => {
    expect(ZIP_TICK_TRANSITIONS.length).toBeGreaterThan(0);
    for (const r of ZIP_TICK_TRANSITIONS) {
      expect(r.doc.length).toBeGreaterThan(0);
      expect(r.statuses.length).toBeGreaterThan(0);
    }
  });
});

describe("lookupZipTransition", () => {
  it("resolves every declared (status, eventType) pair without throwing", () => {
    const pairs = zipTransitionDeclaredPairs();
    for (const { status, eventType } of pairs) {
      const s = state({ status });
      const cfg = clock();
      let ev: ZipTableEvent;
      switch (eventType) {
        case "output_exists":
          ev = { type: "output_exists" };
          break;
        case "schedule_next_tick_interval":
          ev = { type: "schedule_next_tick_interval" };
          break;
        case "chunk_run_success":
          ev = {
            type: "chunk_run_success",
            run: {
              uploadedParts: [],
              nextPartNumber: 2,
              fileIndex: 0,
              zipOffset: "1",
              bytesWrittenTotal: "1",
              filesDone: 0,
              done: false,
            } satisfies RunChunkResponse,
          };
          break;
        case "retryable_backoff":
          ev = {
            type: "retryable_backoff",
            holdStatus: status === "FINALIZING" ? "FINALIZING" : "RUNNING",
            errText: "e",
            kind: "container_5xx",
          };
          break;
        case "chunk_terminal_failure":
          ev = {
            type: "chunk_terminal_failure",
            errText: "e",
            kind: "bad_manifest",
            consecutiveFailures: 1,
          };
          break;
        default: {
          const _n: never = eventType;
          throw new Error(`unhandled event ${_n}`);
        }
      }
      if (eventType === "retryable_backoff" && s.job.consecutiveFailures >= cfg.maxConsecutiveFailures - 1) {
        s.job.consecutiveFailures = 0;
      }
      const r = lookupZipTransition(s, status, ev, cfg);
      expect(r.kind).toBeDefined();
    }
  });

  it("throws when no matrix row exists for (status, event)", () => {
    const s = state({ status: "FINALIZING" });
    const run: RunChunkResponse = {
      uploadedParts: [],
      nextPartNumber: 2,
      fileIndex: 0,
      zipOffset: "0",
      bytesWrittenTotal: "0",
      filesDone: 0,
      done: false,
    };
    expect(() =>
      lookupZipTransition(s, "FINALIZING", { type: "chunk_run_success", run }, clock()),
    ).toThrow(/no ZIP_TICK_TRANSITIONS row/);
  });

  it("retryable_backoff matches computeBackoffSeconds + max(interval, backoff+jitter)", () => {
    const cfg = clock();
    const s = state({ consecutiveFailures: 0 });
    const r = lookupZipTransition(
      s,
      "RUNNING",
      {
        type: "retryable_backoff",
        holdStatus: "RUNNING",
        errText: "x",
        kind: "r2_transient",
      },
      cfg,
    );
    expect(r.kind).toBe("effects");
    if (r.kind !== "effects") return;
    const persist = r.effects.find((e) => e.type === "persist_job");
    expect(persist?.type).toBe("persist_job");
    if (persist?.type !== "persist_job") return;
    const cf = 1;
    const backoffSeconds = computeBackoffSeconds(cf, cfg.retryBaseDelaySeconds);
    const backoffUntil = cfg.nowMs + backoffSeconds * 1000 + cfg.jitterMsSample;
    const expectedNext = Math.max(cfg.nowMs + cfg.tickIntervalMs, backoffUntil);
    expect(persist.job.nextActionAtMs).toBe(expectedNext);
    expect(r.tickResponse?.retryAfterSeconds).toBe(backoffSeconds);
  });

  it("retryable_backoff returns retry_exhausted at cap", () => {
    const cfg = clock({ maxConsecutiveFailures: 2 });
    const s = state({ consecutiveFailures: 1 });
    const r = lookupZipTransition(
      s,
      "RUNNING",
      {
        type: "retryable_backoff",
        holdStatus: "RUNNING",
        errText: "x",
        kind: "unknown",
      },
      cfg,
    );
    expect(r.kind).toBe("retry_exhausted");
  });

  it("schedule_next_tick_interval advances nextActionAtMs", () => {
    const cfg = clock({ nowMs: 10_000, tickIntervalMs: 3_000 });
    const s = state({ status: "PENDING" });
    const r = lookupZipTransition(s, "PENDING", { type: "schedule_next_tick_interval" }, cfg);
    expect(r.kind).toBe("effects");
    if (r.kind !== "effects") return;
    const jobEff = r.effects.find((e) => e.type === "persist_job");
    expect(jobEff?.type).toBe("persist_job");
    if (jobEff?.type !== "persist_job") return;
    expect(jobEff.job.nextActionAtMs).toBe(13_000);
  });

  it("chunk_run_success done=true sets FINALIZING", () => {
    const cfg = clock();
    const s = state();
    const run: RunChunkResponse = {
      uploadedParts: [{ partNumber: 1, etag: '"a"', sizeBytes: 1 }],
      nextPartNumber: 2,
      fileIndex: 0,
      zipOffset: "0",
      bytesWrittenTotal: "1",
      filesDone: 1,
      done: true,
    };
    const r = lookupZipTransition(s, "RUNNING", { type: "chunk_run_success", run }, cfg);
    expect(r.kind).toBe("effects");
    if (r.kind !== "effects") return;
    const pj = r.effects.find((e) => e.type === "persist_job");
    expect(pj?.type).toBe("persist_job");
    if (pj?.type !== "persist_job") return;
    expect(pj.job.status).toBe("FINALIZING");
    expect(r.effects.some((e) => e.type === "insert_uploaded_parts")).toBe(true);
  });
});
