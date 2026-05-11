import { describe, expect, it } from "vitest";
import { zipTickReduce, type ZipTickConfig, type ZipTickState } from "./zip-v2-orchestrator";
import { computeBackoffSeconds } from "../lib/utils";
import type { Checkpoint, JobStateRow, RunChunkResponse } from "../lib/types/types";

function baseCfg(over: Partial<ZipTickConfig> = {}): ZipTickConfig {
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

function baseState(over: Partial<JobStateRow> = {}, cpOver: Partial<Checkpoint> = {}): ZipTickState {
  const job: JobStateRow = {
    jobId: "job-1",
    transferId: "tr-1",
    status: "RUNNING",
    createdAtMs: 999_000,
    updatedAtMs: 999_500,
    consecutiveFailures: 0,
    ...over,
  };
  const checkpoint: Checkpoint = {
    manifestKey: "m/k",
    outputKey: "out/k.zip",
    uploadId: "up-1",
    partSize: 128,
    nextPartNumber: 1,
    fileIndex: 0,
    zipOffset: "0",
    bytesWrittenTotal: "0",
    filesDone: 0,
    done: false,
    ...cpOver,
  };
  return { job, checkpoint };
}

describe("zipTickReduce", () => {
  it("apply_retryable_failure sets nextActionAtMs from backoff + jitter vs tick interval", () => {
    const cfg = baseCfg();
    const prev = baseState();
    const r = zipTickReduce(
      prev,
      {
        type: "apply_retryable_failure",
        holdStatus: "RUNNING",
        errText: "boom",
        kind: "container_5xx",
      },
      cfg,
    );
    expect(r.retryExhausted).toBeUndefined();
    const cf = 1;
    const backoffSeconds = computeBackoffSeconds(cf, cfg.retryBaseDelaySeconds);
    const backoffUntilMs = cfg.nowMs + backoffSeconds * 1000 + cfg.jitterMsSample;
    const expectedNext = Math.max(cfg.nowMs + cfg.tickIntervalMs, backoffUntilMs);
    expect(r.next.job.consecutiveFailures).toBe(1);
    expect(r.next.job.nextActionAtMs).toBe(expectedNext);
    expect(r.next.job.status).toBe("RUNNING");
    expect(r.retryBackoff?.retryAfterSeconds).toBe(backoffSeconds);
    expect(r.commands.map((c) => c.type)).toEqual(["persist_job", "reschedule_storage_alarm"]);
  });

  it("apply_retryable_failure returns retryExhausted when cap reached", () => {
    const cfg = baseCfg({ maxConsecutiveFailures: 3 });
    const prev = baseState({ consecutiveFailures: 2 });
    const r = zipTickReduce(
      prev,
      {
        type: "apply_retryable_failure",
        holdStatus: "FINALIZING",
        errText: "x",
        kind: "r2_transient",
      },
      cfg,
    );
    expect(r.retryExhausted).toBe(true);
    expect(r.commands).toHaveLength(0);
    expect(r.next).toEqual(prev);
  });

  it("chunk_run_success with done true moves job to FINALIZING and persists checkpoint", () => {
    const cfg = baseCfg();
    const prev = baseState();
    const run: RunChunkResponse = {
      uploadedParts: [{ partNumber: 1, etag: '"e1"', sizeBytes: 100 }],
      nextPartNumber: 2,
      fileIndex: 1,
      zipOffset: "99",
      bytesWrittenTotal: "100",
      filesDone: 3,
      done: true,
    };
    const r = zipTickReduce(prev, { type: "chunk_run_success", run }, cfg);
    expect(r.next.job.status).toBe("FINALIZING");
    expect(r.next.checkpoint.done).toBe(true);
    expect(r.next.checkpoint.nextPartNumber).toBe(2);
    const types = r.commands.map((c) => c.type);
    expect(types).toEqual(["insert_uploaded_parts", "persist_checkpoint", "persist_job"]);
  });

  it("chunk_run_success with done false keeps RUNNING", () => {
    const cfg = baseCfg();
    const prev = baseState();
    const run: RunChunkResponse = {
      uploadedParts: [],
      nextPartNumber: 2,
      fileIndex: 0,
      zipOffset: "1",
      bytesWrittenTotal: "50",
      filesDone: 0,
      done: false,
    };
    const r = zipTickReduce(prev, { type: "chunk_run_success", run }, cfg);
    expect(r.next.job.status).toBe("RUNNING");
    expect(r.next.checkpoint.done).toBe(false);
  });

  it("chunk_failed_terminal marks FAILED and emits notify + alarm", () => {
    const cfg = baseCfg();
    const prev = baseState();
    const r = zipTickReduce(
      prev,
      {
        type: "chunk_failed_terminal",
        errText: "terminal",
        kind: "bad_manifest",
        consecutiveFailures: 3,
      },
      cfg,
    );
    expect(r.next.job.status).toBe("FAILED");
    expect(r.next.job.cleanupAtMs).toBe(cfg.nowMs + cfg.cleanupTtlMs);
    expect(r.next.job.consecutiveFailures).toBe(3);
    expect(r.commands.map((c) => c.type)).toEqual([
      "persist_job",
      "reschedule_storage_alarm",
      "notify_transfer_compression_failed",
    ]);
    const notify = r.commands[2];
    expect(notify.type).toBe("notify_transfer_compression_failed");
    if (notify.type === "notify_transfer_compression_failed") {
      expect(notify.transferId).toBe("tr-1");
      expect(notify.outputKey).toBe("out/k.zip");
    }
  });

  it("tick_backoff_skips returns tickResponse only", () => {
    const cfg = baseCfg();
    const prev = baseState();
    const response = { status: "RUNNING" as const, done: false, retryAfterSeconds: 12 };
    const r = zipTickReduce(prev, { type: "tick_backoff_skips", response }, cfg);
    expect(r.commands).toHaveLength(0);
    expect(r.tickResponse).toEqual(response);
    expect(r.next).toEqual(prev);
  });

  it("schedule_next_tick_interval advances nextActionAtMs", () => {
    const cfg = baseCfg({ nowMs: 10_000, tickIntervalMs: 7_000 });
    const prev = baseState({ nextActionAtMs: 5_000 });
    const r = zipTickReduce(prev, { type: "schedule_next_tick_interval" }, cfg);
    expect(r.next.job.nextActionAtMs).toBe(17_000);
  });
});
