import { it, expect } from "bun:test";
import { WallSchedHelper } from "../libs/wal/wall_sched_helper";

// Quick unit tests for WallSchedHelper scheduling and meta-flush behavior.

it("calls onFlushBatch when scheduleFlush timer elapses", async () => {
  let called = false;
  const helper = new WallSchedHelper({
    maxBatchDelayMs: 10,
    onFlushBatch: async () => {
      called = true;
    },
  });

  helper.scheduleFlush();
  // wait longer than timer
  await new Promise((r) => setTimeout(r, 30));
  expect(called).toBe(true);
});

it("start/stop background flush calls onFlush periodically", async () => {
  let calls = 0;
  const helper = new WallSchedHelper({
    maxBatchDelayMs: 20,
    onFlush: async () => {
      calls++;
    },
  });

  helper.startBackgroundFlush();
  // wait for a couple intervals
  await new Promise((r) => setTimeout(r, 120));
  helper.stopBackgroundFlush();
  expect(calls).toBeGreaterThanOrEqual(2);
});

it("triggers onMetaFlush after metaFlushInterval notes", async () => {
  let metaCalled = 0;
  const helper = new WallSchedHelper({
    maxBatchDelayMs: 10,
    metaFlushInterval: 3,
    onMetaFlush: async () => {
      metaCalled++;
    },
  });

  // call noteAppend N times and expect onMetaFlush after 3
  await helper.noteAppend(128);
  await helper.noteAppend(128);
  expect(metaCalled).toBe(0);
  await helper.noteAppend(128);
  expect(metaCalled).toBe(1);
});
