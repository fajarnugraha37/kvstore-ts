import { existsSync, rmSync } from "node:fs";
import { join } from "node:path";
import { describe, it, expect } from "bun:test";
import { Engine } from "../libs/storage/engine";
import { WatchManager } from "../libs/watch/watch_manager";

function mk(dirBase: string, name: string) {
  const dir = join(process.cwd(), "data", `${dirBase}_${name}_${Date.now()}`);
  try {
    if (existsSync(dir)) rmSync(dir, { recursive: true, force: true });
  } catch {}
  return {
    dir,
    engine: new Engine(dir, "log.wal", { timeProvider: () => Date.now() }),
  };
}

describe("watch replay resume", () => {
  it("replayFromWal with minRev filters events and subscriber snapshot persists lastSeenRev", async () => {
    const { dir, engine } = mk("watch", "resume_rev");
    await engine.open();

    // write some keys
    await engine.put(Buffer.from("a1"), Buffer.from("v1")); // rev 1
    await engine.put(Buffer.from("a2"), Buffer.from("v2")); // rev 2
    await engine.put(Buffer.from("a3"), Buffer.from("v3")); // rev 3

    // close and reopen to get wal instance
    await engine.close();
    const e2 = new Engine(dir, "log.wal", { timeProvider: () => Date.now() });
    await e2.open();
    const wm = e2.watchManager as WatchManager;

    // subscribe and replay only entries with rev > 1
    const sub = wm.add({ type: "prefix", prefix: "a" }, false);
    const events: any[] = [];
    sub.onEvent((ev) => events.push(ev));
    await wm.replayFromWal((e2 as any).wal, { minRev: 1 });

    // should have received rev 2 and 3
    const revs = events.map((e) => e.rev).sort((a: number, b: number) => a - b);
    expect(revs).toEqual([2, 3]);

    // subscriber snapshot should carry lastSeenRev (3)
    const snap = (sub as any).toSnapshot();
    expect(snap.lastSeenRev).toBeGreaterThanOrEqual(3);

    await e2.close();
  });

  it("replayFromWal with minWalOffset scans from offset and works with offsets", async () => {
    const { dir, engine } = mk("watch", "resume_offset");
    await engine.open();

    // multiple writes
    await engine.put(Buffer.from("o1"), Buffer.from("x1"));
    await engine.put(Buffer.from("o2"), Buffer.from("x2"));
    // capture wal offset after first two
    const walAny = (engine as any).wal;
    const off =
      typeof walAny.currentEndOffset === "function"
        ? walAny.currentEndOffset()
        : 0;

    await engine.put(Buffer.from("o3"), Buffer.from("x3"));
    await engine.close();

    const e2 = new Engine(dir, "log.wal", { timeProvider: () => Date.now() });
    await e2.open();
    const wm = e2.watchManager as WatchManager;
    const recs: any[] = [];
    const s = wm.add({ type: "prefix", prefix: "o" }, false);
    s.onEvent((ev) => recs.push(ev));

    // replay only entries from the captured WAL offset
    await wm.replayFromWal((e2 as any).wal, { minWalOffset: off });

    // Should have at least the o3 event
    const found = recs.find((r) => r.key === "o3");
    expect(found).toBeTruthy();

    await e2.close();
  });
});
