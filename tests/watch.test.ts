import { existsSync, rmSync } from "node:fs";
import { join } from "node:path";
import { describe, it, expect } from "bun:test";
import { WatchManager } from "../libs/watch/watch_manager";
import { Engine } from "../libs/storage/engine";

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

describe("watch manager basic filters + replay", () => {
  it("matches exact/prefix/suffix/contains/all and supports once/continuous", async () => {
    const { dir, engine } = mk("watch", "basic");
    await engine.open();
    const wm = engine.watchManager;

    const events: any[] = [];
    const s1 = wm.add({ type: "exact", key: "k1" }, true);
    s1.onEvent((ev) => events.push(["exact", ev]));

    const s2 = wm.add({ type: "prefix", prefix: "p" }, false);
    s2.onEvent((ev) => events.push(["prefix", ev]));

    const s3 = wm.add({ type: "suffix", suffix: "z" }, false);
    s3.onEvent((ev) => events.push(["suffix", ev]));

    const s4 = wm.add({ type: "contains", substring: "mid" }, false);
    s4.onEvent((ev) => events.push(["contains", ev]));

    const s5 = wm.add({ type: "all" }, false);
    s5.onEvent((ev) => events.push(["all", ev]));

    // publish directly
    wm.publish({ key: "k1", value: "v1", rev: 1 });
    wm.publish({ key: "p_hello", value: "v2", rev: 2 });
    wm.publish({ key: "my_mid_key", value: "v3", rev: 3 });
    wm.publish({ key: "endz", value: "v4", rev: 4 });

    // s1 is once -> should only get first event
    const exact = events.filter((e) => e[0] === "exact");
    expect(exact.length).toBe(1);

    // prefix should see 'p_hello'
    const pref = events.filter((e) => e[0] === "prefix").map((x) => x[1].key);
    expect(pref).toContain("p_hello");

    // suffix should see 'endz'
    const suf = events.filter((e) => e[0] === "suffix").map((x) => x[1].key);
    expect(suf).toContain("endz");

    // contains should see 'my_mid_key'
    const cont = events.filter((e) => e[0] === "contains").map((x) => x[1].key);
    expect(cont).toContain("my_mid_key");

    // all sees all events
    const all = events.filter((e) => e[0] === "all");
    expect(all.length).toBeGreaterThanOrEqual(4);

    await engine.close();
  });

  it("replay: new subscriber can replay missed events from WAL", async () => {
    const { dir, engine } = mk("watch", "replay");
    await engine.open();
    // write some keys
    await engine.put(Buffer.from("r1"), Buffer.from("x"));
    await engine.put(Buffer.from("r2"), Buffer.from("y"));
    await engine.put(Buffer.from("r3_mid"), Buffer.from("z"));

    // close to flush WAL
    await engine.close();

    // open a new engine to get wal instance for scanning
    const e2 = new Engine(dir, "log.wal", { timeProvider: () => Date.now() });
    await e2.open();
    const wm = e2.watchManager;
    const recs: any[] = [];
    const s = wm.add({ type: "contains", substring: "mid" }, false);
    s.onEvent((ev) => recs.push(ev));

    // replay from wal offset 0
    await wm.replayFromScan((e2 as any).wal.scanWithOffsets(0));

    // we should have received the r3_mid event
    const found = recs.find((r) => r.key === "r3_mid");
    expect(found).toBeTruthy();

    await e2.close();
  });
});
