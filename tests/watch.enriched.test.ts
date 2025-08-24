import { existsSync, rmSync } from "node:fs";
import { join } from "node:path";
import { createServer } from "node:http";
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
    engine: new Engine(dir, "log.wal", {
      timeProvider: () => Date.now(),
    } as any),
  };
}

describe("watch enriched tests (persistence, registry, factory, http)", () => {
  it("persists subscriber snapshots and reattaches via watchHandlerRegistry on restart", async () => {
    const { dir } = mk("watch", "persist_reg");
    const e1 = new Engine(dir, "log.wal", { watchBackend: "file" } as any);
    await e1.open();

    const events1: any[] = [];
    const sub = (e1.watchManager as any).add(
      { type: "prefix", prefix: "users:" },
      false
    );
    sub.onEvent((ev: any) => events1.push(ev));

    await e1.put(Buffer.from("users:alice"), Buffer.from("A"));
    await e1.put(Buffer.from("users:bob"), Buffer.from("B"));

    // capture id and close
    const sid = sub.id;
    await e1.close();

    // reopen with handler registry to auto-attach and resume replay
    const events2: any[] = [];
    const registry: Record<number, (ev: any) => void> = {};
    registry[sid] = (ev: any) => events2.push(ev);

    const e2 = new Engine(dir, "log.wal", {
      watchBackend: "file",
      watchHandlerRegistry: registry,
    } as any);
    await e2.open();

    // replay is performed during open; since the subscriber had already seen
    // those events before shutdown, we expect lastSeenRev to be preserved and
    // therefore the restored handler should NOT receive duplicates. Instead
    // verify it receives new events written after restart.
    await new Promise((r) => setTimeout(r, 100));
    expect(events2.length).toBe(0);

    // write a fresh event and ensure restored handler receives it
    await e2.put(Buffer.from("users:carol"), Buffer.from("C"));
    await new Promise((r) => setTimeout(r, 100));
    expect(events2.find((ev) => ev.key === "users:carol")).toBeTruthy();

    // getSubscriberById should return restored subscriber
    const restored = (e2.watchManager as any).getSubscriberById(sid);
    expect(restored).not.toBeNull();

    await e2.close();
  });

  it("uses watchHandlerFactory to attach handlers based on snapshot metadata", async () => {
    const { dir } = mk("watch", "factory_reg");
    const e1 = new Engine(dir, "log.wal", { watchBackend: "file" } as any);
    await e1.open();
    const sub = (e1.watchManager as any).add(
      { type: "prefix", prefix: "orders:" },
      false
    );
    await e1.put(Buffer.from("orders:1"), Buffer.from("one"));
    const sid = sub.id;
    await e1.close();

    const events: any[] = [];
    const factory = (snap: any) => {
      if (
        snap &&
        snap.filter &&
        snap.filter.type === "prefix" &&
        snap.filter.prefix === "orders:"
      ) {
        return (ev: any) => events.push(ev);
      }
      return undefined;
    };

    const e2 = new Engine(dir, "log.wal", {
      watchBackend: "file",
      watchHandlerFactory: factory,
    } as any);
    await e2.open();
    await new Promise((r) => setTimeout(r, 100));
    // the factory should not cause duplicate replay of already-seen events; ensure
    // new events are delivered instead.
    expect(events.find((e) => e.key === "orders:1")).toBeUndefined();
    await e2.put(Buffer.from("orders:2"), Buffer.from("two"));
    await new Promise((r) => setTimeout(r, 100));
    expect(events.find((e) => e.key === "orders:2")).toBeTruthy();
    await e2.close();
  });

  it("HttpForwarder forwards events to an HTTP endpoint", async () => {
    const posts: any[] = [];
    const srv = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
        try {
          const b = Buffer.concat(chunks).toString();
          posts.push(JSON.parse(b));
        } catch {}
        res.writeHead(200);
        res.end("ok");
      });
    });
    await new Promise<void>((res) => srv.listen(0, "127.0.0.1", () => res()));
    // @ts-ignore
    const port = (srv.address() as any).port;
    const endpoint = `http://127.0.0.1:${port}/post`;

    const e = new Engine(
      join(process.cwd(), "data", `watch_http_${Date.now()}`),
      "log.wal",
      { watchBackend: { type: "http", endpoint } } as any
    );
    await e.open();
    // Add a subscriber on the forwarding backend so publish triggers POST
    (e.watchManager as any).add({ type: "prefix", prefix: "feed:" }, false);
    await e.put(Buffer.from("feed:item1"), Buffer.from("x"));

    // wait for forwarder to deliver
    await new Promise((r) => setTimeout(r, 300));
    expect(posts.length).toBeGreaterThanOrEqual(1);
    const got = posts.find((p) => p && p.key === "feed:item1");
    expect(got).toBeTruthy();

    await e.close();
    srv.close();
  });

  it("HttpForwarder retries on failures and eventually succeeds", async () => {
    let calls = 0;
    const posts: any[] = [];
    const srv = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
        calls++;
        try {
          const b = Buffer.concat(chunks).toString();
          posts.push(JSON.parse(b));
        } catch {}
        // first two calls fail, then succeed
        if (calls <= 2) {
          res.writeHead(500);
          res.end("fail");
        } else {
          res.writeHead(200);
          res.end("ok");
        }
      });
    });
    await new Promise<void>((res) => srv.listen(0, "127.0.0.1", () => res()));
    // @ts-ignore
    const port = (srv.address() as any).port;
    const endpoint = `http://127.0.0.1:${port}/post`;

    const e = new Engine(
      join(process.cwd(), "data", `watch_http_retry_${Date.now()}`),
      "log.wal",
      {
        watchBackend: {
          type: "http",
          endpoint,
          opts: { retries: 4, backoffMs: 10, timeoutMs: 200 },
        },
      } as any
    );
    await e.open();
    (e.watchManager as any).add({ type: "prefix", prefix: "retry:" }, false);
    await e.put(Buffer.from("retry:item1"), Buffer.from("x"));

    // give forwarder time to retry and eventually succeed
    await new Promise((r) => setTimeout(r, 500));
    expect(posts.length).toBeGreaterThanOrEqual(1);
    srv.close();
    await e.close();
  });

  it("once-subscribers are removed after a single event and not persisted", async () => {
    const { dir } = mk("watch", "once_persist");
    const e1 = new Engine(dir, "log.wal", { watchBackend: "file" } as any);
    await e1.open();
    const s = (e1.watchManager as any).add(
      { type: "prefix", prefix: "temp:" },
      true
    );
    const got: any[] = [];
    s.onEvent((ev: any) => got.push(ev));
    await e1.put(Buffer.from("temp:1"), Buffer.from("1"));

    // give it a moment and persist
    await new Promise((r) => setTimeout(r, 50));
    // snapshot should not include this once-subscriber
    const snaps = (e1.watchManager as any).exportSnapshots();
    const found = snaps.find((ss: any) => ss.id === s.id);
    expect(found).toBeUndefined();
    await e1.close();
  });

  it("WatchManager export/restore reproduces subscribers with lastSeenRev", async () => {
    const wm1 = new WatchManager();
    const s = wm1.add({ type: "prefix", prefix: "x:" }, false);
    s.setLastSeenRev(42);
    const snaps = (wm1 as any).exportSnapshots();

    const wm2 = new WatchManager();
    (wm2 as any).restoreSnapshots(snaps);
    const restored = (wm2 as any).getSubscriberById(s.id);
    expect(restored).not.toBeNull();
    expect(restored.getLastSeenRev()).toBe(42);
  });
});
