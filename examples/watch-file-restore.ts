import { Engine } from "../libs/storage/engine";

async function main() {
  const dir = "./data";
  // Start engine with file-backed watch backend so subscriptions are persisted
  const engine = new Engine(dir, "log.wal", { watchBackend: "file" } as any);
  await engine.open();

  // Register a subscriber and attach an event handler
  const sub = engine.watchManager.add({ type: "prefix", prefix: "users:" });
  sub.onEvent((ev) => {
    console.log("[first-run] got event:", ev);
  });

  // Put a key to trigger event
  await engine.put(Buffer.from("users:alice"), Buffer.from("alice-data"));

  // Persisted by FileBackend. Close engine to simulate restart.
  await engine.close();

  // Reopen engine; backend constructor will load persisted snapshots and engine.open
  // will trigger a WAL replay to resume subscribers (so they don't miss events).
  const engine2 = new Engine(dir, "log.wal", { watchBackend: "file" } as any);
  await engine2.open();

  // Reattach handler using getSubscriberById so restored Subscriber gets a live callback
  // Note: subscriber IDs are preserved across restarts by the file backend.
  const snaps = (engine2.watchManager as any).exportSnapshots?.() || [];
  if (snaps.length > 0) {
    const first = snaps[0];
    const restored = (engine2.watchManager as any).getSubscriberById(first.id);
    if (restored) {
      restored.onEvent((ev: any) => {
        console.log("[after-restart] got event:", ev);
      });
    }
  }

  // Put another key to demonstrate resumed subscriber receives events
  await engine2.put(Buffer.from("users:bob"), Buffer.from("bob-data"));

  // allow some time for async forwarding
  await new Promise((r) => setTimeout(r, 200));
  await engine2.close();
}

main().catch((e) => console.error(e));
