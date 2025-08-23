import { Engine } from "./../libs/storage/engine";
import { FileBackend } from "../libs/watch/backends/file_backend";

async function run() {
  const dir = "./data/example_watch";
  // Example 1: registry mapping subscriber id -> handler
  const fileBackend = new FileBackend(dir);
  const engine1 = new Engine(dir, "log.wal", { watchBackend: fileBackend });
  await engine1.open();
  const subId = (engine1.watchManager as any).add("prefix:hello", false).id;
  console.log("created sub id", subId);
  // attach a handler via registry at startup (simulate restart)
  await engine1.close();

  const registry: Record<number, (ev: any) => void> = {};
  registry[subId] = (ev) => console.log("[registry-handler]", ev);

  const engine2 = new Engine(dir, "log.wal", {
    watchBackend: fileBackend,
    watchHandlerRegistry: registry,
  });
  await engine2.open();
  console.log("engine reopened and handler should be attached");
  await engine2.put(Buffer.from("hello1"), Buffer.from("world1"));
  // Example 2: factory mapping snapshot -> handler
  await engine2.close();
  const fileBackend2 = new FileBackend(dir);
  const factory = (snap: any) => {
    if (snap.filter === "prefix:hello")
      return (ev: any) => console.log("[factory-handler]", ev);
    return undefined;
  };
  const engine3 = new Engine(dir, "log.wal", {
    watchBackend: fileBackend2,
    watchHandlerFactory: factory,
  });
  await engine3.open();
  await engine3.put(Buffer.from("hello2"), Buffer.from("world2"));
  await engine3.close();
}

run().catch((e) => console.error(e));
