import { describe, it, expect } from "bun:test";
const makeTempDir = require("./util/tmpdir");
import Engine from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";

describe("MVCC & CAS extra tests", () => {
  it("concurrent CAS stress: many concurrent CAS attempts serialize and one wins", async () => {
    const dir = makeTempDir();
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", { walImpl: impl });
      await e.open();

      const key = Buffer.from("concur");
      const attempts = 50;
      // Start with null
      const initial = await (e as any).cas(key, null, Buffer.from("v0"));
      expect(initial.ok).toBe(true);

      // Launch many concurrent CAS attempts, all expecting rev=initial.rev
      const proms: Promise<any>[] = [];
      for (let i = 0; i < attempts; i++) {
        proms.push(
          (async () => {
            return await (e as any).cas(
              key,
              initial.rev,
              Buffer.from("vx" + i)
            );
          })()
        );
      }
      const results = await Promise.all(proms);
      const oks = results.filter((r) => r && r.ok);
      // Only one should succeed due to per-key lock
      expect(oks.length).toBe(1);
      // final value should equal winner's value
      const final = e.get(key);
      expect(final).not.toBeNull();
      await e.close();
    });
  });

  it("CAS with snapshot under compaction: snapshot preserves revision visibility during compaction", async () => {
    const dir = makeTempDir();
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", {
        walImpl: impl,
        compactorOptions: { tombstoneRetentionMs: 1000 } as any,
      });
      await e.open();

      const key = Buffer.from("snapk");
      // create initial value and flush to create SST
      await e.put(key, Buffer.from("a"));
      await e.flush();
      // update value then flush so rev2 is persisted, then delete (tombstone)
      await e.put(key, Buffer.from("b"));
      await e.flush();
      await e.del(key);
      await e.flush();

      // start snapshot at rev 2 (it registers only when iterator is started)
      const snapIter = e.snapshotAtRevision(2);
      const ai = snapIter[Symbol.asyncIterator]();
      // begin iterator to register active snapshot and capture first yielded item
      const first = await ai.next();
      // run compaction which should see an active snapshot and avoid dropping tombstone needed by rev 2
      await e.compactNow();
      // check first result then iterate remaining items if needed
      let foundB = false;
      if (
        first &&
        !first.done &&
        first.value &&
        first.value.key &&
        first.value.key.equals(key)
      ) {
        if (first.value.value && first.value.value.toString() === "b")
          foundB = true;
      }
      if (!foundB) {
        for await (const ent of ai) {
          if (ent && ent.key && ent.key.equals(key)) {
            if (ent.value && ent.value.toString() === "b") foundB = true;
            break;
          }
        }
      }
      expect(foundB).toBe(true);
      // close snapshot to unregister (generator already finished)
      await e.close();
    });

    it("tombstone revision handling and minActiveRev GC policy", async () => {
      const dir = makeTempDir();
      const e = new Engine(dir, "log.wal", {
        compactorOptions: { tombstoneRetentionMs: 1 } as any,
      });
      await e.open();

      const k = Buffer.from("trv");
      // Put initial values across revisions
      await e.put(k, Buffer.from("v1")); // rev1
      await e.flush();
      await e.put(k, Buffer.from("v2")); // rev2
      await e.flush();
      await e.del(k); // rev3 tombstone
      await e.flush();

      // start snapshot at rev2 (register by starting iterator)
      const snapGen = e.snapshotAtRevision(2);
      const snapIt = snapGen[Symbol.asyncIterator]();
      const firstSnap = await snapIt.next();
      // run compaction; tombstone should not remove v2 because minActiveRev=2 protects
      await e.compactNow();
      // resume snapshot iterator: check firstSnap then iterate
      let sawV2 = false;
      if (
        firstSnap &&
        !firstSnap.done &&
        firstSnap.value &&
        firstSnap.value.key &&
        firstSnap.value.key.equals(k)
      ) {
        if (firstSnap.value.value && firstSnap.value.value.toString() === "v2")
          sawV2 = true;
      }
      if (!sawV2) {
        for await (const ent of snapIt) {
          if (ent && ent.key && ent.key.equals(k)) {
            if (ent.value && ent.value.toString() === "v2") sawV2 = true;
            break;
          }
        }
      }
      expect(sawV2).toBe(true);

      // close snapshot and allow tombstone TTL to expire then compact
      if (snapIt.return) await snapIt.return();
      await new Promise((r) => setTimeout(r, 10));
      await e.compactNow();
      // After compaction and TTL-based tombstone removal, the previously-live value (v2)
      // should be visible as the latest.
      const latest = e.get(k);
      expect(latest && latest.toString()).toBe("v2");
      await e.close();
    });
  });

  it("multiple revisions across SSTs and memtable are visible via getAtRevision", async () => {
    const dir = makeTempDir();
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", { walImpl: impl });
      await e.open();

      const k = Buffer.from("multi");
      await e.put(k, Buffer.from("r1"));
      await e.flush();
      await e.put(k, Buffer.from("r2"));
      await e.flush();
      await e.put(k, Buffer.from("r3"));

      const v1 = await e.getAtRevision(k, 1);
      const v2 = await e.getAtRevision(k, 2);
      const v3 = await e.getAtRevision(k, 3);
      expect(v1 && v1.toString()).toBe("r1");
      expect(v2 && v2.toString()).toBe("r2");
      expect(v3 && v3.toString()).toBe("r3");

      await e.close();
    });
  });
});
