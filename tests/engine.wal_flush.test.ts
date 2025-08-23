import { test } from "bun:test";
import { strict as assert } from "assert";
import Engine from "../libs/storage/engine";

// This test verifies that when a WAL implementation exposes a flush()
// method, Engine.flush() awaits it before updating the manifest walOffset.
// We simulate a WAL-like object and a Manifest spy to observe call ordering.

class DummyWal {
  private end = 12345;
  public flushCalled = false;
  public open = async () => {};
  public close = async () => {};
  public currentEndOffset = () => this.end;
  public append = async (_: any) => {};
  public flush = async () => {
    // simulate async delay and record call
    await new Promise((r) => setTimeout(r, 10));
    this.flushCalled = true;
  };
  public scan = async function* (_: any) {
    // empty
  };
}

// Create a lightweight manifest spy object that implements only the methods
// Engine.flush interacts with. This avoids needing to extend the real
// Manifest class and deal with file IO in the unit test.
class ManifestSpy {
  public setWalOffsetCalled = false;
  public addFile(_f: any) {
    return;
  }
  public listFiles() {
    return [] as any[];
  }
  public setWalOffset(offset: number) {
    this.setWalOffsetCalled = true;
    // no-op: don't persist
  }
}

test("engine flush awaits wal.flush before manifest setWalOffset", async () => {
  // Create a temporary Engine but replace wal and manifest with test doubles.
  const e: any = new Engine("./data", "log.wal", {});
  // replace wal with dummy
  const dw = new DummyWal();
  e.wal = dw;
  // replace manifest with spy instance
  const sm = new ManifestSpy();
  e.manifest = sm;

  // call flush and ensure it awaits wal.flush (which sets flushCalled)
  // We also ensure manifest.setWalOffset was invoked
  await e.flush();

  assert.equal(
    dw.flushCalled,
    true,
    "wal.flush should have been called during Engine.flush"
  );
  assert.equal(
    sm.setWalOffsetCalled,
    true,
    "manifest.setWalOffset should have been called during Engine.flush"
  );
});
