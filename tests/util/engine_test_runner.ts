import fs from "fs";
import os from "os";
import path from "path";
import { spawnSync } from "child_process";

export async function withWalImpls(fn: (impl: "wal" | "handoff") => Promise<void>) {
  // Allow selecting a single impl via WAL_IMPL env var for focused runs
  const env = process.env.WAL_IMPL;
  const available: ("wal" | "handoff")[] = ["wal", "handoff"];
  const impls: ("wal" | "handoff")[] =
    env && (env === "wal" || env === "handoff") ? [env] : available;
  // If WAL_IMPL is not set, run each impl in a separate child process and exit.
  // This provides process-level isolation for background workers/resources.
  if (!process.env.WAL_IMPL) {
    for (const impl of impls) {
      if (process.env.TRACE_WALRUN) console.log(`[walrun] spawning child bun test WAL_IMPL=${impl}`);
      const res = spawnSync("bun", ["test"], {
        env: { ...process.env, WAL_IMPL: impl, TEST_RUN_SUFFIX: impl },
        stdio: "inherit",
        shell: false,
      });
      if (res.error) {
        console.error(`[walrun] spawn error for ${impl}:`, res.error);
        process.exit(1);
      }
      if (typeof res.status === "number" && res.status !== 0) {
        process.exit(res.status);
      }
    }
    // All child runs succeeded; exit parent to avoid duplicate in-process runs.
    process.exit(0);
  }

  for (const impl of impls) {
    // mark the env so makeTempDir can include impl in directory names
    try {
      process.env.TEST_RUN_SUFFIX = impl;
      if (process.env.TRACE_WALRUN) {
        try {
          console.log(`[walrun] setting TEST_RUN_SUFFIX=${impl}`);
        } catch (e) {}
      }
    } catch {}
    await fn(impl);
    // wipe any kvstore-test* dirs under os.tmpdir() to ensure post-impl isolation
    try {
      const tmp = os.tmpdir();
      const children = fs.readdirSync(tmp);
      for (const c of children) {
        if (c.startsWith("kvstore-test")) {
          const p = path.join(tmp, c);
          try {
            fs.rmSync(p, { recursive: true, force: true });
            if (process.env.TRACE_WALRUN) console.log(`[walrun] removed tmp ${p}`);
          } catch (e) {
            if (process.env.TRACE_WALRUN) console.log(`[walrun] failed remove tmp ${p} ${e}`);
          }
        }
      }
    } catch (e) {
      if (process.env.TRACE_WALRUN) console.log(`[walrun] tmp wipe error ${e}`);
    }
  }
}
