export async function withWalImpls(fn: (impl: "wal" | "handoff") => Promise<void>) {
  // Allow selecting a single impl via WAL_IMPL env var for focused runs
  const env = process.env.WAL_IMPL;
  const available: ("wal" | "handoff")[] = ["wal", "handoff"];
  const impls: ("wal" | "handoff")[] =
    env && (env === "wal" || env === "handoff") ? [env] : available;
  for (const impl of impls) {
    // mark the env so makeTempDir can include impl in directory names
    try {
      process.env.TEST_RUN_SUFFIX = impl;
    } catch {}
    await fn(impl);
  }
}
