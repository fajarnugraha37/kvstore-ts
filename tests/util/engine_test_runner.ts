export async function withWalImpls(fn: (impl: "wal" | "handoff") => Promise<void>) {
  const impls: ("wal" | "handoff")[] = ["wal", "handoff"];
  for (const impl of impls) {
    await fn(impl);
  }
}
