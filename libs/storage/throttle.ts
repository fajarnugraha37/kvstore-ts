type MaybeConsumeSig = (
  tokenBucket: any | null,
  writer: any,
  key: Buffer | null,
  val: Buffer | null,
  rev?: number
) => Promise<void>;

let _impl: MaybeConsumeSig = async (
  tokenBucket: any | null,
  writer: any,
  key: Buffer | null,
  val: Buffer | null,
  rev?: number
) => {
  if (!tokenBucket) return;
  try {
    let delta = 0;
    if (writer && typeof writer.deltaSizeForEntry === "function") {
      delta = writer.deltaSizeForEntry(key, val, rev);
    } else {
      const klen = key ? (key as unknown as Buffer).length : 0;
      const vlen = val ? (val as unknown as Buffer).length : 0;
      delta = klen + vlen + 10;
    }
    if (delta > 0) await tokenBucket.consume(delta);
  } catch (e) {
    // swallow errors to avoid compaction aborts from throttler glitches
  }
};

// preserve default implementation so tests can restore it
const _defaultImpl = _impl;

export const maybeConsumePerEntry: MaybeConsumeSig = async (
  tokenBucket: any | null,
  writer: any,
  key: Buffer | null,
  val: Buffer | null,
  rev?: number
) => {
  return _impl(tokenBucket, writer, key, val, rev);
};

// Test-only API to override implementation when running tests
export function __setMaybeConsumeForTests(fn: MaybeConsumeSig | null) {
  if (fn) _impl = fn;
  else _impl = _defaultImpl;
}