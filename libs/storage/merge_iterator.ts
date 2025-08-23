// Streaming k-way merge iterator (memory efficient)
// Accepts multiple AsyncIterable<{ key: Buffer; value: Buffer | null }>
// and yields merged entries in key order, deduping keys by last-seen priority order (later iterables have priority).

import { MinHeap } from "./min_heap";

/**
 * Represents a single entry in the merge process.
 */
// Allow extra metadata on entries (e.g., createdAtSrc, walOffsetSrc) and preserve it through the merge.
type Entry = { key: Buffer; value: Buffer | null; rev?: number; [k: string]: any };

/**
 * Merges multiple sorted async iterables into a single sorted async iterable.
 * @param iterables - The async iterables to merge.
 */
export async function* kWayMerge(
  iterables: AsyncIterable<Entry>[]
): AsyncGenerator<Entry> {
  // We will maintain one async iterator per iterable and keep the current value per iterator.
  const iters = iterables.map((it) => it[Symbol.asyncIterator]());
  // Item carries the original payload so we don't lose auxiliary fields when merging
  type Item = { key: Buffer; payload: Entry; idx: number };
  // Compare by key first, then by source index to have deterministic ordering when keys equal.
  const heap = new MinHeap<Item>((a, b) => {
    const c = Buffer.compare(a.key, b.key);
    if (c !== 0) return c;
    return a.idx - b.idx;
  });

  // advance all iterators to get their first item
  for (let i = 0; i < iters.length; i++) {
    const res = await iters[i]!.next();
    if (!res.done && res.value) {
      const payload = res.value as Entry;
      heap.push({ key: payload.key, payload, idx: i });
    }
  }

  // For deduping, we must pick the highest-priority value for each key. We'll define priority: later iterables have higher priority
  // To achieve that, when multiple entries have the same key, we'll drain all with that key from the heap into a small array and select the one with highest idx.

  while (heap.size > 0) {
    const head = heap.pop()!;
    const key = head.key;
    const same: Item[] = [head];
    while (heap.peek() && Buffer.compare(heap.peek()!.key, key) === 0) {
      same.push(heap.pop()!);
    }

    // choose the payload with highest revision; tie-breaker: higher idx
    let chosen = same[0]!;
    for (const s of same) {
      const srev = typeof s.payload.rev === "number" ? s.payload.rev : 0;
      const crev = typeof chosen.payload.rev === "number" ? chosen.payload.rev : 0;
      if (srev > crev) chosen = s;
      else if (srev === crev && s.idx > chosen.idx) chosen = s;
    }

    // If there are multiple candidates for the same key, attach the full candidate list
    // (preserving payload objects) so callers can make informed decisions (e.g., tombstone
    // GC may want to promote the next-highest non-tombstone when skipping a tombstone).
    if (same.length > 1) {
      const candidates = same.map((s) => s.payload);
      const out = Object.assign({}, chosen.payload, { candidates });
      yield out as Entry;
    } else {
      // yield the original payload so auxiliary fields are preserved
      yield chosen.payload;
    }

    // advance each iterator that we drained to refill heap
    for (const s of same) {
      const it = iters[s.idx];
      if (!it) continue;
      const res = await it.next();
      if (!res.done && res.value) {
        const payload = res.value as Entry;
        heap.push({ key: payload.key, payload, idx: s.idx });
      }
    }
  }
}

export default kWayMerge;
