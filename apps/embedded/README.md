# Embedded KVStore wrapper (apps/embedded)

This module provides a lightweight, application-friendly wrapper around the project's lower-level storage Engine.
It is intended to demonstrate embedding the database into an app and to provide a higher-level, ergonomic API for
common tasks like simple key/value operations, leases, transactions, snapshots, and watch subscription management.

## Files

- `kv.ts` — the `KVStore` wrapper class and helpers.

## Key features and API

The wrapper focuses on pragmatic, easy-to-use primitives. The most important methods are:

- open(dir?: string): Promise<void>
  - Open or create the DB at the provided directory.

- close(): Promise<void>
  - Flushes and closes the engine resources.

- put(key: string, value: string | Uint8Array): Promise<{rev:number}>
- get(key: string): Promise<{value?:Uint8Array, rev?:number}>
- del(key: string): Promise<void>

- cas(key: string, expectedRev: number, value: string): Promise<{ok:boolean, rev?:number}>

- beginTransaction(): Transaction
  - Starts a higher-level transaction object that buffers writes. The wrapper exposes a `pendingKeys()` helper on
    the active transaction so callers (and the CLI completer) can inspect keys modified in the current transaction.

- lease.* helpers: grant, attach, renew, revoke
  - Basic lease lifecycle helpers for TTL-based keys.

- listKeys(prefix?: string): Promise<string[]>
- scan(opts?): AsyncIterable<Row> / scanStream(opts) — streaming scanner for large datasets

- snapshot(): Promise<Array<{key:string, value:Uint8Array}>>

- watch(filter, handler): watch/unwatch helper that integrates with the project's WatchManager to receive
  live change events.

## Example usage

Basic put/get:

```ts
import { KVStore } from './apps/embedded/kv';

const kv = new KVStore('./data/myapp');
await kv.open();
await kv.put('name', 'alice');
const got = await kv.get('name');
console.log(Buffer.from(got.value || []).toString());
await kv.close();
```

Transactions (conceptual):

```ts
const tx = kv.beginTransaction();
await tx.put('a', '1');
await tx.put('b', '2');
// pendingKeys() is useful for UI completion and debugging
console.log(tx.pendingKeys());
await tx.commit();
```

Scanning large datasets (streaming):

```ts
for await (const row of kv.scanStream({ prefix: 'logs/' })) {
  // process row.key / row.value
}
```

## Design rationale and implementation notes

- Minimal wrapper: the wrapper intentionally keeps a small surface area focused on convenience and readability rather than
  exposing every engine internals. For high-throughput or advanced use-cases, import and use `libs/storage/engine.ts` directly.

- Pending keys: transactions track a set of keys the transaction modifies. This is surfaced by `pendingKeys()` and used by
  the CLI completer to provide smarter, transaction-aware completions.

- Streaming scans: the wrapper exposes streaming scanners to avoid buffering large result sets in memory. Use `scanStream` when
  the dataset may be large or when piping results directly to another process.

- Small inverted index (optional): the repository includes a tiny 3-gram inverted index concept used by the CLI to accelerate
  "contains" and fuzzy searches. The index is small, optional, and designed as an example — for production workloads
  consider a dedicated text/indexing subsystem.

- Watch/subscriber management: the wrapper makes it easier to attach/detach watchers and to export subscriber snapshots for
  persistence or reattachment. Watch handlers are reattached by id using `attachHandlerRegistry` when supplied with a registry JSON.

## When to use this wrapper vs. the engine directly

- Use the wrapper for demos, tooling (like the CLI), and small applications where convenience matters more than raw performance.
- Use the engine directly when you need fine-grained control, optimized batching, or to implement custom compaction/indexing
  strategies.

## Troubleshooting and tips

- If transactions appear to fail unexpectedly, inspect the `pendingKeys()` list and ensure your expectations about concurrent
  writes match the engine's concurrency model.
- For large scans, prefer `scanStream` to avoid memory pressure.

---
This module is intended as an approachable example and integration point; please adapt its ideas for your application's needs.
