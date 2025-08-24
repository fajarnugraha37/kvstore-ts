# kvstore-ts

A compact, embeddable key/value store implemented in TypeScript. The repo provides a storage Engine, an
embeddable convenience wrapper, a CLI for exploration and tooling, and reference implementations of WAL, manifest,
compaction, transactions, watches, and a small inverted index used for demo fuzzy/contains searches.

This README now contains deeper technical details: repository layout, architectural diagrams described in
text, technical decisions, detailed dataflows (write/read/tx/compaction/recovery/watch), and a project milestone
roadmap.

---

## Repository layout

This project focuses on a compact, single-node durable KV storage engine implemented in TypeScript with a small set of supporting tools. Current top-level folders are the canonical surface for development and tests:

- `src/` — main library entry and core TypeScript modules (engine, storage wiring, CLI adapters).
- `wal/` — write-ahead log implementation and helpers.
- `storage/` 
    - SSTable (datafile) writer/reader, manifest, compaction, and storage facade
    - small manifest utilities and metadata helpers (file metadata tracking).
    - compactor and policies (background GC / compaction worker).
    - transaction manager and related helpers.
- `watch/` — watch/subscription manager and matching utilities.
- `apps/cli/` — command-line tooling and interactive shell.
- `apps/embedded/` — embeddable wrapper and examples for embedding `KVStore` in apps.
- `utils/` — low-level utilities (bytes, checksum helpers, fs wrappers).
- `data/` — example DB directory used by tests and local runs (not committed in production).

Other top-level files: `package.json`, `tsconfig.json`, `README.md`, `bun.lock` and test fixtures.

---

## Project plan

Below is a concise, prioritized plan and a tiny contract for the project. This replaces the previous long-form
architecture notes with an actionable roadmap and component-level responsibilities so the README matches the
implementation and the project's current scope.

Short plan
- Present a prioritized checklist of components (MVP first).
- For each component: purpose, responsibilities, implementation notes, tests/edge-cases, suggested tech choices.
- End with recommended next steps and minimal milestone plan.

Contract (tiny)
- Inputs: client requests (put/get/txn/watch/lease/join), cluster messages (raft log entries / RPCs), disk I/O.
- Outputs: persisted state, linearizable responses, cluster state changes, metrics & logs.
- Success criteria: durable single-node correctness, then linearizability across cluster with leader election and recovery.
- Error modes: disk failure, node crash, network partition, slow IO, inconsistent state after crash (must recover), split-brain.

Prioritized checklist (MVP → extras)
1. Core storage (single-node durable)
2. WAL + append-only log + compaction
3. Simple API server (gRPC/HTTP)
4. Simple client operations (get/put/delete)
5. Watch/subscription mechanism
6. Snapshots & compaction
7. Consensus (Raft) and leader state machine
8. Cluster membership + bootstrapping
9. Transactions / Compare-And-Swap (MVCC)
10. Leases / TTL and lease eviction
11. Security: TLS + auth + RBAC
12. Metrics, health, and diagnostics
13. Backup/restore, defragmentation
14. Stress tests, chaos/fault-injection tests
15. CLI tooling and admin APIs
16. Production hardening (observability, tuning)

Component notes (condensed)

1) Core storage (single-node)
- Purpose: persist key→value and metadata (versions).
- Responsibilities: store key/value byte arrays; support Get/Put/Delete and range scans; maintain per-key revision/version (MVCC) for transactions and linearizability.
- Implementation notes: choose an engine (embedded DB like Level or custom WAL+SST); use lexicographic key layout for range queries.
- Tests & edge-cases: crash recovery, large key/value sizes, binary keys, concurrency correctness.

2) WAL + append-only log + compaction
- Purpose: durability and recovery; source-of-truth for replication.
- Responsibilities: durable append of commands, sync to disk, efficient compaction.
- Implementation notes: compact binary header (version/type/len/checksum), batching, truncate-on-recover behavior (already implemented in WAL code).

3) API server (gRPC/HTTP)
- Purpose: expose client-facing APIs (put/get/delete/txn/watch/lease) and health/metrics.
- Implementation notes: use gRPC for production clients, HTTP/JSON for tooling.

4) State machine & client operations
- Purpose: apply log entries to storage deterministically and idempotently.

5) Watch / subscription manager
- Purpose: allow clients to subscribe to key events; deliver events in order after entries are applied.

6) Snapshots & compaction
- Purpose: truncate WAL/log and reduce storage by creating snapshots of applied state; atomic replace of snapshot files.

7) Consensus (Raft)
- Purpose: replicated, fault-tolerant state machine; prefer reuse of mature libraries for correctness.

... (other components summarized above; keep details in design docs or separate RFCs)

Minimal MVP roadmap (concrete)
1. Single-node durable KV: Storage + WAL + simple HTTP/gRPC API for put/get/delete + basic tests.
2. Watches + simple snapshots.
3. Add Raft (3-node cluster) using WAL as raft log backend.
4. Transactions, leases, auth, metrics.
5. Operational tooling: CLI, backup/restore, stress harness, monitoring.

Recommended immediate next steps
- Choose runtime and raft/storage libraries (Go + etcd/raft recommended for production; Node ok for prototyping).
- Implement single-node storage + WAL (reuse existing WAL code) and a minimal API server.
- Add crash-recovery tests and a stress harness.

If you prefer, I can scaffold the storage module files and implement either a minimal proof-of-concept (MemTable + SST writer + tests) or wire the memtable->SST flush path integrated with the existing WAL and manifest.

---

## Detailed technical flows

Below are explicit step-by-step flows you can use as a reference when reasoning about code or implementing features.

### Write (single-op) flow

1. API entry (client/library) calls `engine.put(key, value)`.
2. Validation & encoding: key/value validated and possibly encoded (e.g., MessagePack) and a checksum computed.
3. WAL append: an operation record (op type, key, metadata, location hint) is appended to the WAL and fsync'ed
	 according to durability settings.
4. Datafile append: the value is appended to the active datafile; the manifest is updated atomically (or deferred to
	 a checkpoint) with the new segment references.
5. Index update: in-memory index and small inverted index are updated to reflect the new key location.
6. Notify subscribers: the watch manager emits the event with old/new metadata.
7. Return: operation returns success along with a revision or version id.

Notes: If `put` is part of a transaction, steps 4-6 are deferred until `tx.commit()`; WAL append still records the
transactional intent so recovery can reconstitute pending transactions.

### Transaction commit flow

1. Begin transaction: `tx = engine.beginTransaction()` — allocates ephemeral buffers and records start state.
2. Transactional operations (tx.put/del): buffered in-memory; WAL entries with transaction id may be appended to
	 record intent (depending on implementation choices).
3. Commit: on `tx.commit()` the engine validates conflicts (if any), appends a transactional commit record to WAL,
	 writes buffered entries to datafiles, updates indexes, notifies watchers, and releases transaction resources.
4. Abort: `tx.abort()` discards buffered changes and may append an abort record.

### Read flow (get/scan)

1. `get(key)` consults the in-memory index for a location. If not found, look up a small per-file index or fallback to
 	 a sequential scan of index metadata.
2. Read blob via IO helper, verify checksum, decode, and return value and revision metadata.
3. `scan()` returns an AsyncIterable that yields rows as they are resolved; `scanStream()` writes rows directly to a
	 provided output (stdout, file, or stream) for very large datasets.

### Compaction/GC flow

1. Scheduler picks a compaction target (candidate segments with ratio of dead/live data above threshold).
2. Compactor reads live entries, writes them to a new compact segment, updates the manifest and frees old segments via
	 the freelist.
3. If a crash occurs during compaction, manifest WAL entries and atomic renames ensure partial compactions are rolled
	 forward or ignored safely during recovery.

### Recovery flow (startup)

1. On startup, the Engine reads the last manifest/superblock to determine active segments.
2. Replay WAL entries newer than the last checkpoint to apply missed operations or complete partial commits.
3. Rebuild in-memory index and optional indexes from manifests and datafiles (or use persisted index files if present
	 and validated).

---

## How the database stores and retrieves data (detailed)

This section summarizes the concrete storage model and the exact step-by-step paths for writes and reads so you
have a single reference in the README for how data moves through the system.

Storage model summary
- Append-only segmented datafiles: values are appended to immutable segments (datafiles). The manifest/superblock
	records which segments are active.
- WAL-first durability: mutating operations are first appended to the WAL so they can be recovered after a crash.
- Hot index: an in-memory index maps keys to the latest location (segment id + offset + length).
- Optional persisted inverted 3-gram index used by the CLI for contains/fuzzy searches.

Write path (single operation: put / del / cas)
1. API entry: caller invokes `engine.put(key, value)`.
2. Encode & metadata: the engine encodes the value (MessagePack or raw bytes), computes checksum, and assigns a
	 revision/version id.
3. WAL append: create a WAL record describing the operation (op type, key, metadata) and append it to WAL; flush
	 depending on durability configuration. This guarantees recoverability.
4. Datafile append: append blob (with a small header: length, checksum, rev) to the active datafile segment.
5. In-memory index update: update the in-memory index to point the key to the new segment + offset + rev.
6. Persisted index update: if a persisted inverted index is configured, update it incrementally or mark it for
	 asynchronous rebuild.
7. Notify watchers: the watch manager emits a durable change event (old/new metadata) after the WAL/datafile writes
	 as required by the durability model.
8. Return: client receives success and revision metadata.

Notes: if `put` is done inside a transaction, datafile/index updates and watcher notifications are deferred until
commit; WAL may still record transactional intent.

Transactional write path (commit)
1. `beginTransaction()` allocates an in-memory transaction context and records start metadata.
2. `tx.put()` / `tx.del()` buffer writes in the transaction context (not visible to other readers).
3. On `tx.commit()` the engine performs conflict checks (optimistic or lock-based depending on implementation), then
	 appends a transaction-commit record to WAL and writes buffered blobs to datafiles.
4. Indexes are updated atomically with the commit step; watchers are notified after commit is durable.
5. On `tx.abort()` the buffers are dropped; an abort WAL record may be written depending on the durability model.

Read path (get / scan / contains)
-- Single-key lookup (`get`): consult in-memory index -> fetch blob from datafile via IO helper -> verify checksum ->
 	decode and return value + rev.
- Range/prefix scan: walk the in-memory index in key order and yield rows lazily (AsyncIterable) or stream them
	directly with `scanStream()` to avoid buffering large results.
- Contains/fuzzy: use persisted inverted 3-gram index to generate candidate keys, rank by Levenshtein or heuristics,
	then fetch candidates using the normal lookup path.

Compaction & garbage collection
- Scheduler selects candidate segments (high dead/live ratio).
- Compactor reads live entries and writes them into new segment(s), updating the manifest atomically and freeing
	old segments via the freelist.
- Crash during compaction is handled by manifest/WAL coordination: partial results are either completed or ignored
	safely during recovery.

Recovery (startup) summary
1. Read last manifest/superblock to determine active segments.
2. Replay WAL entries newer than last checkpoint to reapply missed operations or finish partial commits.
3. Rebuild or validate in-memory indexes from manifest/datafiles (or load persisted index files if validated).

Consistency, durability and trade-offs
- Durability: an operation is durable once its WAL record is flushed. Configurable batching affects latency vs
	throughput trade-offs.
- Atomicity: single-op atomic; multi-op transactions are atomic at the commit boundary.
- Isolation: single-node transactional isolation depends on the engine's concurrency model (optimistic/locks).
- Performance trade-offs: WAL-first gives a clear recovery story at the cost of extra IO; background compaction trades
	CPU/IO for reclaimed disk space.

Data integrity and validation
- Checksums are stored with data blobs and verified on reads.
- Manifests and WAL are validated at startup; corrupted WAL typically requires truncation or restore from snapshot.

Common failure scenarios and mitigations
- Lost power after WAL append: replay WAL on recovery to reapply the op.
- WAL corruption: truncate/repair or restore from snapshot.
- Compaction crash: manifest/WAL ensures partial compactions don't corrupt the visible state.

Performance knobs and recommendations
- Batch writes when possible to reduce WAL fsync frequency.
- Use `scanStream()` for large exports to avoid memory pressure.
- Tune compaction thresholds and segment sizes according to workload (many small writes vs bulk loads).

## On-disk layouts

This section records the exact, authoritative byte-level formats implemented in the codebase (WAL, SST, Manifest).
Use these when building tooling, format validators, or diagrams.

### Requirements checklist
- WAL entry headers/trailers (exact offsets, types, endianness, checksum) — implemented below.
- SST (datafile) file/header/entry/index/footer layouts (per-entry fields, varint tails, CRCs, compression flag) — implemented below.
- Manifest on-disk representation and atomic persist semantics — implemented below.

### WAL (write-ahead log)

- Entry layout on disk: header (12 bytes) | payload (N bytes) | trailer (12 bytes, identical format as header)
- Header / trailer (12 bytes) byte offsets and meaning (all numeric fields are big-endian unless noted):
	- offset 0 (1 byte): version (uint8)
	- offset 1 (1 byte): type (uint8)
	- offset 2-3 (2 bytes): reserved (uint16)
	- offset 4-7 (4 bytes): payload length (uint32 BE)
	- offset 8-11 (4 bytes): checksum (uint32 BE) — Adler-32 of the payload bytes

Notes:
- The WAL writer writes header + payload + trailer. On recovery the reader validates header/trailer pair and checksum and
	will truncate any trailing partial/corrupt bytes. Payloads are encoded with MessagePack (fall back to JSON where required).
- Metadata files: the WAL subsystem maintains small companion files (.meta.json and .segments.json) that record global
	base/next offsets and rotated segment boundaries.

ASCII layout (example):

	[hdr:12] [payload:N] [trailer:12]
	00..00  12..(12+N-1)   (12+N)..(23+N)

Concrete example (JSON payload used for illustration). Payload bytes and Adler-32 were computed from the example payload:

	payload (UTF-8): {"op":"put","key":"foo"}
	payload length: 24 bytes

	full bytes (hex):
	01 01 00 00 00 00 00 18 5c 04 07 6e  7b 22 6f 70 22 3a 22 70 75 74 22 2c 22 6b 65 79 22 3a 22 66 6f 6f 22 7d  01 01 00 00 00 00 00 18 5c 04 07 6e

	Interpretation:
	- first 12 bytes = header: version=0x01, type=0x01, reserved=0x0000, length=0x00000018 (24), checksum=0x5c04076e
	- next 24 bytes = payload (UTF-8 JSON)
	- final 12 bytes = trailer (identical to header)

### SST / datafile (SSTable) — format v3 (current)

- File header: 4-byte magic "SST1" followed by 1-byte version (total 5 bytes).
- Per-entry layout (v3) in sequence:
	- key length: 4 bytes (uint32 BE)
	- key: keyLen bytes
	- value length: 4 bytes (uint32 BE) — 0xFFFFFFFF indicates a tombstone (deleted)
	- value: valueLen bytes (omitted for tombstone)
	- revision: varint (7-bit groups, LSB-first continuation semantics)
	- walOffset: varint
	- createdAt: varint

- Index layout (written after blocks):
	- 4 bytes: block count (uint32 BE)
	- for each block:
		- fklen (4 bytes uint32 BE)
		- fk (fklen bytes) — first key in block
		- offset (8 bytes uint64 BE) — file offset where block begins
		- storedLen (4 bytes uint32 BE) — length of stored bytes; high bit (0x80000000) indicates compression
		- crc32c (4 bytes uint32 BE) — CRC32C of the stored bytes

- Footer (SST_FOOTER_LEN = 28 bytes):
	- indexOffset: 8 bytes (uint64 BE)
	- bloomOffset: 8 bytes (uint64 BE) — zero if no bloom present
	- indexCks: 4 bytes (uint32 BE) — CRC32C of the index region
	- footerCks: 4 bytes (uint32 BE) — CRC32C of the footer region (excludes magic)
	- magic: 4 bytes (uint32 BE) — 0x53535446

Checksums and compression:
- SST blocks/index/footer use CRC32C (Castagnoli) as implemented in the repository.
- Blocks may be compressed; storedLen will have BLOCK_COMPRESSED_FLAG (0x80000000) set when compressed.

Concrete SST entry example (synthetic):

- Consider putting key="abc" value="xyz" with revision=1, walOffset=123, createdAt=1690000000.
- Encoding (per-entry sequence):
	- keyLen (u32 BE): 00 00 00 03
	- key bytes: 61 62 63
	- valueLen (u32 BE): 00 00 00 03
	- value bytes: 78 79 7a
	- revision (varint for 1): 01
	- walOffset (varint for 123): 7b
	- createdAt (varint for 1690000000): 80 8f a6 f5 05  (varint representation shown as bytes)

Hex stream (concatenated):
	00 00 00 03 61 62 63 00 00 00 03 78 79 7a 01 7b 80 8f a6 f5 05

Notes: createdAt varint above is illustrative; actual varint bytes depend on the varint encoder's grouping — the README varint convention above is authoritative (7-bit groups, continuation bit).

### Manifest (manifest.json)

- The manifest is persisted as JSON with this top-level shape: { files: PersistedFileMeta[], walOffset: number }
- PersistedFileMeta contains: { file: string, minKeyHex: string, maxKeyHex: string, size: number, level?: number, walOffset?: number, createdAt?: number }
- Persist semantics: write to a temporary file (manifest.json.tmp) -> optional fsync on tmp -> atomic rename to manifest.json -> optional fsync on directory when strictAtomicity is enabled. This ensures an atomic manifest replace on platforms that support rename semantics.

### Helpers & encodings

- Integer endianness: all fixed-width integers (u32, u64) are big-endian on disk.
- Varints: 7-bit groups with continuation bit; LSB-first group order (standard little-endian varint-style encoding used in the helper utilities).
- Checksums: WAL uses Adler-32 over payload bytes; SST uses CRC32C for blocks, index and footer integrity.

### Quick write/recovery sequence (ASCII)

	Client -> Engine.put()
			 -> WAL.append(header,payload,trailer) + fdatasync
			 -> (optionally) Datafile/SST append or batch into SSTWriter
			 -> (optionally) manifest persist (tmp -> rename -> dir fsync)
			 -> in-memory index update -> notify watchers

Recovery (startup): read manifest -> open active SST files -> scan WAL entries newer than manifest.walOffset -> apply operations -> rebuild in-memory index

## Observability and debugging

- Metrics: small metrics module exports counters/histograms for ops/sec, latency, compaction stats, and WAL throughput.
- Logging: each major subsystem (WAL, compactor, txn manager, watcher) emits structured logs to help triage issues.
- Snapshots: use the snapshot/export facility to produce consistent dumps for debugging or offline analysis.

---

## Project milestones & roadmap

Planned milestones are grouped into short, medium, and long-term goals.

Short-term (next 1-4 weeks)
- Finish README and documentation (this change).
- Add `examples/` with runnable scripts for: put/get, tx commit/abort, streaming scan, snapshot export/import.
- Add smoke tests and a GitHub Actions workflow to run core smoke tests on push.

Medium-term (1-3 months)
- Improve the inverted index: make it incremental and persistent with configurable shard sizes.
- Add a small HTTP admin API for metrics, compaction controls, and snapshot exports.
- Write unit + integration tests for transactional conflict resolution and compaction edge cases.
- Add benchmark harnesses and CI perf regression checks.

---

## How you can help / contribute

- Open issues for bugs or feature requests and assign a small scope to each change.
- Send PRs with tests for bug fixes and new features; run `npm test` and `npm run typecheck` locally.
- Help build the examples and CI workflow.

---

## Benchmark: 1M write / 1M read

This repository includes a heavy benchmark that writes one million (1,000,000) entries and then performs two read modes over the same dataset: point-reads (one-by-one via `get`) and range scan (prefix scan). The bench is intentionally heavy to exercise WAL, memtable, SST flushing and compaction code paths and to produce realistic IO/CPU/memory characteristics.

### Files and scripts

- `bench/kv_bench.ts` — the heavy benchmark script. It performs:
	- write 1,000,000 entries with a 256B payload (keys are `k_0000000`..`k_0999999`), batched in groups of 1,000 writes.
	- read 1,000,000 entries by issuing batched `get()` calls.
	- read 1,000,000 entries by streaming a prefix scan (`scanStream({ startWith: 'k_' })`).
	- prints memory stats between phases.
- `scripts/run_and_record_bench.cjs` — cross-platform runner that executes the bench and saves the raw mitata/bench output to `tmp/mitata_last.txt` (and appends run-level CPU/duration metadata).
- `scripts/append_bench_readme.ts` — parses the captured output and appends a Markdown table under the `## Benchmarks` section in `README.md` containing per-bench avg latency, memory and captured CPU percent.

### How to run

- Run the cross-platform recorder (recommended on Windows/macOS/Linux):

	```powershell
	bun run bench:mitata:record
	```

	This will:
	- execute `bench/kv_bench.ts`, writing and reading 1M entries (may take several minutes depending on hardware),
	- produce `tmp/mitata_last.txt` containing raw bench output,
	- append a Markdown table summary to the `## Benchmarks` section in this `README.md`.

Notes and resource expectations
- Disk usage: the bench writes ~256 bytes per value × 1,000,000 ≈ 256MB for raw values, plus WAL/metadata/overhead and SST files — plan for ~1GB peak depending on compression and compaction behavior.
- Memory: depends on memtable and SStable readers; monitor RSS while running. The runner records a simple memory snapshot (RSS / heapUsed) printed between phases and the append script will parse average memory where available.
- Duration: on modern laptops this may take minutes; on slower machines it may take longer. The runner records run duration and an approximate CPU busy percentage.

### Interpreting README entries

- After a run, the `## Benchmarks` section will contain a Markdown table with columns: `name | avg_ms | avg_mem_mb | cpu_percent | ts`.
	- `name` — bench name (e.g. `put-small`, `get-small`, `scan-prefix-stream`).
	- `avg_ms` — average milliseconds per iteration reported by mitata.
	- `avg_mem_mb` — parsed average memory (where available) reported in mitata output.
	- `cpu_percent` — approximate CPU busy percentage measured during the run.
	- `ts` — ISO timestamp when the bench was recorded.

### Safety and cleanup
- The benchmark will create and (by default) overwrite `./data/bench_kv`. If you have important data in that directory, move it before running the benchmark.
- To clean generated data after the run:

	```powershell
	rm -r ./data/bench_kv tmp

### Reports

