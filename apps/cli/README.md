```md
# KVStore CLI (apps/cli)

This CLI is a small, practical front-end for the embedded KVStore wrapper found in `apps/embedded/kv.ts`.
It demonstrates common operations, interactive shell usage, and scripting-friendly commands. The CLI is
implemented in TypeScript and executed with Bun in this repo.

## Quick prerequisites

- Bun installed (https://bun.sh).
- From the repo root you can run commands with `bun run ./apps/cli/cli.ts ...` or make the script executable:

  chmod +x apps/cli/cli.ts

  then run: `./apps/cli/cli.ts ...`

## DB path semantics

- Positional DB directory: most commands accept a positional `<dbdir>` as the first non-flag argument.
- `--db` flag: you may pass `--db=PATH` or `--db PATH` anywhere in the command to select the DB directory.
- Default: if neither is provided, the CLI uses a default directory resolved by `DEFAULT_DB_DIR` (by default `./data` in the repo root) or the environment variable `KV_DB_DIR`.

The CLI helper `extractAndRemoveDir()` recognizes both forms and removes the value from the parsed args so the rest of the command sees only its command-specific parameters.

## Interactive shell

Run `./apps/cli/cli.ts shell [<dbdir> | --db PATH]` to open an interactive Readline shell.

Features:
- Command completion (prefix-first, then Levenshtein fuzzy scoring).
- Transaction-aware completion: when inside a transaction the completer prioritizes pending keys for `tx.*` commands.
- Persistent per-DB history file stored as `.kv_history_<basename>` in the user's home directory.

Use `shell -c "put name alice; get name"` to run a sequence of commands non-interactively and exit.

## Common Commands and examples

Replace `<dbdir>` below with a path or use `--db` as described above.

- put: store a value

  bun run ./apps/cli/cli.ts put ./data/mydb name alice

- get: read a value

  bun run ./apps/cli/cli.ts get ./data/mydb name

- del: delete a key

  bun run ./apps/cli/cli.ts del ./data/mydb name

- cas: compare-and-swap (expect spec: key, expectedRev, newValue)

  bun run ./apps/cli/cli.ts cas ./data/mydb name 5 alice2

- scan: list keys/values with optional filters

  bun run ./apps/cli/cli.ts scan ./data/mydb --prefix=us --limit=100

- scan with streaming (--stream): for large datasets, streams rows to stdout instead of buffering.

  bun run ./apps/cli/cli.ts scan ./data/mydb --stream --prefix=logs

- watch: subscribe to events for a filter (blocks by default; use `--background` to detach)

  bun run ./apps/cli/cli.ts watch ./data/mydb "prefix:session"

- unwatch: remove a previously set watcher by id

  bun run ./apps/cli/cli.ts unwatch ./data/mydb <watchId>

- snapshot: capture current DB contents (prints JSON array of key/value)

  bun run ./apps/cli/cli.ts snapshot ./data/mydb

- export-subs: export subscriber state to a file

  bun run ./apps/cli/cli.ts export-subs ./data/mydb subs.json

- attach-registry: load a registry JSON and re-attach handlers by id

  bun run ./apps/cli/cli.ts attach-registry ./data/mydb registry.json

## Transactions (tx.*)

The CLI exposes transaction helpers in the shell. Example interactive flow:

  shell> tx.begin
  shell> tx.put name bob
  shell> tx.get name
  shell> tx.commit

Notes:
- While a transaction is active the completer prioritizes keys that were modified in the transaction (via `pendingKeys()` on the embedded wrapper).
- The wrapper tracks pending keys so you get relevant completion suggestions and better UX while composing multi-step transactions.

## Lease operations

- lease.grant <ttl> [key...] — create a lease and optionally attach keys
- lease.attach <leaseId> <key> — attach a key to lease
- lease.renew <leaseId> — renew an active lease
- lease.revoke <leaseId> — revoke a lease and detach associated keys

Example:

  bun run ./apps/cli/cli.ts lease.grant ./data/mydb 60 name

## Completion behaviour

- Prefix-first: exact/strong prefix matches are preferred.
- Fuzzy fallback: if no prefix match, completions are ranked by Levenshtein distance.
- Transaction context: when inside a `tx.*` command the completer will surface transaction-pending keys first.

## Scripting and automation

- Use `shell -c "...; ..."` to run a script of commands non-interactively.
- The CLI is designed to be composable in shell scripts — `scan --stream` and `watch` make streaming integration easier.

## Troubleshooting and notes

- If the CLI complains about the DB layout, confirm you pointed to the correct `data` directory (the engine expects the same on-disk layout used by the repo).
- The CLI uses the embedded wrapper (`apps/embedded/kv.ts`) for higher-level convenience. For production workloads embed the KV primitives directly into your app and avoid the demo CLI for heavy traffic.

## Implementation notes (high level)

- The CLI is implemented as a single TypeScript script using Node Readline for shell behaviour.
- Completion: a custom completer implements prefix checking and Levenshtein fuzzy ranking. It queries the KV for keys (cached per-shell session) and consults the wrapper's `pendingKeys()` during transactions.
- DB selection: `extractAndRemoveDir()` accepts `--db` or a positional directory and normalizes it for subsequent command parsing.

## Quick smoke test

1. Put a key and read it back:

  bun run ./apps/cli/cli.ts put ./data/mydb smoke.key hello
  bun run ./apps/cli/cli.ts get ./data/mydb smoke.key

2. Open shell and test completion:

  bun run ./apps/cli/cli.ts shell ./data/mydb

Type `put sm` then press Tab — you should see suggestions.

---
This CLI is a convenience/demo layer intended to illustrate how to drive the embedded KVStore. For production use, consider embedding the wrapper's logic directly or creating a small HTTP/gRPC front-end tailored to your usage and performance needs.

```
