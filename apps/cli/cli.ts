#!/usr/bin/env bun

import { KVStore } from "../embedded/kv";
import * as fs from "fs";
import readline from "node:readline";
import path from "path";

// default DB dir: can be overridden with KV_DB_DIR env var
const DEFAULT_DB_DIR =
  process.env.KV_DB_DIR || path.join(process.cwd(), "data");
function resolveDir(dir?: string) {
  return dir && dir.length ? dir : DEFAULT_DB_DIR;
}

// Extract --db or positional dir (at index 1) from args and return cleaned args
function extractAndRemoveDir(origArgs: string[]) {
  const args = origArgs.slice();
  let dir: string | undefined = undefined;
  // search for --db or --db=foo
  for (let i = 1; i < args.length; i++) {
    const a = args[i];
    if (!a) continue;
    if (a === "--db") {
      if (i + 1 < args.length) {
        dir = args[i + 1];
        args.splice(i, 2);
        return { dir, args };
      }
    }
    if (typeof a === "string" && a.startsWith("--db=")) {
      const parts = a.split("=");
      dir = parts.length > 1 ? parts[1] : undefined;
      args.splice(i, 1);
      return { dir, args };
    }
  }
  // if no flag, treat positional second token as dir if present and not a flag
  if (
    args.length > 1 &&
    typeof args[1] === "string" &&
    !args[1].startsWith("--")
  ) {
    dir = args[1];
    args.splice(1, 1);
  }
  return { dir, args };
}

function color(s: string, code: number) {
  return `\x1b[${code}m${s}\x1b[0m`;
}
// simple levenshtein distance for fuzzy scoring
function levenshtein(a: string, b: string) {
  if (a === b) return 0;
  const m = a.length;
  const n = b.length;
  const dp: number[] = new Array(n + 1).fill(0);
  for (let j = 0; j <= n; j++) dp[j] = j;
  for (let i = 1; i <= m; i++) {
    let prev = dp[0]!;
    dp[0] = i;
    for (let j = 1; j <= n; j++) {
      const cur = dp[j];
      const cost = a.charAt(i - 1) === b.charAt(j - 1) ? 0 : 1;
      const aVal = dp[j]! + 1;
      const bVal = dp[j - 1]! + 1;
      const cVal = prev + cost;
      dp[j] =
        aVal < bVal ? (aVal < cVal ? aVal : cVal) : bVal < cVal ? bVal : cVal;
      prev = cur as number;
    }
  }
  return dp[n];
}
function printTable(rows: string[][], cols: string[]) {
  const widths = cols.map((c, i) =>
    Math.max(c.length, ...rows.map((r) => (r[i] ? String(r[i]).length : 0)))
  ) as number[];
  // header
  const header = cols
    .map((c, i) => color(c.padEnd(widths[i] ?? 0), 36))
    .join("  ");
  console.log(header);
  for (const r of rows) {
    console.log(
      r
        .map((c, i) =>
          String(c === null || c === undefined ? "" : c).padEnd(widths[i] ?? 0)
        )
        .join("  ")
    );
  }
}

function usage() {
  console.log(`kvstore CLI

Usage:
  cli put <dbdir | --db=path> <key> <value>
  cli get <dbdir | --db=path> <key>
  cli watch <dbdir | --db=path> <pattern>
  cli unwatch <dbdir | --db=path> <subId>
  cli snapshot <dbdir | --db=path> [rev]
  cli export-subs <dbdir | --db=path> <out.json>
  cli attach-registry <dbdir | --db=path> <json-registry>

Examples:
  cli put ./data/mydb name alice
  cli get ./data/mydb name
  cli watch ./data/mydb "prefix:na"
`);
  console.log(
    "\nIf <dbdir> is omitted, the default is: " +
      DEFAULT_DB_DIR +
      " (override with KV_DB_DIR or --db=PATH)"
  );
}

async function main() {
  const args = process.argv.slice(2);
  if (args.length === 0) return usage();
  const cmd = args[0];

  switch (cmd) {
    case "put": {
      const { dir: d, args: a } = extractAndRemoveDir(args);
      const dir = resolveDir(d);
      const key = a[1];
      const value = a.slice(2).join(" ");
      if (!key || value === undefined) return usage();
      // parse optional --ttl or -t flag
      let ttl: number | undefined = undefined;
      const ttlIdx = a.indexOf("--ttl");
      if (ttlIdx >= 0 && a.length > ttlIdx + 1) ttl = Number(a[ttlIdx + 1]);
      const tShort = a.indexOf("-t");
      if (!ttl && tShort >= 0 && a.length > tShort + 1)
        ttl = Number(a[tShort + 1]);
      const kv = new KVStore(dir);
      await kv.open();
      await kv.put(key, value, ttl ? { leaseTtlMs: ttl } : undefined);
      await kv.close();
      console.log("OK");
      break;
    }
    case "get": {
      const { dir: d, args: a } = extractAndRemoveDir(args);
      const dir = resolveDir(d);
      const key = a[1];
      if (!key) return usage();
      const kv = new KVStore(dir);
      await kv.open();
      const v = await kv.get(key);
      console.log(v);
      await kv.close();
      break;
    }
    case "scan": {
      const { dir: d, args: a } = extractAndRemoveDir(args);
      const dir = resolveDir(d);
      const rest = a.slice(1);
      const opts: any = {};
      for (const p of rest) {
        if (p.startsWith("--start=")) opts.startWith = p.split("=")[1];
        if (p.startsWith("--end=")) opts.endWith = p.split("=")[1];
        if (p.startsWith("--contains=")) opts.contains = p.split("=")[1];
        if (p.startsWith("--exact=")) opts.exact = p.split("=")[1];
        if (p.startsWith("--limit=")) opts.limit = Number(p.split("=")[1]);
        if (p.startsWith("--offset=")) opts.offset = Number(p.split("=")[1]);
        if (p === "--json") opts.json = true;
        if (p === "--stream") opts.stream = true;
        if (p.startsWith("--regex=")) opts.regex = p.split("=")[1];
        if (p.startsWith("--fuzzy=")) opts.fuzzy = p.split("=")[1];
      }
      const kv = new KVStore(dir);
      await kv.open();
      if (opts.stream) {
        for await (const r of kv.scanStream(opts)) {
          if (opts.json) console.log(JSON.stringify(r));
          else console.log(r.key, "\t", r.value);
        }
      } else {
        const res = await kv.scan(opts);
        if (opts.json) console.log(JSON.stringify(res, null, 2));
        else
          printTable(
            res.map((r: any) => [r.key, r.value]),
            ["Key", "Value"]
          );
      }
      await kv.close();
      break;
    }
    case "watch": {
      const { dir: d, args: a } = extractAndRemoveDir(args);
      const dir = resolveDir(d);
      const block = a.includes("-b") || a.includes("--block");
      const pattern = a
        .slice(1)
        .filter((p) => p !== "-b" && p !== "--block")
        .join(" ");
      if (!pattern) return usage();
      const kv = new KVStore(dir);
      await kv.open();
      console.log("watching. press Ctrl+C to exit");
      const handler = (ev: any) => console.log("EVENT", JSON.stringify(ev));
      const id = kv.watch(
        { substring: pattern, type: "contains" },
        false,
        handler
      );
      console.log("watch started id=", id);
      if (block) {
        await new Promise<void>((resolve) => {
          const onSig = () => {
            try {
              kv.unwatch(id);
            } catch (e) {}
            process.removeListener("SIGINT", onSig);
            resolve();
          };
          process.on("SIGINT", onSig);
        });
        console.log("watch stopped");
      }
      await kv.close();
      break;
    }
    case "unwatch": {
      const { dir: d, args: a } = extractAndRemoveDir(args);
      const dir = resolveDir(d);
      const idStr = a[1];
      if (!idStr) return usage();
      const kv = new KVStore(dir);
      await kv.open();
      kv.unwatch(Number(idStr));
      await kv.close();
      console.log("unwatched");
      break;
    }
    case "snapshot": {
      const { dir: d, args: a } = extractAndRemoveDir(args);
      const dir = resolveDir(d);
      const revStr = a[1];
      const kv = new KVStore(dir);
      await kv.open();
      const rev = revStr ? Number(revStr) : undefined;
      const s = await kv.snapshot(rev);
      console.log(JSON.stringify(s, null, 2));
      await kv.close();
      break;
    }
    case "export-subs": {
      const { dir: d, args: a } = extractAndRemoveDir(args);
      const dir = resolveDir(d);
      const out = a[1];
      if (!out) return usage();
      const kv = new KVStore(dir);
      await kv.open();
      const snaps = kv.exportSubscribers();
      fs.writeFileSync(out, JSON.stringify(snaps, null, 2));
      await kv.close();
      console.log("exported", out);
      break;
    }
    case "attach-registry": {
      const { dir: d, args: a } = extractAndRemoveDir(args);
      const dir = resolveDir(d);
      const registryPath = a[1];
      if (!registryPath) return usage();
      const kv = new KVStore(dir);
      await kv.open();
      const reg = JSON.parse(fs.readFileSync(registryPath, "utf-8"));
      // registry: { "123": "console" }
      const mapping: Record<number, (ev: any) => void> = {};
      for (const k of Object.keys(reg)) {
        const n = Number(k);
        if (reg[k] === "console")
          mapping[n] = (ev) => console.log("EV", JSON.stringify(ev));
      }
      kv.attachHandlerRegistry(mapping);
      await kv.close();
      console.log("attached registry");
      break;
    }
    case "shell": {
      // interactive shell: cli shell [dbdir | --db=path] [-c "cmd1;cmd2;..."]
      const { dir: d, args: a } = extractAndRemoveDir(args);
      const dir = resolveDir(d);
      // a[1] might be -c or a script
      const maybeFlag = a[1];
      const maybeCmd = a[2];

      const kv = new KVStore(dir);
      await kv.open();

      // non-interactive script mode for automation/testing
      if (maybeFlag === "-c" && typeof maybeCmd === "string") {
        const cmds = maybeCmd
          .split(";")
          .map((s) => s.trim())
          .filter(Boolean);
        for (const line of cmds) await handleShellLine(kv, line);
        await kv.close();
        break;
      }

      // banner
      console.log(color("KVStore CLI", 33) + " - interactive shell");
      console.log(`connected to: ${dir}`);
      console.log("Type 'help' for commands, Ctrl+C to exit.\n");

      const dbName = path.basename(path.resolve(dir));
      const histFile = path.join(
        process.env.HOME || process.env.USERPROFILE || ".",
        `.kv_history_${dbName}`
      );
      // load history if present
      let history: string[] = [];
      try {
        if (fs.existsSync(histFile))
          history = fs
            .readFileSync(histFile, "utf-8")
            .split(/\r?\n/)
            .filter(Boolean)
            .slice(-1000);
      } catch (e) {}

      // basic list of known commands and aliases
      const commands = [
        "put",
        "get",
        "del",
        "cas",
        "watch",
        "monitor",
        "unwatch",
        "snapshot",
        "export-subs",
        "attach-registry",
        "lease.grant",
        "lease.attach",
        "lease.renew",
        "lease.revoke",
        "tx.begin",
        "tx.put",
        "tx.del",
        "tx.commit",
        "tx.abort",
        "help",
        "exit",
      ];

      // LRU key cache for fast completion; will be populated from kv.listKeys()
      let keyCache: string[] = [];
      const refreshKeyCache = async () => {
        try {
          keyCache = await kv.listKeys(1000);
        } catch (e) {
          keyCache = [];
        }
      };
      await refreshKeyCache();

      // completer: suggest commands and keys
      const completer = async (line: string) => {
        const parts = line.split(/\s+/).filter(Boolean);
        const last = (parts.length ? parts[parts.length - 1] : "") as string;
        // when first token, rank commands by prefix then levenshtein
        if (parts.length <= 1) {
          const candidates = commands.slice();
          const scored = candidates
            .map((c) => ({
              c,
              s: c.startsWith(last) ? 0 : levenshtein(c, last),
            }))
            .sort((a, b) => a.s! - b.s! || a.c.localeCompare(b.c))
            .map((x) => x.c);
          return [scored.slice(0, 50), last];
        }
        const first = parts[0] || "";
        // dynamic tx-aware completion
        if (first.startsWith("tx")) {
          const tx = (kv as any)._activeTx;
          if (first === "tx.del" || (first === "tx" && parts[1] === "del")) {
            const pending: string[] =
              tx && typeof tx.pendingKeys === "function"
                ? tx.pendingKeys()
                : tx && tx._pendingKeys
                ? Array.from(tx._pendingKeys)
                : [];
            const keys = pending.length
              ? pending
              : keyCache.length
              ? keyCache
              : await kv.listKeys(200);
            const scored = keys
              .filter((k) => k)
              .map((k) => ({
                k,
                s: k.startsWith(last) ? 0 : levenshtein(k, last),
              }))
              .sort((a, b) => a.s! - b.s! || a.k.localeCompare(b.k))
              .map((x) => x.k);
            return [scored.slice(0, 50), last];
          }
          if (first === "tx.put" || (first === "tx" && parts[1] === "put")) {
            const keys = keyCache.length ? keyCache : await kv.listKeys(200);
            const scored = keys
              .map((k) => ({
                k,
                s: k.startsWith(last) ? 0 : levenshtein(k, last),
              }))
              .sort((a, b) => a.s! - b.s! || a.k.localeCompare(b.k))
              .map((x) => x.k);
            return [scored.slice(0, 50), last];
          }
        }
        const keyVerbs = new Set([
          "get",
          "del",
          "cas",
          "tx.put",
          "tx.del",
          "lease.attach",
        ]);
        if (
          keyVerbs.has(first) ||
          (first === "tx" && parts[1] && ["put", "del"].includes(parts[1]))
        ) {
          try {
            const keys = keyCache.length ? keyCache : await kv.listKeys(200);
            const scored = keys
              .filter((k) => k)
              .map((k) => ({
                k,
                s: k.startsWith(last) ? 0 : levenshtein(k, last),
              }))
              .sort((a, b) => a.s! - b.s! || a.k.localeCompare(b.k))
              .map((x) => x.k);
            return [scored.slice(0, 50), last];
          } catch (e) {
            return [[], last];
          }
        }
        if (first.startsWith("lease")) {
          try {
            const leases = await kv.listLeases();
            const hits = leases
              .map(String)
              .map((s) => ({
                s,
                sc: s.startsWith(last) ? 0 : levenshtein(s, last),
              }))
              .sort((a, b) => a.sc! - b.sc! || a.s.localeCompare(b.s))
              .map((x) => x.s);
            return [hits.length ? hits : leases.map(String), last];
          } catch (e) {
            return [[], last];
          }
        }
        try {
          const keys = keyCache.length ? keyCache : await kv.listKeys(200);
          const scored = keys
            .filter((k) => k)
            .map((k) => ({
              k,
              s: k.startsWith(last) ? 0 : levenshtein(k, last),
            }))
            .sort((a, b) => a.s! - b.s! || a.k.localeCompare(b.k))
            .map((x) => x.k);
          if (scored.length) return [scored.slice(0, 50), last];
          return [keys.slice(0, 50), last];
        } catch (e) {
          return [[], last];
        }
      };

      const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
        terminal: true,
        completer,
      });
      try {
        (rl as any).history = history.reverse();
      } catch (e) {}
      rl.setPrompt("kv> ");
      rl.prompt();
      rl.on("line", async (line) => {
        const raw = line.trim();
        if (!raw) {
          rl.prompt();
          return;
        }
        if (/^put\s+/i.test(raw) || /^del\s+/i.test(raw))
          refreshKeyCache().catch(() => {});
        if (raw === "exit" || raw === "quit") {
          rl.close();
          return;
        }
        try {
          await handleShellLine(kv, raw);
        } catch (e) {
          console.error(
            "error:",
            e && (e as any).message ? (e as any).message : e
          );
        }
        try {
          fs.appendFileSync(histFile, raw + "\n");
        } catch (e) {}
        rl.prompt();
      });
      rl.on("close", async () => {
        try {
          await kv.close();
        } catch (e) {}
        process.exit(0);
      });
      break;
    }
    case "help":
      usage();
      break;
    default:
      usage();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

// Shell helper (kept at bottom so main can reference it)
async function handleShellLine(kv: any, line: string) {
  const parts = line.split(/\s+/).filter(Boolean);
  const cmd = parts[0];
  const flags = parts.filter((p) => p.startsWith("--"));
  const asJson = flags.includes("--json");
  const asCompact = flags.includes("--compact") || flags.includes("--c");

  switch (cmd) {
    case "put": {
      const key = parts[1];
      let valueParts = parts.slice(2);
      let ttl: number | undefined = undefined;
      for (let i = 0; i < valueParts.length; i++) {
        const p = valueParts[i] || "";
        const m = p.match(/^--ttl=(\d+)$/);
        if (m) {
          ttl = Number(m[1]);
          valueParts.splice(i, 1);
          break;
        }
        if (p === "--ttl" && i + 1 < valueParts.length) {
          ttl = Number(valueParts[i + 1]);
          valueParts.splice(i, 2);
          break;
        }
        if (p === "-t" && i + 1 < valueParts.length) {
          ttl = Number(valueParts[i + 1]);
          valueParts.splice(i, 2);
          break;
        }
      }
      const value = valueParts.join(" ");
      if (!key) {
        console.log("usage: put <key> <value>");
        break;
      }
      await kv.put(key, value, ttl ? { leaseTtlMs: ttl } : undefined);
      console.log("OK");
      break;
    }
    case "get": {
      const key = parts[1];
      if (!key) {
        console.log("usage: get <key>");
        break;
      }
      const v = await kv.get(key);
      if (asJson) console.log(JSON.stringify({ key, value: v }));
      else console.log(v);
      break;
    }
    case "del": {
      const key = parts[1];
      if (!key) {
        console.log("usage: del <key>");
        break;
      }
      await kv.del(key);
      console.log("OK");
      break;
    }
    case "cas": {
      const key = parts[1];
      const expected = parts[2] === "null" ? null : Number(parts[2]);
      const newVal = parts.slice(3).join(" ");
      if (!key) {
        console.log("usage: cas <key> <expectedRev|null> <newValue|null>");
        break;
      }
      const res = await kv.cas(
        key,
        expected,
        newVal === "null" ? null : newVal
      );
      if (asJson) console.log(JSON.stringify(res));
      else if (res.ok) console.log(`OK rev=${res.rev}`);
      else
        console.log(
          `FAILED (currentRev=${
            typeof res.currentRev === "number" ? res.currentRev : "unknown"
          })`
        );
      break;
    }
    case "watch": {
      const block = parts.includes("-b") || parts.includes("--block");
      const pattern = parts
        .filter((p) => p !== "-b" && p !== "--block")
        .slice(1)
        .join(" ");
      if (!pattern) {
        console.log("usage: watch [-b] <pattern>");
        break;
      }
      const handler = (ev: any) => {
        if (asJson) console.log(JSON.stringify(ev));
        else if (asCompact) console.log(`${ev.key}: ${ev.value}`);
        else console.log("EVENT", JSON.stringify(ev));
      };
      const id = kv.watch(pattern, false, handler);
      console.log("watch started id=", id);
      if (block) {
        console.log("blocking; press Ctrl+C to stop");
        await new Promise<void>((resolve) => {
          const onSig = () => {
            try {
              kv.unwatch(id);
            } catch (e) {}
            process.removeListener("SIGINT", onSig);
            resolve();
          };
          process.on("SIGINT", onSig);
        });
        console.log("watch stopped");
      }
      break;
    }
    case "monitor": {
      console.log("entering monitor mode; press Ctrl+C to exit");
      try {
        const recent = await kv.recentEvents(100);
        if (recent && recent.length) {
          console.log("--- recent events ---");
          for (const r of recent) console.log(JSON.stringify(r));
          console.log("--- streaming new events ---");
        }
      } catch (e) {}
      const tail: any[] = [];
      const handler = (ev: any) => {
        tail.push(ev);
        if (tail.length > 100) tail.shift();
        console.log(JSON.stringify(ev));
      };
      const id = kv.watch({}, false, handler);
      await new Promise<void>((resolve) => {
        const onSig = () => {
          try {
            kv.unwatch(id);
          } catch (e) {}
          process.removeListener("SIGINT", onSig);
          resolve();
        };
        process.on("SIGINT", onSig);
      });
      console.log("monitor stopped");
      break;
    }
    case "unwatch": {
      const id = Number(parts[1]);
      if (!id) {
        console.log("usage: unwatch <id>");
        break;
      }
      kv.unwatch(id);
      console.log("unwatched", id);
      break;
    }
    case "snapshot": {
      const rev = parts[1] ? Number(parts[1]) : undefined;
      const s = await kv.snapshot(rev);
      if (flags.includes("--json")) console.log(JSON.stringify(s, null, 2));
      else
        printTable(
          s.map((r: any) => [r.key, r.value]),
          ["Key", "Value"]
        );
      break;
    }
    case "scan": {
      const opts: any = {};
      for (const p of parts.slice(1)) {
        if (p.startsWith("--start=")) opts.startWith = p.split("=")[1];
        if (p.startsWith("--end=")) opts.endWith = p.split("=")[1];
        if (p.startsWith("--contains=")) opts.contains = p.split("=")[1];
        if (p.startsWith("--exact=")) opts.exact = p.split("=")[1];
        if (p.startsWith("--limit=")) opts.limit = Number(p.split("=")[1]);
        if (p.startsWith("--offset=")) opts.offset = Number(p.split("=")[1]);
        if (p === "--json") opts.json = true;
      }
      const res = await kv.scan(opts);
      if (opts.json) console.log(JSON.stringify(res, null, 2));
      else
        printTable(
          res.map((r: any) => [r.key, r.value]),
          ["Key", "Value"]
        );
      break;
    }
    case "export-subs": {
      const out = parts[1];
      if (!out) {
        console.log("usage: export-subs <out.json>");
        break;
      }
      const snaps = kv.exportSubscribers();
      require("fs").writeFileSync(out, JSON.stringify(snaps, null, 2));
      console.log("exported", out);
      break;
    }
    case "lease.grant": {
      const ttl = Number(parts[1]) || 60000;
      const id = kv.grantLease(ttl);
      console.log("lease", id);
      break;
    }
    case "lease.attach": {
      const id = Number(parts[1]);
      const key = parts[2];
      if (!id || !key) {
        console.log("usage: lease.attach <id> <key>");
        break;
      }
      kv.attachLease(id, key);
      console.log("attached");
      break;
    }
    case "lease.renew": {
      const id = Number(parts[1]);
      const ttl = Number(parts[2]) || 60000;
      if (!id) {
        console.log("usage: lease.renew <id> <ttlMs>");
        break;
      }
      const ok = kv.renewLease(id, ttl);
      console.log(ok ? "OK" : "NOTFOUND");
      break;
    }
    case "lease.revoke": {
      const id = Number(parts[1]);
      const delKeys = parts[2] === "true";
      if (!id) {
        console.log("usage: lease.revoke <id> [deleteKeys]");
        break;
      }
      const ok = kv.revokeLease(id, delKeys);
      console.log(ok ? "OK" : "NOTFOUND");
      break;
    }
    case "tx.begin": {
      const tx = kv.beginTransaction();
      (kv as any)._activeTx = tx;
      console.log("tx started");
      break;
    }
    case "tx.put": {
      const tx = (kv as any)._activeTx;
      if (!tx) {
        console.log("no active tx; use tx.begin");
        break;
      }
      const key = parts[1];
      const val = parts.slice(2).join(" ");
      if (!key) {
        console.log("usage: tx.put <key> <value>");
        break;
      }
      tx.put(Buffer.from(key), Buffer.from(val || ""));
      console.log("queued");
      break;
    }
    case "tx.del": {
      const tx = (kv as any)._activeTx;
      if (!tx) {
        console.log("no active tx; use tx.begin");
        break;
      }
      const key = parts[1];
      if (!key) {
        console.log("usage: tx.del <key>");
        break;
      }
      tx.del(Buffer.from(key));
      console.log("queued");
      break;
    }
    case "tx.commit": {
      const tx = (kv as any)._activeTx;
      if (!tx) {
        console.log("no active tx; use tx.begin");
        break;
      }
      const res = await tx.commit();
      (kv as any)._activeTx = null;
      if (asJson) console.log(JSON.stringify(res));
      else console.log(res.ok ? `OK revs=${res.revs}` : "FAILED");
      break;
    }
    case "tx.abort": {
      const tx = (kv as any)._activeTx;
      if (!tx) {
        console.log("no active tx; use tx.begin");
        break;
      }
      tx.abort();
      (kv as any)._activeTx = null;
      console.log("aborted");
      break;
    }
    case "attach-registry": {
      const p = parts[1];
      if (!p) {
        console.log("usage: attach-registry <registry.json>");
        break;
      }
      const reg = JSON.parse(require("fs").readFileSync(p, "utf-8"));
      const mapping: Record<number, (ev: any) => void> = {};
      for (const k of Object.keys(reg)) {
        const n = Number(k);
        if (reg[k] === "console")
          mapping[n] = (ev) => console.log("EV", JSON.stringify(ev));
      }
      kv.attachHandlerRegistry(mapping);
      console.log("attached registry");
      break;
    }
    case "help": {
      console.log(
        "commands: put|get|del|cas|watch|monitor|unwatch|snapshot|export-subs|attach-registry|lease.*|tx.*|help|exit"
      );
      console.log("use `help <cmd>` for command help");
      break;
    }
    default:
      console.log("unknown command; type help");
  }
}
