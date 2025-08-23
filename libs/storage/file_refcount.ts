import fs, {
  existsSync,
  unlinkSync,
  writeFileSync,
  readdirSync,
  readFileSync,
} from "node:fs";
import path, { dirname, basename, join } from "node:path";
import { randomBytes } from "node:crypto";

// Simple in-process reference counting for SST files.
// Usage:
//  - readers call acquire(path) when they begin reading and release(path) when done.
//  - writers/compactor call requestDelete(path) to attempt deletion; deletion will be deferred until refcount==0.

const counts = new Map<string, number>();
const pendingDelete = new Set<string>();
// sentinel file created by this process per path
const sentinels = new Map<string, string>();

export function sentinelPrefix(path: string) {
  const base = basename(path);
  return `${base}.lock.`;
}

function ownSentinelName(path: string) {
  // include pid and a small random token
  const id = randomBytes(4).toString("hex");
  return `${basename(path)}.lock.${process.pid}.${id}.ref`;
}

export function otherSentinelsExist(path: string) {
  try {
    const dir = dirname(path);
    const base = basename(path);
    const prefix = `${base}.lock.`;
    const files = readdirSync(dir);
    for (const name of files) {
      if (!name.startsWith(prefix)) continue;
      const full = join(dir, name);
      try {
        // try to read pid from content (if present)
        const buf = readFileSync(full, "utf8");
        const pidStr = String(buf).split(":")[0];
        const pid = parseInt(pidStr || "0", 10);
        if (pid > 0) {
          try {
            // check if process exists
            process.kill(pid, 0);
            return true;
          } catch (e) {
            // process not alive; cleanup stale sentinel
            try {
              unlinkSync(full);
            } catch {}
            continue;
          }
        } else {
          // can't parse pid, treat as present
          return true;
        }
      } catch (e) {
        // if we can't read, assume it exists
        return true;
      }
    }
  } catch (e) {}
  return false;
}

export function acquire(path: string) {
  const cur = counts.get(path) || 0;
  counts.set(path, cur + 1);
  // create on-disk sentinel only once per process for this path
  if (cur === 0) {
    try {
      const dir = dirname(path);
      const name = ownSentinelName(path);
      const full = join(dir, name);
      writeFileSync(full, `${process.pid}:${Date.now()}`);
      sentinels.set(path, full);
    } catch (e) {
      // best-effort
    }
  }
}

export function release(path: string) {
  const cur = counts.get(path) || 0;
  if (cur <= 1) {
    counts.delete(path);
    // remove our sentinel file
    try {
      const s = sentinels.get(path);
      if (s) {
        try {
          unlinkSync(s);
        } catch {}
        sentinels.delete(path);
      }
    } catch {}
    // If there is a pending delete request, perform it now if no other sentinels remain
    if (pendingDelete.has(path)) {
      try {
        pendingDelete.delete(path);
        if (!otherSentinelsExist(path) && existsSync(path)) unlinkSync(path);
      } catch {}
    }
  } else {
    counts.set(path, cur - 1);
  }
}

export function requestDelete(path: string) {
  const cur = counts.get(path) || 0;
  try {
    if (cur === 0 && !otherSentinelsExist(path)) {
      if (existsSync(path)) unlinkSync(path);
    } else {
      // defer deletion until readers finish; create a delete marker to persist intent
      pendingDelete.add(path);
      try {
        const marker = `${path}.delete`;
        writeFileSync(marker, `${process.pid}:${Date.now()}`);
      } catch {}
    }
  } catch (e) {
    // best-effort
  }
}

export function cleanupDeleteMarkers(
  dir: string,
  opts?: { staleMs?: number; log?: boolean }
) {
  const scanned = { markers: 0, removed: 0, filesDeleted: 0 };
  try {
    const files = fs.readdirSync(dir);
    const staleMs =
      typeof (opts && opts.staleMs) === "number"
        ? opts!.staleMs!
        : 24 * 3600 * 1000; // 1 day default
    for (const name of files) {
      if (!name.endsWith(".delete")) continue;
      scanned.markers++;
      const marker = path.join(dir, name);
      const base = name.slice(0, -".delete".length);
      const target = path.join(dir, base);
      let content = "";
      try {
        content = fs.readFileSync(marker, "utf8");
      } catch {}
      const parts = String(content || "").split(":");
      const ts = parseInt(parts[1] || "0", 10) || 0;
      const age = Date.now() - ts;
      // If file doesn't exist, remove the marker
      if (!fs.existsSync(target)) {
        try {
          fs.unlinkSync(marker);
          scanned.removed++;
          if (opts && opts.log)
            console.log(
              "file_refcount: removed stale marker for missing file",
              marker
            );
        } catch {}
        continue;
      }
      // If marker is older than staleMs and no other sentinels exist, delete target and marker
      if (age >= staleMs) {
        try {
          // if other sentinels exist, skip
          if (!otherSentinelsExist(target)) {
            try {
              if (fs.existsSync(target)) {
                fs.unlinkSync(target);
                scanned.filesDeleted++;
                if (opts && opts.log)
                  console.log("file_refcount: deleted orphaned file", target);
              }
            } catch {}
            try {
              fs.unlinkSync(marker);
              scanned.removed++;
              if (opts && opts.log)
                console.log("file_refcount: removed marker", marker);
            } catch {}
          }
        } catch {}
      }
    }
  } catch (e) {
    // ignore
  }
  return scanned;
}

export function refCount(path: string) {
  return counts.get(path) || 0;
}

export default { acquire, release, requestDelete, refCount };
