import {
  writeFileSync,
  readFileSync,
  renameSync,
  existsSync,
  mkdirSync,
} from "node:fs";

export type PersistedFileMeta = {
  file: string;
  minKeyHex: string;
  maxKeyHex: string;
  size: number;
  level?: number;
  walOffset?: number;
  createdAt?: number;
};

export class Manifest {
  private files: PersistedFileMeta[] = [];
  private walOffset: number = 0;
  constructor(private dir: string) {}

  static load(dir: string) {
    const m = new Manifest(dir);
    try {
      const p = `${dir}/manifest.json`;
      if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
      if (!existsSync(p)) return m;
      const buf = readFileSync(p, "utf8");
      const json = JSON.parse(buf);
      // support legacy format (array) and new format { files, walOffset }
      if (Array.isArray(json)) {
        m.files = json as PersistedFileMeta[];
      } else if (json && typeof json === "object") {
        m.files = (json.files || []) as PersistedFileMeta[];
        m.walOffset = typeof json.walOffset === "number" ? json.walOffset : 0;
      }
    } catch (e) {
      // ignore and return empty manifest
    }
    return m;
  }

  listFiles() {
    return Array.from(this.files);
  }

  listFilesByLevel(level: number) {
    return this.files.filter((f) => (f.level || 0) === level).slice();
  }

  addFile(meta: PersistedFileMeta) {
    // default level 0 when omitted
    if (typeof meta.level !== "number") meta.level = 0;
  // ensure createdAt is present for TTL/GC decisions
  if (typeof meta.createdAt !== 'number') meta.createdAt = Date.now();
    this.files.push(meta);
    this.persist();
  }

  removeFiles(filePaths: string[]) {
    this.files = this.files.filter((f) => !filePaths.includes(f.file));
    this.persist();
  }

  /**
   * Replace a set of files with new metas atomically (in-memory then persist once).
   */
  replaceFiles(oldFilePaths: string[], newMetas: PersistedFileMeta[]) {
    this.files = this.files.filter((f) => !oldFilePaths.includes(f.file));
    // ensure createdAt is present on new metas
    for (const m of newMetas) {
      if (typeof m.createdAt !== 'number') m.createdAt = Date.now();
      if (typeof m.level !== 'number') m.level = 0;
    }
    this.files.push(...newMetas);
    this.persist();
  }

  getWalOffset() {
    return this.walOffset;
  }
  setWalOffset(offset: number) {
    this.walOffset = offset;
    this.persist();
  }

  persist() {
    const p = `${this.dir}/manifest.json`;
    const tmp = `${p}.tmp`;
    try {
      if (!existsSync(this.dir)) mkdirSync(this.dir, { recursive: true });
      const out = { files: this.files, walOffset: this.walOffset };
      writeFileSync(tmp, JSON.stringify(out, null, 2), "utf8");
      renameSync(tmp, p);
    } catch (e) {
      // best-effort
      try {
        if (existsSync(tmp))
          writeFileSync(
            p,
            JSON.stringify(
              { files: this.files, walOffset: this.walOffset },
              null,
              2
            ),
            "utf8"
          );
      } catch {}
    }
  }
}

export default Manifest;
