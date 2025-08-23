import http from "node:http";
import https from "node:https";
import { Subscriber } from "../subscriber";
import type { WatchBackend } from "../watch_manager";

export class HttpForwarder implements WatchBackend {
  private subs = new Map<number, Subscriber>();
  private nextId = 1;
  // opts: { retries?: number, backoffMs?: number, headers?: Record<string,string>, timeoutMs?: number, auth?: { type:'bearer'|'basic', token?:string, user?:string, pass?:string }, rejectUnauthorized?: boolean }
  constructor(private endpoint: string, private opts?: any) {}

  add(filter: any, once = false) {
    const id = this.nextId++;
    const s = new Subscriber(id, filter, once);
    this.subs.set(s.id, s);
    return s;
  }

  remove(sub: Subscriber) {
    this.subs.delete(sub.id);
    try {
      sub.close();
    } catch {}
  }

  publish(ev: any) {
    // forward event via POST with retries, headers, auth and TLS options
    const body = JSON.stringify(ev || {});
    const u = new URL(this.endpoint);
    const isHttps = u.protocol === "https:";
    const maxRetries =
      typeof this.opts?.retries === "number" ? this.opts.retries : 3;
    const baseBackoff =
      typeof this.opts?.backoffMs === "number" ? this.opts.backoffMs : 200;
    const headers: any = Object.assign(
      { "content-type": "application/json" },
      this.opts?.headers || {}
    );
    if (this.opts?.auth) {
      if (this.opts.auth.type === "bearer" && this.opts.auth.token)
        headers["authorization"] = `Bearer ${this.opts.auth.token}`;
      else if (this.opts.auth.type === "basic" && this.opts.auth.user)
        headers["authorization"] = `Basic ${Buffer.from(
          `${this.opts.auth.user}:${this.opts.auth.pass || ""}`
        ).toString("base64")}`;
    }

    const doOne = (attempt: number): Promise<void> => {
      return new Promise((resolve) => {
        try {
          const optsReq: any = {
            method: "POST",
            hostname: u.hostname,
            port: u.port || (isHttps ? 443 : 80),
            path: u.pathname + (u.search || ""),
            headers,
            timeout:
              typeof this.opts?.timeoutMs === "number"
                ? this.opts.timeoutMs
                : 5000,
          };
          if (typeof this.opts?.rejectUnauthorized === "boolean")
            optsReq.rejectUnauthorized = !!this.opts.rejectUnauthorized;
          const transport = isHttps ? https : http;
          const req = transport.request(optsReq, (res: any) => {
            // consume body to avoid socket leak
            res.on && res.on("data", () => {});
            res.on && res.on("end", () => {});
            // treat 2xx as success
            if (
              res &&
              typeof res.statusCode === "number" &&
              res.statusCode >= 200 &&
              res.statusCode < 300
            )
              return resolve();
            // otherwise treat as failure and maybe retry
            const backoff = baseBackoff * Math.pow(2, attempt);
            if (attempt < maxRetries) {
              setTimeout(() => resolve(doOne(attempt + 1)), backoff);
            } else resolve();
          });
          req.on("error", () => {
            const backoff = baseBackoff * Math.pow(2, attempt);
            if (attempt < maxRetries)
              setTimeout(() => resolve(doOne(attempt + 1)), backoff);
            else resolve();
          });
          try {
            req.write(body);
          } catch {}
          req.end();
        } catch (e) {
          // swallow errors and resolve
          resolve();
        }
      });
    };

    // fire-and-forget
    void doOne(0).catch(() => {});
  }

  exportSnapshots() {
    const out: any[] = [];
    for (const s of this.subs.values()) out.push(s.toSnapshot());
    return out;
  }

  // No-op restore: forwarding backend cannot reattach event handlers, but we can
  // preserve lastSeenRev metadata so the system can resume WAL replay correctly.
  restoreSnapshots(snaps: any[]) {
    if (!Array.isArray(snaps)) return;
    for (const s of snaps) {
      try {
        const id = typeof s.id === "number" ? s.id : this.nextId++;
        const sub = new Subscriber(id, s.filter, !!s.once);
        Subscriber.restoreSnapshot(sub, s);
        this.subs.set(sub.id, sub);
        if (sub.id >= this.nextId) this.nextId = sub.id + 1;
      } catch {}
    }
  }

  async replayFromScan(scanIterable: AsyncIterable<any>, minRev?: number) {
    for await (const rec of scanIterable) {
      if (!rec || !rec.value) continue;
      const v = rec.value;
      if (v.tx && Array.isArray(v.ops)) {
        const txLeaseId = typeof v.leaseId === "number" ? v.leaseId : undefined;
        const txLeaseExpiresAt =
          typeof v.leaseExpiresAt === "number" ? v.leaseExpiresAt : undefined;
        const txCreatedAt =
          typeof v.createdAt === "number" ? v.createdAt : undefined;
        for (const op of v.ops) {
          if (!op) continue;
          const augmented = Object.assign({}, op);
          if (
            typeof augmented.leaseId !== "number" &&
            typeof txLeaseId === "number"
          )
            augmented.leaseId = txLeaseId;
          if (
            typeof augmented.leaseExpiresAt !== "number" &&
            typeof txLeaseExpiresAt === "number"
          )
            augmented.leaseExpiresAt = txLeaseExpiresAt;
          if (
            typeof augmented.createdAt !== "number" &&
            typeof txCreatedAt === "number"
          )
            augmented.createdAt = txCreatedAt;
          this._maybePublishOp(augmented, minRev);
        }
      } else {
        if (v.key !== undefined) this._maybePublishOp(v, minRev);
      }
    }
  }

  private _maybePublishOp(op: any, minRev?: number) {
    try {
      const rev = typeof op.rev === "number" ? op.rev : undefined;
      if (typeof minRev === "number") {
        if (typeof rev !== "number" || rev <= minRev) return;
      }
      const key = String(op.key);
      const val = op.value == null ? null : String(op.value);
      const ev: any = {
        key,
        value: val,
        rev,
        createdAt: op.createdAt,
        leaseId: op.leaseId,
        leaseExpiresAt: op.leaseExpiresAt,
      };
      this.publish(ev);
    } catch {}
  }

  getSubscriberById(id: number) {
    return this.subs.get(id) || null;
  }
}
