export type WallSchedCallbacks = {
  maxBatchDelayMs?: number;
  onFlushBatch?: () => Promise<void>;
  onFlush?: () => Promise<void>;
  onMetaFlush?: () => Promise<void>;
  metaFlushInterval?: number;
};

/**
 * Small composable scheduler helper for WAL implementations.
 *
 * This is intentionally minimal: it provides `scheduleFlush`, `startBackgroundFlush`
 * and `stopBackgroundFlush` behavior and delegates actual work to provided
 * callbacks. It is safe to compose into different WAL implementations without
 * forcing inheritance.
 */
export class WallSchedHelper {
  private flushTimer: any = null;
  private bgFlushInterval: any = null;
  private maxBatchDelayMs: number;
  private onFlushBatch: () => Promise<void>;
  private onFlush: () => Promise<void>;
  private onMetaFlush: () => Promise<void>;
  private metaFlushInterval: number;
  private appendSinceMeta = 0;

  constructor(cb: WallSchedCallbacks = {}) {
    this.maxBatchDelayMs = cb.maxBatchDelayMs ?? 50;
    this.onFlushBatch = cb.onFlushBatch ?? (async () => {});
    this.onFlush = cb.onFlush ?? (async () => {});
    this.onMetaFlush = cb.onMetaFlush ?? (async () => {});
    this.metaFlushInterval = (cb as any).metaFlushInterval ?? 64;
  }

  scheduleFlush() {
    if (this.flushTimer != null) return;
    this.flushTimer = setTimeout(async () => {
      this.flushTimer = null;
      try {
        await this.onFlushBatch();
      } catch (e) {
        // swallow timer errors
      }
    }, this.maxBatchDelayMs);
  }

  clearFlushTimer() {
    if (this.flushTimer != null) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
  }

  startBackgroundFlush() {
    if (this.bgFlushInterval != null) return;
    const interval = Math.max(10, Math.floor(this.maxBatchDelayMs / 2));
    this.bgFlushInterval = setInterval(() => {
      // best-effort
      this.onFlush().catch(() => {});
    }, interval);
  }

  stopBackgroundFlush() {
    if (this.bgFlushInterval != null) {
      clearInterval(this.bgFlushInterval);
      this.bgFlushInterval = null;
    }
  }

  /**
   * Notify helper that `bytes` durable were appended. When `appendSinceMeta`
   * reaches `metaFlushInterval` the helper will call `onMetaFlush()` and reset
   * the counter.
   */
  async noteAppend(bytes: number) {
    // increase a logical counter per-append; size is unused but kept for future use
    this.appendSinceMeta++;
    if (this.appendSinceMeta >= this.metaFlushInterval) {
      this.appendSinceMeta = 0;
      try {
        await this.onMetaFlush();
      } catch (e) {
        // swallow errors
      }
    }
  }
}
