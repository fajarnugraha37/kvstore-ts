import { Mutex } from "../locks";

export abstract class WallSched {
  protected writeLock = new Mutex();
  protected flushTimer: any = null;
  protected maxBatchDelayMs = 50;
  protected bgFlushInterval: any = null;

  public abstract flushBatch(): Promise<void>;
  public abstract flush(): Promise<void>;

  protected scheduleFlush() {
    if (this.flushTimer != null) return;
    this.flushTimer = setTimeout(async () => {
      this.flushTimer = null;
      try {
        await this.writeLock.acquire();
        try {
          await this.flushBatch();
        } finally {
          this.writeLock.release();
        }
      } catch (e) {
        // ignore timer errors
      }
    }, this.maxBatchDelayMs);
  }

  protected startBackgroundFlush() {
    if (this.bgFlushInterval != null) return;
    // flush at half the maxBatchDelay by default
    const interval = Math.max(10, Math.floor(this.maxBatchDelayMs / 2));
    this.bgFlushInterval = setInterval(() => {
      // best-effort, don't await
      this.flush().catch(() => {});
    }, interval);
  }

  protected stopBackgroundFlush() {
    if (this.bgFlushInterval != null) {
      clearInterval(this.bgFlushInterval);
      this.bgFlushInterval = null;
    }
  }
}
