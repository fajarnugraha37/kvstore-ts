import type { MutexInterface } from "./mutex.types";
import { Semaphore } from "./semaphore";

/**
 * A mutex implementation using a semaphore.
 * This mutex allows exclusive access to a critical section.
 */
export class Mutex implements MutexInterface {
  /**
   * The underlying semaphore used for synchronization.
   */
  private _semaphore: Semaphore;

  /**
   * @param cancelError The error to throw when the operation is canceled.
   */
  constructor(cancelError?: Error) {
    this._semaphore = new Semaphore(1, cancelError);
  }

  /**
   * Acquire the mutex.
   * @param priority The priority of the acquisition request.
   * @returns A promise that resolves with a releaser function.
   */
  async acquire(priority = 0): Promise<MutexInterface.Releaser> {
    const [, releaser] = await this._semaphore.acquire(1, priority);

    return releaser;
  }

  /**
   * Run a callback exclusively with the given priority.
   * @param callback The callback to run.
   * @param priority The priority of the execution request.
   * @returns A promise that resolves with the result of the callback.
   */
  runExclusive<T>(
    callback: MutexInterface.Worker<T>,
    priority = 0
  ): Promise<T> {
    return this._semaphore.runExclusive(() => callback(), 1, priority);
  }

  /**
   * Check if the mutex is currently locked.
   * @returns True if the mutex is locked, false otherwise.
   */
  isLocked(): boolean {
    return this._semaphore.isLocked();
  }

  /**
   * Wait for the mutex to be unlocked.
   * @param priority The priority of the wait request.
   * @returns A promise that resolves when the mutex is unlocked.
   */
  waitForUnlock(priority = 0): Promise<void> {
    return this._semaphore.waitForUnlock(1, priority);
  }

  /**
   * Release the mutex.
   */
  release(): void {
    if (this._semaphore.isLocked()) this._semaphore.release();
  }

  /**
   * Cancel all pending requests.
   */
  cancel(): void {
    return this._semaphore.cancel();
  }
}
