import { E_CANCELED } from "./error";
import { findIndexFromEnd, insertSorted } from "./helper";
import type { SemaphoreInterface, QueueEntry, Waiter } from "./semaphore.types";

/**
 * A counting semaphore implementation.
 * This semaphore allows a certain number of concurrent accesses, with a weight
 * assigned to each access request. Higher priority requests can preempt lower
 * priority ones.
 */
export class Semaphore implements SemaphoreInterface {
  /**
   * The current queue of pending requests.
   */
  private _queue: Array<QueueEntry> = [];
  /**
   * The current weighted waiters.
   */
  private _weightedWaiters: Array<Array<Waiter>> = [];

  /**
   * @param _value The initial value of the semaphore.
   * @param _cancelError The error to throw when the operation is canceled.
   */
  constructor(
    private _value: number,
    private _cancelError: Error = E_CANCELED
  ) {}

  /**
   * Acquire a semaphore with the given weight and priority.
   * @param weight The weight of the access request.
   * @param priority The priority of the access request.
   * @returns A promise that resolves with the current value and a releaser function.
   */
  acquire(
    weight = 1,
    priority = 0
  ): Promise<[number, SemaphoreInterface.Releaser]> {
    if (weight <= 0)
      throw new Error(`invalid weight ${weight}: must be positive`);

    return new Promise((resolve, reject) => {
      const task: QueueEntry = { resolve, reject, weight, priority };
      // Find the correct position in the queue
      const i = findIndexFromEnd(
        this._queue,
        (other) => priority <= other.priority
      );
      // If the item is the highest priority, it goes to the front
      if (i === -1 && weight <= this._value) {
        // Needs immediate dispatch, skip the queue
        this._dispatchItem(task);
      } else {
        // Needs to be queued
        this._queue.splice(i + 1, 0, task);
      }
    });
  }

  /**
   * Run a callback exclusively with the given weight and priority.
   * @param callback The callback to run.
   * @param weight The weight of the access request.
   * @param priority The priority of the access request.
   * @returns A promise that resolves with the result of the callback.
   */
  async runExclusive<T>(
    callback: SemaphoreInterface.Worker<T>,
    weight = 1,
    priority = 0
  ): Promise<T> {
    // Acquire the semaphore
    const [value, release] = await this.acquire(weight, priority);

    try {
      // Run the callback
      return await callback(value);
    } finally {
      release();
    }
  }

  /**
   * Wait for the semaphore to be unlocked.
   * @param weight The weight of the access request.
   * @param priority The priority of the access request.
   * @returns A promise that resolves when the semaphore is unlocked.
   */
  waitForUnlock(weight = 1, priority = 0): Promise<void> {
    if (weight <= 0)
      throw new Error(`invalid weight ${weight}: must be positive`);

    // Check if the semaphore can be locked immediately
    if (this._couldLockImmediately(weight, priority)) {
      return Promise.resolve();
    } else {
      // Needs to be queued
      return new Promise((resolve) => {
        // Wait for the semaphore to be unlocked
        if (!this._weightedWaiters[weight - 1])
          this._weightedWaiters[weight - 1] = [];
        // Insert the waiter into the correct position
        insertSorted(this._weightedWaiters[weight - 1]!, { resolve, priority });
      });
    }
  }

  /**
   * Check if the semaphore is currently locked.
   * @returns True if the semaphore is locked, false otherwise.
   */
  isLocked(): boolean {
    return this._value <= 0;
  }

  /**
   * Get the current value of the semaphore.
   * @returns The current value of the semaphore.
   */
  getValue(): number {
    return this._value;
  }

  /**
   * Set the current value of the semaphore.
   * @param value The new value of the semaphore.
   */
  setValue(value: number): void {
    this._value = value;
    this._dispatchQueue();
  }

  /**
   * Release the semaphore, increasing its value.
   * @param weight The weight of the release operation.
   */
  release(weight = 1): void {
    if (weight <= 0)
      throw new Error(`invalid weight ${weight}: must be positive`);

    this._value += weight;
    this._dispatchQueue();
  }

  /**
   * Cancel all pending requests.
   */
  cancel(): void {
    this._queue.forEach((entry) => entry.reject(this._cancelError));
    this._queue = [];
  }

  /**
   * Dispatch the next item in the queue.
   */
  private _dispatchQueue(): void {
    this._drainUnlockWaiters();
    // Find the correct position in the queue
    while (this._queue.length > 0 && this._queue[0]!.weight <= this._value) {
      this._dispatchItem(this._queue.shift()!);
      this._drainUnlockWaiters();
    }
  }

  /**
   * Dispatch the next item in the queue.
   * @param item The item to dispatch.
   */
  private _dispatchItem(item: QueueEntry): void {
    const previousValue = this._value;
    this._value -= item.weight;
    // Notify the item that it has been dispatched
    item.resolve([previousValue, this._newReleaser(item.weight)]);
  }

  /**
   * Create a new releaser function for the given weight.
   * @param weight The weight of the release operation.
   * @returns A function that releases the semaphore when called.
   */
  private _newReleaser(weight: number): () => void {
    let called = false;

    return () => {
      if (called) return;
      called = true;

      this.release(weight);
    };
  }

  /**
   * Drain all unlock waiters.
   */
  private _drainUnlockWaiters(): void {
    // If there are no queued items, resolve all waiters for the current value
    if (this._queue.length === 0) {
      // Resolve all waiters for the current value
      for (let weight = this._value; weight > 0; weight--) {
        const waiters = this._weightedWaiters[weight - 1];
        if (!waiters) continue;
        waiters.forEach((waiter) => waiter.resolve());
        this._weightedWaiters[weight - 1] = [];
      }
    } else {
      // If there are queued items, resolve waiters for all weights up to the current value
      const queuedPriority = this._queue[0]!.priority;
      // Find the correct position in the waiters
      for (let weight = this._value; weight > 0; weight--) {
        const waiters = this._weightedWaiters[weight - 1];
        if (!waiters) continue;
        const i = waiters.findIndex(
          (waiter) => waiter.priority <= queuedPriority
        );
        (i === -1 ? waiters : waiters.splice(0, i)).forEach((waiter) =>
          waiter.resolve()
        );
      }
    }
  }

  /**
   * Check if the semaphore can be locked immediately.
   * @param weight The weight of the lock operation.
   * @param priority The priority of the lock operation.
   * @returns True if the semaphore can be locked immediately, false otherwise.
   */
  private _couldLockImmediately(weight: number, priority: number) {
    return (
      (this._queue.length === 0 || this._queue[0]!.priority < priority) &&
      weight <= this._value
    );
  }
}

