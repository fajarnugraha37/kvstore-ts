export interface SemaphoreInterface {
  acquire(
    weight?: number,
    priority?: number
  ): Promise<[number, SemaphoreInterface.Releaser]>;

  runExclusive<T>(
    callback: SemaphoreInterface.Worker<T>,
    weight?: number,
    priority?: number
  ): Promise<T>;

  waitForUnlock(weight?: number, priority?: number): Promise<void>;

  isLocked(): boolean;

  getValue(): number;

  setValue(value: number): void;

  release(weight?: number): void;

  cancel(): void;
}

export namespace SemaphoreInterface {
  export interface Releaser {
    (): void;
  }

  export interface Worker<T> {
    (value: number): Promise<T> | T;
  }
}

export interface Priority {
  priority: number;
}

export interface QueueEntry {
  resolve(result: [number, SemaphoreInterface.Releaser]): void;
  reject(error: unknown): void;
  weight: number;
  priority: number;
}

export interface Waiter {
  resolve(): void;
  priority: number;
}
