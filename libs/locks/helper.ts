import type { MutexInterface } from "./mutex.types";
import type { Priority, SemaphoreInterface } from "./semaphore.types";

export function insertSorted<T extends Priority>(a: T[], v: T) {
  const i = findIndexFromEnd(a, (other) => v.priority <= other.priority);
  a.splice(i + 1, 0, v);
}

export function findIndexFromEnd<T>(
  a: T[],
  predicate: (e: T) => boolean
): number {
  for (let i = a.length - 1; i >= 0; i--) {
    if (a[i] && predicate(a[i]!)) {
      return i;
    }
  }
  return -1;
}

export function isSemaphore(sync: SemaphoreInterface | MutexInterface): sync is SemaphoreInterface {
    return (sync as SemaphoreInterface).getValue !== undefined;
}