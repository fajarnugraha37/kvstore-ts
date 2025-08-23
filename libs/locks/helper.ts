import type { MutexInterface } from "./mutex.types";
import type { Priority, SemaphoreInterface } from "./semaphore.types";

/**
 * Insert an item into a sorted array.
 * @param a The array to insert into.
 * @param v The item to insert.
 */
export function insertSorted<T extends Priority>(a: T[], v: T) {
  const i = findIndexFromEnd(a, (other) => v.priority <= other.priority);
  a.splice(i + 1, 0, v);
}

/**
 * Find the index of the last element in the array that satisfies the predicate.
 * @param a The array to search.
 * @param predicate The predicate to test each element.
 * @returns The index of the last element that satisfies the predicate, or -1 if none do.
 */
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

/**
 * Check if the given synchronization primitive is a semaphore.
 * @param sync The synchronization primitive to check.
 * @returns True if the primitive is a semaphore, false otherwise.
 */
export function isSemaphore(sync: SemaphoreInterface | MutexInterface): sync is SemaphoreInterface {
    return (sync as SemaphoreInterface).getValue !== undefined;
}