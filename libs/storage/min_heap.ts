/**
 * A min-heap implementation in TypeScript.
 */
export class MinHeap<T> {
  /**
   * The heap's data.
   */
  private data: T[] = [];

  /**
   * Creates a min-heap.
   * @param cmp - A comparison function that defines the heap order.
   */
  constructor(private cmp: (a: T, b: T) => number) {
    // no-op
  }

  /**
   * Pushes a new value onto the heap.
   * @param v - The value to push.
   */
  push(v: T) {
    this.data.push(v);
    this.bubbleUp(this.data.length - 1);
  }

  /**
   * Pops the top value off the heap.
   * @returns The top value of the heap, or undefined if the heap is empty.
   */
  pop(): T | undefined {
    if (this.data.length === 0) return undefined;
    const top = this.data[0]!;
    const last = this.data.pop() as T;
    if (this.data.length) {
      this.data[0] = last;
      this.sinkDown(0);
    }
    return top;
  }

  /**
   * Returns the value at the top of the heap without removing it.
   * @returns The value at the top of the heap, or undefined if the heap is empty.
   */
  peek(): T | undefined {
    return this.data[0];
  }

  /**
   * Returns the number of items in the heap.
   */
  get size() {
    return this.data.length;
  }

  /**
   * Bubbles up the item at the given index to maintain the heap property.
   * @param i - The index of the item to bubble up.
   */
  private bubbleUp(i: number) {
    const item = this.data[i] as T;
    while (i > 0) {
      const p = Math.floor((i - 1) / 2);
      if (this.cmp(this.data[p] as T, item) <= 0) break;
      this.data[i] = this.data[p] as T;
      i = p;
    }
    this.data[i] = item;
  }

  /**
   * Sinks down the item at the given index to maintain the heap property.
   * @param i - The index of the item to sink down.
   */
  private sinkDown(i: number) {
    const n = this.data.length;
    const item = this.data[i] as T;
    while (true) {
      let left = 2 * i + 1;
      let right = left + 1;
      let smallest = i;
      if (
        left < n &&
        this.cmp(this.data[left] as T, this.data[smallest] as T) < 0
      )
        smallest = left;
      if (
        right < n &&
        this.cmp(this.data[right] as T, this.data[smallest] as T) < 0
      )
        smallest = right;
      if (smallest === i) break;
      this.data[i] = this.data[smallest] as T;
      i = smallest;
    }
    this.data[i] = item;
  }
}
