const DEFAULT_INITIAL_CAPACITY = 16;
const MIN_CAPACITY = 16;

/**
 * A FIFO ring buffer backed by a circular array. Provides:
 *
 * - `push()` — O(1) amortized (O(n) on resize)
 * - `shift()` — O(1)
 * - `size`    — O(1)
 * - `drain()` — O(n)
 *
 * The buffer grows (doubles) when full and shrinks (halves) when
 * utilization drops below 25%, bounded by {@link MIN_CAPACITY}.
 */
export class RingBuffer<T> {
  #buffer: (T | undefined)[];
  #head = 0;
  #size = 0;
  #capacity: number;

  constructor(initialCapacity = DEFAULT_INITIAL_CAPACITY) {
    this.#capacity = Math.max(MIN_CAPACITY, initialCapacity);
    this.#buffer = Array.from({length: initialCapacity});
  }

  get size(): number {
    return this.#size;
  }

  push(value: T): void {
    if (this.#size === this.#capacity) {
      this.#grow();
    }
    this.#buffer[(this.#head + this.#size) % this.#capacity] = value;
    this.#size++;
  }

  /** Removes and returns the front element, or `undefined` if empty. */
  shift(): T | undefined {
    if (this.#size === 0) {
      return undefined;
    }
    const value = this.#buffer[this.#head];
    this.#buffer[this.#head] = undefined; // allow GC of removed element
    this.#head = (this.#head + 1) % this.#capacity;
    this.#size--;

    // Shrink when utilization drops below 25%, keeping a minimum capacity.
    if (
      this.#size > 0 &&
      this.#size < this.#capacity >> 2 &&
      this.#capacity > MIN_CAPACITY
    ) {
      this.#shrink();
    }

    return value;
  }

  /**
   * Removes all elements matching `value` (by identity `===`).
   *
   * This is O(n) — suitable for rare operations like cancellation.
   *
   * @returns The number of elements removed.
   */
  delete(value: T): number {
    if (this.#size === 0) {
      return 0;
    }
    // Rebuild the buffer without matching elements.
    const newBuffer: (T | undefined)[] = Array.from({length: this.#capacity});
    let newSize = 0;
    let count = 0;
    for (let i = 0; i < this.#size; i++) {
      const item = this.#buffer[(this.#head + i) % this.#capacity];
      if (item === value) {
        count++;
      } else {
        newBuffer[newSize++] = item;
      }
    }
    this.#buffer = newBuffer;
    this.#head = 0;
    this.#size = newSize;
    return count;
  }

  /**
   * Removes and returns all elements in FIFO order, resetting the buffer.
   */
  drain(): T[] {
    const result = Array.from<T>({length: this.#size});
    for (let i = 0; i < this.#size; i++) {
      result[i] = this.#buffer[(this.#head + i) % this.#capacity] as T;
    }
    this.#head = 0;
    this.#size = 0;
    this.#buffer = Array.from({length: this.#capacity});
    return result;
  }

  /**
   * Grows the buffer in place by doubling its length and relocating
   * only the wrapped-around elements (those before #head) into the
   * newly available space. No new array is allocated.
   */
  #grow(): void {
    const oldLen = this.#buffer.length;
    const newLen = oldLen * 2;
    this.#buffer.length = newLen;
    this.#capacity = newLen;

    // Copy wrapped-around elements (indices 0..head-1) into the new space.
    for (let i = 0; i < this.#head; i++) {
      this.#buffer[oldLen + i] = this.#buffer[i];
      this.#buffer[i] = undefined; // ensure gc when the element is removed
    }
  }

  /**
   * Shrinks the buffer in place by halving its length. Elements whose
   * indices fall in the removed half are relocated into the lower half,
   * then the array is truncated.
   */
  #shrink(): void {
    const oldLen = this.#buffer.length;
    const newLen = Math.max(MIN_CAPACITY, oldLen >> 1);

    // Move elements that sit in the upper half down by newLen.
    const left = Math.max(newLen, this.#head);
    const right = Math.min(this.#head + this.#size, oldLen);
    for (let i = left; i < right; i++) {
      this.#buffer[i - newLen] = this.#buffer[i];
    }

    if (this.#head >= newLen) {
      this.#head -= newLen;
    }
    this.#buffer.length = newLen;
    this.#capacity = newLen;
  }
}
