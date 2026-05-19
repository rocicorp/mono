// High-volume RM -> VS streams should not make every FIFO dequeue move the
// rest of the array. Keep the head-index/compaction bookkeeping in one small
// type so hot queues can stay O(1) without spreading that machinery through
// the caller.
export class HeadIndexedQueue<T> {
  // `undefined` is a valid queue value for some callers, so deleted slots use a
  // private sentinel instead of overloading undefined.
  readonly #entries: (T | EmptySlot)[] = [];
  // #head is the first slot worth scanning; #count is the number of live values
  // after holes from deletes/shifts. Keeping both lets shift() stay O(1) while
  // deleteMatching() can remove arbitrary pending entries.
  #head = 0;
  #count = 0;

  get size(): number {
    return this.#count;
  }

  push(value: T): void {
    this.#entries.push(value);
    this.#count++;
  }

  peek(): T | undefined {
    if (this.#count === 0) {
      return undefined;
    }
    for (let i = this.#head; i < this.#entries.length; i++) {
      const value = this.#entries[i];
      if (value !== EMPTY_SLOT) {
        return value;
      }
    }
    return undefined;
  }

  shift(): T | undefined {
    if (this.#count === 0) {
      this.clear();
      return undefined;
    }

    while (this.#head < this.#entries.length) {
      const value = this.#entries[this.#head];
      // Clear the reference before returning so long-lived streams do not keep
      // consumed payloads alive until the next compaction.
      this.#entries[this.#head] = EMPTY_SLOT;
      this.#head++;
      if (value !== EMPTY_SLOT) {
        this.#count--;
        this.#maybeCompact();
        return value;
      }
    }

    this.clear();
    return undefined;
  }

  last(): T | undefined {
    if (this.#count === 0) {
      return undefined;
    }
    for (let i = this.#entries.length - 1; i >= this.#head; i--) {
      const value = this.#entries[i];
      if (value !== EMPTY_SLOT) {
        return value;
      }
    }
    return undefined;
  }

  replaceLast(value: T): void {
    for (let i = this.#entries.length - 1; i >= this.#head; i--) {
      if (this.#entries[i] !== EMPTY_SLOT) {
        this.#entries[i] = value;
        return;
      }
    }
    this.push(value);
  }

  deleteFirst(value: T): boolean {
    for (let i = this.#head; i < this.#entries.length; i++) {
      if (this.#entries[i] === value) {
        this.#entries[i] = EMPTY_SLOT;
        this.#count--;
        this.#maybeCompact();
        return true;
      }
    }
    return false;
  }

  deleteMatching(predicate: (value: T) => boolean): number {
    let deleted = 0;
    // Scan backward so several deletions can create holes without changing the
    // indexes of entries that have not been inspected yet.
    for (let i = this.#entries.length - 1; i >= this.#head; i--) {
      const value = this.#entries[i];
      if (value !== EMPTY_SLOT && predicate(value)) {
        this.#entries[i] = EMPTY_SLOT;
        this.#count--;
        deleted++;
      }
    }
    this.#maybeCompact();
    return deleted;
  }

  toArray(): T[] {
    const values: T[] = [];
    for (let i = this.#head; i < this.#entries.length; i++) {
      const value = this.#entries[i];
      if (value !== EMPTY_SLOT) {
        values.push(value);
      }
    }
    return values;
  }

  clear(): void {
    this.#entries.length = 0;
    this.#head = 0;
    this.#count = 0;
  }

  #maybeCompact(): void {
    if (this.#count === 0) {
      // Empty queues should release the whole backing array immediately; this is
      // the common state after a stream drains.
      this.clear();
    } else if (this.#head > 1024 && this.#head * 2 > this.#entries.length) {
      // Compaction copies the live suffix, so wait until the dead prefix is both
      // large and at least half the array.
      this.#entries.splice(0, this.#head);
      this.#head = 0;
    }
  }
}

const EMPTY_SLOT = Symbol('empty-slot');
type EmptySlot = typeof EMPTY_SLOT;
