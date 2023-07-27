import {assert} from './asserts.js';
import {resolver} from '@rocicorp/resolver';

/**
 * Primitive for synchronizing across concurrent logic.
 */
export class CountDownLatch {
  readonly #promise: Promise<void>;
  readonly #resolve: (value: void) => void;
  #value: number;

  constructor(initialValue = 1) {
    assert(initialValue > 0);
    const {promise, resolve} = resolver<void>();
    this.#promise = promise;
    this.#resolve = resolve;
    this.#value = initialValue;
  }

  /**
   * Returns a Promise that resolves when the value reaches zero.
   */
  zero(): Promise<void> {
    return this.#promise;
  }

  /** Returns the current value of the latch. */
  value(): number {
    return this.#value;
  }

  /** Reduces the value unless it is already zero. */
  countDown() {
    if (this.#value === 0) {
      return;
    }
    this.#value--;
    if (this.#value === 0) {
      this.#resolve();
    }
  }
}
