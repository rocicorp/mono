import {assert} from './asserts.js';
import {must} from './must.js';

/**
 * Primitive for synchronizing across concurrent logic.
 */
export class CountDownLatch {
  readonly #promise: Promise<void>;
  readonly #resolve: (value: void | PromiseLike<void>) => void;
  #value: number;

  constructor(initialValue = 1) {
    assert(initialValue > 0);
    let capturedResolve:
      | ((value: void | PromiseLike<void>) => void)
      | undefined;
    this.#promise = new Promise(resolve => {
      capturedResolve = resolve;
    });
    this.#resolve = must(capturedResolve);
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

/** A one-time signal for coordinating a single event across concurrent logic. */
export class Signal {
  readonly #latch = new CountDownLatch(1);

  notification(): Promise<void> {
    return this.#latch.zero();
  }

  notify() {
    this.#latch.countDown();
  }
}
