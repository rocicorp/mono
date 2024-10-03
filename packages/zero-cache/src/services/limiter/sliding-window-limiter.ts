import {assert} from 'shared/src/asserts.js';

type Window = {
  start: number;
  count: number;
};

/**
 * The limiter is a sliding window rate limiter that allows a maximum
 * number of mutations per window.
 *
 * The window is divided into two parts: the prev window and the next window.
 * The prev window is the part of the window that has already passed, and the
 * next window is the part of the window that is still in the future.
 *
 * The limiter keeps track of the number of mutations in each window. When the
 * current time moves completely past the next window, the limiter creates new
 * windows (this only happens if the limiter is not called for a long time).
 * When the current time moves completely into the next window, the
 * limiter rotates the windows.
 *
 * Case 1: need new windows
 * |----|----|
 *             |----| (sliding window)
 *
 * Case 2: rotate windows
 * |----|----|
 *      |----| (sliding window)
 *
 * or
 *
 * |----|----|
 *         |----| (sliding window)
 *
 * The leading edge of the sliding window is current time.
 *
 * The limiter increments the count for the prev window if the current time is
 * in the prior window. Otherwise, it increments the count for the next
 * window.
 *
 * The limiter computes the total mutations by: taking the number of mutation in the next window and
 * adding the number of mutation from the prev window, weighted by
 * the fraction of the window that has passed. The total mutations must be less
 * than the maximum number of mutations allowed per window.
 *
 * |----|----|
 *    |----| (sliding window)
 * ^-- the sliding window covers 25% of the prior window. Only 25% of that window's count counts.
 * The entirety of the next window's count counts.
 */
export class SlidingWindowLimiter {
  readonly #windowSizeMs: number;
  readonly #maxMutations: number;

  #priorWindow: Window;
  #nextWindow: Window;

  constructor(windowSizeMs: number, maxMutations: number) {
    this.#windowSizeMs = windowSizeMs;
    this.#maxMutations = maxMutations;

    [this.#priorWindow, this.#nextWindow] = this.#newWindows();
  }

  canDo(): boolean {
    const now = Date.now();

    // If the current sliding window is completely past the next window, we need new windows.
    if (
      now - this.#windowSizeMs >
      this.#nextWindow.start + this.#windowSizeMs
    ) {
      [this.#priorWindow, this.#nextWindow] = this.#newWindows();
    }

    // Has the current sliding window moved completely into the next window?
    // Then rotate the windows.
    if (now - this.#windowSizeMs >= this.#nextWindow.start) {
      this.#rotateWindows();
    }

    // If "now" in the prior or next window? Increment the count for the correct window.
    if (now < this.#nextWindow.start) {
      this.#priorWindow.count++;
    } else {
      this.#nextWindow.count++;
    }

    // Now compute the total mutations in the current window.
    // Weighted by the fraction of the window that has passed.
    const totalCalls = this.totalCallsForTime(now);
    return totalCalls <= this.#maxMutations;
  }

  totalCallsForTime(now: number): number {
    let fraction: number;
    if (now < this.#priorWindow.start + this.#windowSizeMs) {
      fraction = 1;
    } else {
      fraction =
        (this.#priorWindow.start +
          (this.#windowSizeMs - 1) -
          (now - this.#windowSizeMs)) /
        this.#windowSizeMs;
    }
    if (fraction < 0) {
      fraction = 0;
    }
    assert(
      fraction <= 1,
      'The past cannot contribute more than the a full window.',
    );
    const totalCalls =
      this.#priorWindow.count * fraction + this.#nextWindow.count;
    return totalCalls;
  }

  get priorWindow(): Window {
    return this.#priorWindow;
  }

  get nextWindow(): Window {
    return this.#nextWindow;
  }

  /**
   * If there were no mutations in over `windowSizeMs` we need new windows.
   */
  #newWindows() {
    const now = Date.now();
    const start = now - (now % this.#windowSizeMs);
    return [
      {
        // clamp to a `windowSizeMs` boundary to start the prior window.
        start,
        count: 0,
      },
      {
        start: start + this.#windowSizeMs,
        count: 0,
      },
    ];
  }

  #rotateWindows() {
    this.#priorWindow = this.#nextWindow;
    this.#nextWindow = {
      start: this.#priorWindow.start + this.#windowSizeMs,
      count: 0,
    };
  }
}
