import {vi} from 'vitest';

/**
 * Controls over the stubbed `WeakRef` installed by
 * {@linkcode withStubbedWeakRef}.
 */
export type WeakRefControl = {
  /**
   * Marks `target` collected: every `WeakRef` to it derefs to `undefined` from
   * now on. Stands in for the garbage collector, which cannot be driven
   * portably from a test.
   */
  collect(target: object): void;

  /**
   * How many `WeakRef`s have been constructed. A structure that holds
   * something strongly constructs none, which is how a test tells the two
   * apart without reading its fields.
   */
  created(): number;

  /**
   * How many times `deref` has been called. A lookup that stops at its match
   * derefs fewer entries than one that walks a whole bucket.
   */
  derefs(): number;

  /** Resets {@linkcode derefs} to zero. */
  resetDerefs(): void;
};

/**
 * Runs `fn` with `WeakRef` replaced by a stub whose targets are collected on
 * demand and whose use is counted.
 *
 * Retention is the property these structures are built around, and asserting
 * it otherwise means either exposing their internals or waiting on a real GC.
 * Counting construction and deref calls gets at the same facts from outside:
 * what is held weakly, and how much of a bucket a lookup walked.
 */
export function withStubbedWeakRef(
  fn: (control: WeakRefControl) => void,
): void {
  const dead = new WeakSet<object>();
  let created = 0;
  let derefs = 0;

  class StubWeakRef<T extends object> {
    readonly #target: T;

    constructor(target: T) {
      this.#target = target;
      created++;
    }

    deref(): T | undefined {
      derefs++;
      return dead.has(this.#target) ? undefined : this.#target;
    }
  }

  vi.stubGlobal('WeakRef', StubWeakRef);
  try {
    fn({
      collect: target => void dead.add(target),
      created: () => created,
      derefs: () => derefs,
      resetDerefs: () => void (derefs = 0),
    });
  } finally {
    vi.unstubAllGlobals();
  }
}
