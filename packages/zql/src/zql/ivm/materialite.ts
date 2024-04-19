// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore next.js is having issues finding the .d.ts
import type {Comparator} from '@vlcn.io/ds-and-algos/types';
import {must} from 'shared/src/must.js';
import {MutableSetSource} from './source/set-source.js';
import type {SourceInternal} from './source/source.js';
import type {Version} from './types.js';

export type MaterialiteForSourceInternal = {
  readonly materialite: Materialite;
  nextVersion(): number;
  getVersion(): number;
  addDirtySource(source: SourceInternal): void;
};

export class Materialite {
  #version: Version;
  #dirtySources: Set<SourceInternal> = new Set();

  #currentTx: Version | null = null;
  #internal: MaterialiteForSourceInternal;

  constructor() {
    this.#version = 0;
    this.#internal = {
      materialite: this,
      getVersion: () => this.#version,
      nextVersion: () => {
        this.#version += 1;
        return this.#version;
      },
      addDirtySource: (source: SourceInternal) => {
        this.#dirtySources.add(source);
        // auto-commit if not in a transaction
        if (this.#currentTx === null) {
          this.#currentTx = this.#version + 1;
          this.#commit();
        }
      },
    };
  }

  newSetSource<T extends object>(comparator: Comparator<T>) {
    return new MutableSetSource<T>(this.#internal, comparator);
  }

  /**
   * Run the provided lambda in a transaction.
   * Will be committed when the lambda exits
   * and all incremental computations that depend on modified inputs
   * will be run.
   *
   * An exception to this is in the case of nested transactions.
   * No incremental computation will run until the outermost transaction
   * completes.
   *
   * If the transaction throws, all pending inputs which were queued will be rolled back.
   * If a nested transaction throws, all transactions in the stack are rolled back.
   *
   * In this way, nesting transactions only exists to allow functions to be ignorant
   * of what transactions other functions that they call may create. It would be problematic
   * if creating transactions within transactions failed as it would preclude the use of
   * libraries that use transactions internally.
   */
  tx(fn: () => void) {
    if (this.#currentTx === null) {
      this.#currentTx = this.#version + 1;
    } else {
      // nested transaction
      // just run the function as we're already inside the
      // scope of a transaction that will handle rollback and commit.
      fn();
      return;
    }
    let failed = false;

    try {
      // ideally we do not need to queue a micro task
      // in order to place all experimental watch calls into the same transaction.
      // arv is working on this.
      queueMicrotask(() => {
        if (!failed) {
          this.#commit();
        } else {
          this.#rollback();
        }
        this.#dirtySources.clear();
      });
      fn();
    } catch (e) {
      failed = true;
      throw e;
    }
  }

  #rollback() {
    this.#currentTx = null;
    for (const source of this.#dirtySources) {
      source.onRollback();
    }
  }

  #commit() {
    this.#version = must(this.#currentTx);
    this.#currentTx = null;
    for (const source of this.#dirtySources) {
      source.onCommitEnqueue(this.#version);
    }
    for (const source of this.#dirtySources) {
      source.onCommitted(this.#version);
    }
  }
}
