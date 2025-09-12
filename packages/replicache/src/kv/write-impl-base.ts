import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {
  promiseFalse,
  promiseTrue,
  promiseVoid,
} from '../../../shared/src/resolved-promises.ts';
import {
  type FrozenJSONValue,
  deepFreeze,
  deepFreezeAllowUndefined,
} from '../frozen-json.ts';
import type {Read} from './store.ts';
import {
  maybeTransactionIsClosedRejection,
  transactionIsClosedRejection,
} from './throw-if-closed.ts';

export const deleteSentinel = Symbol();
type DeleteSentinel = typeof deleteSentinel;

export class WriteImplBase {
  protected readonly _pending: Map<string, FrozenJSONValue | DeleteSentinel> =
    new Map();
  readonly #read: Read;

  constructor(read: Read) {
    this.#read = read;
  }

  has(key: string): Promise<boolean> {
    if (this.#read.closed) {
      return transactionIsClosedRejection();
    }
    switch (this._pending.get(key)) {
      case undefined:
        return this.#read.has(key);
      case deleteSentinel:
        return promiseFalse;
      default:
        return promiseTrue;
    }
  }

  async get(key: string): Promise<FrozenJSONValue | undefined> {
    if (this.#read.closed) {
      return transactionIsClosedRejection();
    }
    const v = this._pending.get(key);
    switch (v) {
      case deleteSentinel:
        return undefined;
      case undefined: {
        const v = await this.#read.get(key);
        return deepFreezeAllowUndefined(v);
      }
      default:
        return v;
    }
  }

  put(key: string, value: ReadonlyJSONValue) {
    return (
      maybeTransactionIsClosedRejection(this.#read) ??
      (this._pending.set(key, deepFreeze(value)), promiseVoid)
    );
  }

  del(key: string): Promise<void> {
    return (
      maybeTransactionIsClosedRejection(this.#read) ??
      (this._pending.set(key, deleteSentinel), promiseVoid)
    );
  }

  release(): void {
    this.#read.release();
  }

  get closed(): boolean {
    return this.#read.closed;
  }
}
