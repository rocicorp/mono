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

  put(key: string, value: ReadonlyJSONValue): Promise<void> {
    this._pending.set(key, deepFreeze(value));
    return promiseVoid;
  }

  del(key: string): Promise<void> {
    this._pending.set(key, deleteSentinel);
    return promiseVoid;
  }

  release(): void {
    this.#read.release();
  }

  get closed(): boolean {
    return this.#read.closed;
  }
}
