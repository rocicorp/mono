import {FrozenJSONValue, ReadonlyJSONValue, deepFreeze} from '../json.js';
import {promiseFalse, promiseTrue, promiseVoid} from '../resolved-promises.js';
import type {Read} from './store.js';

export const deleteSentinel = Symbol();
type DeleteSentinel = typeof deleteSentinel;

export class WriteImplBase {
  protected readonly _pending: Map<string, FrozenJSONValue | DeleteSentinel> =
    new Map();
  private readonly _read: Read;

  constructor(read: Read) {
    this._read = read;
  }

  has(key: string): Promise<boolean> {
    switch (this._pending.get(key)) {
      case undefined:
        return this._read.has(key);
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
        const v = await this._read.get(key);
        return deepFreeze(v);
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
    this._read.release();
  }

  get closed(): boolean {
    return this._read.closed;
  }
}
