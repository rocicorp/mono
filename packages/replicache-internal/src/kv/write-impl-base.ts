import {
  promiseFalse,
  promiseTrue,
  promiseUndefined,
  promiseVoid,
} from '../resolved-promises.js';
import type {Read, Value} from './store';

export const deleteSentinel = Symbol();
export type DeleteSentinel = typeof deleteSentinel;

export class WriteImplBase {
  protected readonly _pending: Map<string, Value | DeleteSentinel> = new Map();
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

  get(key: string): Promise<Value | undefined> {
    const v = this._pending.get(key);
    switch (v) {
      case deleteSentinel:
        return promiseUndefined;
      case undefined:
        return this._read.get(key);
      default:
        return Promise.resolve(v);
    }
  }

  put(key: string, value: Value): Promise<void> {
    this._pending.set(key, value);
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
