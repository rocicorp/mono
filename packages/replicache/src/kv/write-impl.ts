import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import type {FrozenJSONValue} from '../frozen-json.ts';
import {ReadImpl} from './read-impl.ts';
import type {Write} from './store.ts';
import {transactionIsClosedRejection} from './throw-if-closed.ts';
import {deleteSentinel, WriteImplBase} from './write-impl-base.ts';

export class WriteImpl extends WriteImplBase implements Write {
  readonly #map: Map<string, FrozenJSONValue>;

  constructor(map: Map<string, FrozenJSONValue>, release: () => void) {
    super(new ReadImpl(map, release));
    this.#map = map;
  }

  commit(): Promise<void> {
    if (this.closed) {
      return transactionIsClosedRejection();
    }

    // HOT. Do not allocate entry tuple and destructure.
    this._pending.forEach((value, key) => {
      if (value === deleteSentinel) {
        this.#map.delete(key);
      } else {
        this.#map.set(key, value);
      }
    });
    this._pending.clear();
    this.release();
    return promiseVoid;
  }
}
