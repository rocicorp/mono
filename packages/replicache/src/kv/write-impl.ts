import type {FrozenJSONValue} from '../frozen-json.js';
import {promiseVoid} from '../resolved-promises.js';
import {ReadImpl} from './read-impl.js';
import type {Write} from './store.js';
import {deleteSentinel, WriteImplBase} from './write-impl-base.js';

export class WriteImpl extends WriteImplBase implements Write {
  readonly #map: Map<string, FrozenJSONValue>;

  constructor(map: Map<string, FrozenJSONValue>, release: () => void) {
    super(new ReadImpl(map, release));
    this.#map = map;
  }

  commit(): Promise<void> {
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
