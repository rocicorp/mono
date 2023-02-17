import type {FrozenJSONValue} from '../json.js';
import {promiseVoid} from '../resolved-promises.js';
import {ReadImpl} from './read-impl.js';
import type {Write} from './store.js';
import {deleteSentinel, WriteImplBase} from './write-impl-base.js';

export class WriteImpl extends WriteImplBase implements Write {
  private readonly _map: Map<string, FrozenJSONValue>;

  constructor(map: Map<string, FrozenJSONValue>, release: () => void) {
    super(new ReadImpl(map, release));
    this._map = map;
  }

  commit(): Promise<void> {
    // HOT. Do not allocate entry tuple and destructure.
    this._pending.forEach((value, key) => {
      if (value === deleteSentinel) {
        this._map.delete(key);
      } else {
        this._map.set(key, value);
      }
    });
    this._pending.clear();
    this.release();
    return promiseVoid;
  }
}
