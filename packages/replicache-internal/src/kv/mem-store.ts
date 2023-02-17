import {RWLock} from '@rocicorp/lock';
import type {FrozenJSONValue} from '../json.js';
import {promiseVoid} from '../resolved-promises.js';
import {ReadImpl} from './read-impl.js';
import type {Read, Store, Write} from './store.js';
import {WriteImpl} from './write-impl.js';

type StorageMap = Map<string, FrozenJSONValue>;

type Value = {readonly lock: RWLock; readonly map: StorageMap};

const stores = new Map<string, Value>();

export function clearAllNamedMemStoresForTesting(): void {
  stores.clear();
}

/**
 * A named in-memory Store implementation.
 *
 * Two (or more) named memory stores with the same name will share the same
 * underlying storage. They will also share the same read/write locks, so that
 * only one write transaction can be running at the same time.
 *
 * @experimental This class is experimental and might be removed or changed
 * in the future without following semver versioning. Please be cautious.
 */
export class MemStore implements Store {
  private readonly _map: StorageMap;
  private readonly _rwLock: RWLock;
  private _closed = false;

  constructor(name: string) {
    const entry = stores.get(name);
    let lock: RWLock;
    let map: StorageMap;
    if (entry) {
      ({lock, map} = entry);
    } else {
      lock = new RWLock();
      map = new Map();
      stores.set(name, {lock, map});
    }
    this._rwLock = lock;
    this._map = map;
  }

  async read(): Promise<Read> {
    const release = await this._rwLock.read();
    return new ReadImpl(this._map, release);
  }

  async write(): Promise<Write> {
    const release = await this._rwLock.write();
    return new WriteImpl(this._map, release);
  }

  close(): Promise<void> {
    this._closed = true;
    return promiseVoid;
  }

  get closed(): boolean {
    return this._closed;
  }
}
