import {RWLock} from '@rocicorp/lock';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import type {FrozenJSONValue} from '../frozen-json.ts';
import {ReadImpl} from './read-impl.ts';
import type {Read, Store, Write} from './store.ts';
import {throwIfStoreClosed} from './throw-if-closed.ts';
import {WriteImpl} from './write-impl.ts';

type StorageMap = Map<string, FrozenJSONValue>;

type Value = {readonly lock: RWLock; readonly map: StorageMap};

const stores = new Map<string, Value>();

export function clearAllNamedMemStoresForTesting(): void {
  stores.clear();
}

export function dropMemStore(name: string): Promise<void> {
  stores.delete(name);
  return promiseVoid;
}

export function hasMemStore(name: string): boolean {
  return stores.has(name);
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
  readonly #map: StorageMap;
  readonly #rwLock: RWLock;
  #closed = false;

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
    this.#rwLock = lock;
    this.#map = map;
  }

  async read(): Promise<Read> {
    throwIfStoreClosed(this);
    const release = await this.#rwLock.read();
    return new ReadImpl(this.#map, release);
  }

  async write(): Promise<Write> {
    throwIfStoreClosed(this);
    const release = await this.#rwLock.write();
    return new WriteImpl(this.#map, release);
  }

  close(): Promise<void> {
    this.#closed = true;
    return promiseVoid;
  }

  get closed(): boolean {
    return this.#closed;
  }
}
