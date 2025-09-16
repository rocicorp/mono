/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import {RWLock} from '@rocicorp/lock';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import type {FrozenJSONValue} from '../frozen-json.ts';
import {ReadImpl} from './read-impl.ts';
import type {Read, Store, Write} from './store.ts';
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
    const release = await this.#rwLock.read();
    return new ReadImpl(this.#map, release);
  }

  async write(): Promise<Write> {
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
