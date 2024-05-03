import {RWLock} from '@rocicorp/lock';
import {promiseVoid} from 'shared/src/resolved-promises.js';
import type {FrozenJSONValue} from '../frozen-json.js';
import {stringCompare} from '../string-compare.js';
import {ReadImpl} from './read-impl.js';
import type {Read, Store, Write} from './store.js';
import {WriteImpl} from './write-impl.js';

export class TestMemStore implements Store {
  readonly #map: Map<string, FrozenJSONValue> = new Map();
  readonly #rwLock = new RWLock();
  #closed = false;

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

  snapshot(): Record<string, FrozenJSONValue> {
    const entries = [...this.#map.entries()];
    entries.sort((a, b) => stringCompare(a[0], b[0]));
    return Object.fromEntries(entries);
  }

  restoreSnapshot(snapshot: Record<string, FrozenJSONValue>): void {
    this.#map.clear();

    for (const [k, v] of Object.entries(snapshot)) {
      this.#map.set(k, v);
    }
  }

  /**
   * This exposes the underlying map for testing purposes.
   */
  entries(): IterableIterator<[string, FrozenJSONValue]> {
    return this.#map.entries();
  }

  map(): Map<string, FrozenJSONValue> {
    return this.#map;
  }

  clear(): void {
    this.#map.clear();
  }
}
