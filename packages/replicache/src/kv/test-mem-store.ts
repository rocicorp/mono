import {RWLock} from '@rocicorp/lock';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import {stringCompare} from '../../../shared/src/string-compare.ts';
import type {FrozenJSONValue} from '../frozen-json.ts';
import {ReadImpl} from './read-impl.ts';
import type {Read, Store, Write} from './store.ts';
import {throwIfStoreClosed} from './throw-if-closed.ts';
import {WriteImpl} from './write-impl.ts';

export class TestMemStore implements Store {
  readonly #map: Map<string, FrozenJSONValue> = new Map();
  readonly #rwLock = new RWLock();
  #closed = false;

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
