import {RWLock} from '@rocicorp/lock';
import type {FrozenJSONValue} from '../json.js';
import {promiseVoid} from '../resolved-promises.js';
import {stringCompare} from '../string-compare.js';
import {WriteImpl} from './write-impl.js';
import {ReadImpl} from './read-impl.js';
import type {Read, Store, Write} from './store.js';

export class TestMemStore implements Store {
  private readonly _map: Map<string, FrozenJSONValue> = new Map();
  private readonly _rwLock = new RWLock();
  private _closed = false;

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

  snapshot(): Record<string, FrozenJSONValue> {
    const entries = [...this._map.entries()];
    entries.sort((a, b) => stringCompare(a[0], b[0]));
    return Object.fromEntries(entries);
  }

  restoreSnapshot(snapshot: Record<string, FrozenJSONValue>): void {
    this._map.clear();

    for (const [k, v] of Object.entries(snapshot)) {
      this._map.set(k, v);
    }
  }

  /**
   * This exposes the underlying map for testing purposes.
   */
  entries(): IterableIterator<[string, FrozenJSONValue]> {
    return this._map.entries();
  }

  map(): Map<string, FrozenJSONValue> {
    return this._map;
  }

  clear(): void {
    this._map.clear();
  }
}
