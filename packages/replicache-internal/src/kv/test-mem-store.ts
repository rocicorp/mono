import {RWLock} from '@rocicorp/lock';
import type {FrozenJSONValue} from '../json.js';
import {promiseVoid} from '../resolved-promises.js';
import {stringCompare} from '../string-compare.js';
import type {Read, Store, Write} from './store.js';
import {deleteSentinel, WriteImplBase} from './write-impl-base.js';

export class TestMemStore implements Store {
  private readonly _map: Map<string, FrozenJSONValue> = new Map();
  private readonly _rwLock = new RWLock();
  private _closed = false;

  async read(): Promise<Read> {
    const release = await this._rwLock.read();
    return new ReadImpl(this._map, release);
  }

  async withRead<R>(fn: (read: Read) => R | Promise<R>): Promise<R> {
    const read = await this.read();
    try {
      return await fn(read);
    } finally {
      read.release();
    }
  }

  async write(): Promise<Write> {
    const release = await this._rwLock.write();
    return new WriteImpl(this._map, release);
  }

  async withWrite<R>(fn: (write: Write) => R | Promise<R>): Promise<R> {
    const write = await this.write();
    try {
      return await fn(write);
    } finally {
      write.release();
    }
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

class ReadImpl implements Read {
  private readonly _map: Map<string, FrozenJSONValue>;
  private readonly _release: () => void;
  private _closed = false;

  constructor(map: Map<string, FrozenJSONValue>, release: () => void) {
    this._map = map;
    this._release = release;
  }

  release() {
    this._release();
    this._closed = true;
  }

  get closed(): boolean {
    return this._closed;
  }

  has(key: string): Promise<boolean> {
    return Promise.resolve(this._map.has(key));
  }

  get(key: string): Promise<FrozenJSONValue | undefined> {
    return Promise.resolve(this._map.get(key));
  }
}

class WriteImpl extends WriteImplBase implements Write {
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
