/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import {RWLock} from '@rocicorp/lock';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import {stringCompare} from '../../../shared/src/string-compare.ts';
import type {FrozenJSONValue} from '../frozen-json.ts';
import {ReadImpl} from './read-impl.ts';
import type {Read, Store, Write} from './store.ts';
import {WriteImpl} from './write-impl.ts';

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
