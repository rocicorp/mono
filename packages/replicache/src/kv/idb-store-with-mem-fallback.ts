import type {LogContext} from '@rocicorp/logger';
import {navigator} from '../../../shared/src/navigator.ts';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import {IDBStore} from './idb-store.ts';
import {MemStore, dropMemStore} from './mem-store.ts';
import type {Read, Store, Write} from './store.ts';

/**
 * This store uses an {@link IDBStore} by default. If the {@link IDBStore} fails
 * to open the DB with an exception that matches
 * {@link isFirefoxPrivateBrowsingError} we switch out the implementation to use
 * a {@link MemStore} instead.
 *
 * The reason this is relatively complicated is that when {@link IDBStore} is
 * created, it calls `openDatabase` synchronously, but that returns a `Promise`
 * that will reject in the case of Firefox private browsing. We don't await this
 * promise until we call `read` or `write` so we cannot do the switch until
 * then.
 */

export class IDBStoreWithMemFallback implements Store {
  readonly #lc: LogContext;
  readonly #name: string;
  #store: Store;
  constructor(lc: LogContext, name: string) {
    this.#lc = lc;
    this.#name = name;
    this.#store = new IDBStore(name);
  }

  read(): Promise<Read> {
    return this.#withBrainTransplant(s => s.read());
  }

  write(): Promise<Write> {
    return this.#withBrainTransplant(s => s.write());
  }

  async #withBrainTransplant<T extends Read>(
    f: (store: Store) => Promise<T>,
  ): Promise<T> {
    try {
      return await f(this.#store);
    } catch (e) {
      if (isFirefoxPrivateBrowsingError(e)) {
        // It is possible that we end up with multiple pending read/write and
        // they all reject. Make sure we only replace the implementation once.
        if (this.#store instanceof IDBStore) {
          this.#lc.info?.(
            'Switching to MemStore because of Firefox private browsing error',
          );
          this.#store = new MemStore(this.#name);
        }
        return f(this.#store);
      }
      throw e;
    }
  }

  close(): Promise<void> {
    return this.#store.close();
  }

  get closed(): boolean {
    return this.#store.closed;
  }
}

function isFirefoxPrivateBrowsingError(e: unknown): e is DOMException {
  return (
    isFirefox() &&
    e instanceof DOMException &&
    e.name === 'InvalidStateError' &&
    e.message ===
      'A mutation operation was attempted on a database that did not allow mutations.'
  );
}

function isFirefox(): boolean {
  return navigator?.userAgent?.includes('Firefox') ?? false;
}

export function newIDBStoreWithMemFallback(
  lc: LogContext,
  name: string,
): Store {
  if (isFirefox()) {
    return new IDBStoreWithMemFallback(lc, name);
  }
  return new IDBStore(name);
}

export function dropIDBStoreWithMemFallback(name: string): Promise<void> {
  if (!isFirefox()) {
    return dropIDBStore(name);
  }
  try {
    return dropIDBStore(name);
  } catch (e) {
    if (isFirefoxPrivateBrowsingError(e)) {
      return dropMemStore(name);
    }
  }
  return promiseVoid;
}

function dropIDBStore(name: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const req = indexedDB.deleteDatabase(name);
    req.onsuccess = () => resolve();
    req.onerror = () => reject(req.error);
  });
}
