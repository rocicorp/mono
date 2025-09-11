import {resolver} from '@rocicorp/resolver';
import {assertNotNull} from '../../../shared/src/asserts.ts';
import {mustGetBrowserGlobal} from '../../../shared/src/browser-env.ts';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import {
  type FrozenJSONValue,
  deepFreezeAllowUndefined,
} from '../frozen-json.ts';
import type {Read, Store, Write} from './store.ts';
import {
  storeIsClosedRejection,
  transactionIsClosedRejection,
} from './throw-if-closed.ts';
import {WriteImplBase, deleteSentinel} from './write-impl-base.ts';

const RELAXED = {durability: 'relaxed'} as const;
const OBJECT_STORE = 'chunks';

export class IDBStore implements Store {
  #db: Promise<IDBDatabase>;
  #closed = false;
  #idbDeleted = false;

  constructor(name: string) {
    this.#db = openDatabase(name);
  }

  read(): Promise<Read> {
    if (this.#closed) {
      return storeIsClosedRejection();
    }
    return this.#withReopen(readImpl);
  }

  write(): Promise<Write> {
    if (this.#closed) {
      return storeIsClosedRejection();
    }
    return this.#withReopen(writeImpl);
  }

  async close(): Promise<void> {
    if (!this.#idbDeleted) {
      const db = await this.#db;
      db.close();
    }
    this.#closed = true;
  }

  get closed(): boolean {
    return this.#closed;
  }

  async #withReopen<R>(fn: (db: IDBDatabase) => R): Promise<R> {
    // Tries to reopen an IndexedDB, and rejects if the database needs
    // upgrading (is missing for whatever reason).
    const reopenExistingDB = async (name: string): Promise<IDBDatabase> => {
      const {promise, resolve, reject} = resolver<IDBDatabase>();
      const req = indexedDB.open(name);

      req.onupgradeneeded = () => {
        const tx = req.transaction;
        assertNotNull(tx);
        tx.abort();
        this.#idbDeleted = true;
        reject(
          new IDBNotFoundError(
            `Expected IndexedDB not found: ${name}. This likely means that the user deleted IndexedDB instances while the app was running. This is non-fatal. The app will continue running in memory until reload.`,
          ),
        );
      };

      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);

      const db = await promise;
      db.onversionchange = () => db.close();
      return db;
    };

    // We abstract on `readImpl` to work around an issue in Safari. Safari does
    // not allow any microtask between a transaction is created until it is
    // first used. We used to use `await read()` here instead of `await
    // this._db` but then there is a microtask between the creation of the
    // transaction and the return of this function. By doing `await this._db`
    // here we only await the db and no await is involved with the transaction.
    // See https://github.com/jakearchibald/idb-keyval/commit/1af0a00b1a70a678d2f9cf5e74c55a22e57324c5#r55989916
    const db = await this.#db;

    try {
      return fn(db);
    } catch (e: unknown) {
      if (!this.#closed && e instanceof DOMException) {
        if (e.name === 'InvalidStateError') {
          this.#db = reopenExistingDB(db.name);
          const reopened = await this.#db;
          return fn(reopened);
        } else if (e.name === 'NotFoundError') {
          // This edge-case can happen if the db has been deleted and the
          // user/developer has DevTools open in certain browsers.
          // See discussion at https://github.com/rocicorp/replicache-internal/pull/216
          this.#idbDeleted = true;
          mustGetBrowserGlobal('indexedDB').deleteDatabase(db.name);
          throw new IDBNotFoundError(
            `Expected IndexedDB ${db.name} missing object store. Deleting db. This is non-fatal, the app will continue working in memory until it is reloaded.`,
          );
        }
      }
      throw e;
    }
  }
}

class ReadImpl implements Read {
  readonly #tx: IDBTransaction;
  #closed = false;

  constructor(tx: IDBTransaction) {
    this.#tx = tx;
  }

  has(key: string): Promise<boolean> {
    if (this.#closed) {
      return transactionIsClosedRejection();
    }
    return new Promise((resolve, reject) => {
      const req = objectStore(this.#tx).count(key);
      req.onsuccess = () => resolve(req.result > 0);
      req.onerror = () => reject(req.error);
    });
  }

  get(key: string): Promise<FrozenJSONValue | undefined> {
    if (this.#closed) {
      return transactionIsClosedRejection();
    }
    return new Promise((resolve, reject) => {
      const req = objectStore(this.#tx).get(key);
      req.onsuccess = () => resolve(deepFreezeAllowUndefined(req.result));
      req.onerror = () => reject(req.error);
    });
  }

  release(): void {
    this.#closed = true;
    // Do nothing. We rely on IDB locking.
  }

  get closed(): boolean {
    return this.#closed;
  }
}

class WriteImpl extends WriteImplBase {
  readonly #tx: IDBTransaction;
  #closed = false;

  constructor(tx: IDBTransaction) {
    super(new ReadImpl(tx));
    this.#tx = tx;
  }

  commit(): Promise<void> {
    if (this.#closed) {
      return transactionIsClosedRejection();
    }
    if (this._pending.size === 0) {
      return promiseVoid;
    }

    return new Promise((resolve, reject) => {
      const tx = this.#tx;
      const store = objectStore(tx);
      for (const [key, val] of this._pending) {
        if (val === deleteSentinel) {
          store.delete(key);
        } else {
          store.put(val, key);
        }
      }
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  release(): void {
    // We rely on IDB locking so no need to do anything here.
    this.#closed = true;
    super.release();
  }

  get closed(): boolean {
    return this.#closed;
  }
}

function writeImpl(db: IDBDatabase): Write {
  const tx = db.transaction(OBJECT_STORE, 'readwrite', RELAXED);
  return new WriteImpl(tx);
}

function readImpl(db: IDBDatabase): Read {
  const tx = db.transaction(OBJECT_STORE, 'readonly');
  return new ReadImpl(tx);
}

function objectStore(tx: IDBTransaction): IDBObjectStore {
  return tx.objectStore(OBJECT_STORE);
}

function openDatabase(name: string): Promise<IDBDatabase> {
  const idb = mustGetBrowserGlobal('indexedDB');
  return new Promise((resolve, reject) => {
    const req = idb.open(name);
    req.onupgradeneeded = () => {
      req.result.createObjectStore(OBJECT_STORE);
    };
    req.onsuccess = () => {
      const db = req.result;
      // Another tab/process wants to modify the db, so release it.
      db.onversionchange = () => db.close();
      resolve(db);
    };
    req.onerror = () => reject(req.error);
  });
}

/**
 * This error is thrown when we detect that the IndexedDB has been removed. This
 * does not normally happen but can happen during development if the user has
 * DevTools open and deletes the IndexedDB from there.
 */
export class IDBNotFoundError extends Error {
  name = 'IDBNotFoundError';
}
