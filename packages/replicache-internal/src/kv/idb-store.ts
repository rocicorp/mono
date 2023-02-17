import {deleteSentinel, WriteImplBase} from './write-impl-base.js';
import type {Read, Store, Write} from './store.js';
import {resolver} from '@rocicorp/resolver';
import {assertNotNull} from '../asserts.js';
import {wrap} from './idb-util.js';
import {FrozenJSONValue, deepFreeze} from '../json.js';

const RELAXED = {durability: 'relaxed'};
const OBJECT_STORE = 'chunks';

const enum WriteState {
  OPEN,
  COMMITTED,
  ABORTED,
}

export class IDBStore implements Store {
  private _db: Promise<IDBDatabase>;
  private _closed = false;
  private _idbDeleted = false;

  constructor(name: string) {
    this._db = openDatabase(name);
  }

  async read(): Promise<Read> {
    return await this._withReopen(readImpl);
  }

  async write(): Promise<Write> {
    return await this._withReopen(writeImpl);
  }

  async close(): Promise<void> {
    if (!this._idbDeleted) {
      const db = await this._db;
      db.close();
    }
    this._closed = true;
  }

  get closed(): boolean {
    return this._closed;
  }

  private async _withReopen<R>(fn: (db: IDBDatabase) => R): Promise<R> {
    // Tries to reopen an IndexedDB, and rejects if the database needs
    // upgrading (is missing for whatever reason).
    const reopenExistingDB = async (name: string): Promise<IDBDatabase> => {
      const {promise, resolve, reject} = resolver<IDBDatabase>();
      const req = indexedDB.open(name);

      req.onupgradeneeded = () => {
        const tx = req.transaction;
        assertNotNull(tx);
        tx.abort();
        this._idbDeleted = true;
        reject(new IDBNotFoundError(`Replicache IndexedDB not found: ${name}`));
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
    const db = await this._db;

    try {
      return fn(db);
    } catch (e: unknown) {
      if (!this._closed && e instanceof DOMException) {
        if (e.name === 'InvalidStateError') {
          this._db = reopenExistingDB(db.name);
          const reopened = await this._db;
          return fn(reopened);
        } else if (e.name === 'NotFoundError') {
          // This edge-case can happen if the db has been deleted and the
          // user/developer has DevTools open in certain browsers.
          // See discussion at https://github.com/rocicorp/replicache-internal/pull/216
          this._idbDeleted = true;
          indexedDB.deleteDatabase(db.name);
          throw new IDBNotFoundError(
            `Replicache IndexedDB ${db.name} missing object store. Deleting db.`,
          );
        }
      }
      throw e;
    }
  }
}

class ReadImpl implements Read {
  private readonly _tx: IDBTransaction;
  private _closed = false;

  constructor(tx: IDBTransaction) {
    this._tx = tx;
  }

  async has(key: string): Promise<boolean> {
    return (await wrap(objectStore(this._tx).count(key))) > 0;
  }

  async get(key: string): Promise<FrozenJSONValue | undefined> {
    const v = await wrap(objectStore(this._tx).get(key));
    return deepFreeze(v);
  }

  release(): void {
    this._closed = true;
    // Do nothing. We rely on IDB locking.
  }

  get closed(): boolean {
    return this._closed;
  }
}

class WriteImpl extends WriteImplBase {
  private readonly _tx: IDBTransaction;
  private readonly _onTxEnd: Promise<void>;
  private _txState = WriteState.OPEN;
  private _closed = false;

  constructor(tx: IDBTransaction) {
    super(new ReadImpl(tx));
    this._tx = tx;
    this._onTxEnd = this._addTransactionListeners();
  }

  private async _addTransactionListeners(): Promise<void> {
    const tx = this._tx;
    const p: Promise<WriteState> = new Promise((resolve, reject) => {
      tx.onabort = () => resolve(WriteState.ABORTED);
      tx.oncomplete = () => resolve(WriteState.COMMITTED);
      tx.onerror = () => reject(tx.error);
    });

    // When the transaction completes/aborts, set the state.
    this._txState = await p;
  }

  async commit(): Promise<void> {
    if (this._pending.size === 0) {
      return;
    }

    const store = objectStore(this._tx);
    for (const [key, val] of this._pending) {
      if (val === deleteSentinel) {
        store.delete(key);
      } else {
        store.put(val, key);
      }
    }
    await this._onTxEnd;

    if (this._txState === WriteState.ABORTED) {
      throw new Error('Transaction aborted');
    }
  }

  release(): void {
    // We rely on IDB locking so no need to do anything here.
    this._closed = true;
  }

  get closed(): boolean {
    return this._closed;
  }
}

function writeImpl(db: IDBDatabase) {
  // TS does not have type defs for the third options argument yet.
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore Expected 1-2 arguments, but got 3.ts(2554)
  const tx = db.transaction(OBJECT_STORE, 'readwrite', RELAXED);
  return new WriteImpl(tx);
}

function readImpl(db: IDBDatabase) {
  const tx = db.transaction(OBJECT_STORE, 'readonly');
  return new ReadImpl(tx);
}

function objectStore(tx: IDBTransaction): IDBObjectStore {
  return tx.objectStore(OBJECT_STORE);
}

async function openDatabase(name: string): Promise<IDBDatabase> {
  const req = indexedDB.open(name);
  req.onupgradeneeded = () => {
    req.result.createObjectStore(OBJECT_STORE);
  };
  const db = await wrap(req);
  // Another tab/process wants to modify the db, so release it.
  db.onversionchange = () => db.close();
  return db;
}

/**
 * This {@link Error} is thrown when we detect that the IndexedDB has been
 * removed. This does not normally happen but can happen during development if
 * the user has DevTools open and deletes the IndexedDB from there.
 */
export class IDBNotFoundError extends Error {
  name = 'IDBNotFoundError';
}
