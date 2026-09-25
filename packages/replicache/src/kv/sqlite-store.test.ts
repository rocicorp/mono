import {afterEach, describe, expect, test, vi} from 'vitest';
import {getOrInsertComputed} from '../../../shared/src/map.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import {StorageFailureError} from '../storage-failure.ts';
import {withWrite} from '../with-transactions.ts';
import {
  SQLiteStore,
  SQLiteWrite,
  SQLiteStoreRead,
  clearAllNamedStoresForTesting,
  type PreparedStatements,
  type SQLiteDatabase,
} from './sqlite-store.ts';

function makePreparedStatement() {
  return {
    all: vi.fn().mockResolvedValue([]),
    exec: vi.fn().mockResolvedValue(undefined),
  };
}

/** One shared statement per width, so assertions can inspect a stable object. */
function makeBatchStatement() {
  const cache = new Map<number, ReturnType<typeof makePreparedStatement>>();
  return (n: number) =>
    getOrInsertComputed(cache, n, () => makePreparedStatement());
}

function makePreparedStatements(): PreparedStatements {
  return {
    has: makePreparedStatement(),
    get: makePreparedStatement(),
    hasMany: makePreparedStatement(),
    getMany: makePreparedStatement(),
    del: makePreparedStatement(),
    put: makePreparedStatement(),
    putN: makeBatchStatement(),
    delN: makeBatchStatement(),
  };
}

test('SQLiteWrite batches deletes and upserts into one bound statement each', async () => {
  const release = vi.fn();
  const db: SQLiteDatabase = {
    close: vi.fn(),
    destroy: vi.fn(),
    prepare: vi.fn(),
    execSync: vi.fn(),
  };
  const preparedStatements = makePreparedStatements();

  const write = new SQLiteWrite(release, db, preparedStatements);

  await write.del('delete-1');
  await write.put('upsert-1', 'value-1');
  await write.del('delete-2');
  await write.put('upsert-2', {nested: true});
  await write.commit();
  write.release();

  // Both pairs go out as one 2-wide statement each, with the key and the
  // JSON-encoded value bound as real parameters rather than routed through a
  // single JSON document for json_each() to parse back out.
  expect(preparedStatements.putN(2).exec).toHaveBeenCalledWith([
    'upsert-1',
    '"value-1"',
    'upsert-2',
    '{"nested":true}',
  ]);
  expect(preparedStatements.delN(2).exec).toHaveBeenCalledWith([
    'delete-1',
    'delete-2',
  ]);
  expect(db.execSync).toHaveBeenCalledWith('COMMIT');
  expect(release).toHaveBeenCalledTimes(1);
});

test('SQLiteWrite splits a commit wider than MAX_BATCH across statement widths', async () => {
  const release = vi.fn();
  const db: SQLiteDatabase = {
    close: vi.fn(),
    destroy: vi.fn(),
    prepare: vi.fn(),
    execSync: vi.fn(),
  };
  const preparedStatements = makePreparedStatements();
  const write = new SQLiteWrite(release, db, preparedStatements);

  // 129 is deliberately just past MAX_BATCH and not a power of two, so it has
  // to split into a full 128-wide statement plus a 1-wide remainder.
  const n = 129;
  for (let i = 0; i < n; i++) {
    await write.put(`put-${i}`, i);
    await write.del(`del-${i}`);
  }
  await write.commit();
  write.release();

  for (const [width, calls] of [
    [128, 1],
    [1, 1],
  ] as const) {
    expect(preparedStatements.putN(width).exec).toHaveBeenCalledTimes(calls);
    expect(preparedStatements.delN(width).exec).toHaveBeenCalledTimes(calls);
  }

  // Every key reaches SQLite exactly once, in order, with its value alongside.
  const putParams = [128, 1].flatMap(
    w => vi.mocked(preparedStatements.putN(w).exec).mock.calls[0][0],
  );
  expect(putParams).toEqual(
    Array.from({length: n}, (_, i) => [`put-${i}`, String(i)]).flat(),
  );

  const delParams = [128, 1].flatMap(
    w => vi.mocked(preparedStatements.delN(w).exec).mock.calls[0][0],
  );
  expect(delParams).toEqual(Array.from({length: n}, (_, i) => `del-${i}`));

  // The single-shot json_each statements are no longer used at all.
  expect(preparedStatements.put.exec).not.toHaveBeenCalled();
  expect(preparedStatements.del.exec).not.toHaveBeenCalled();
});

test('SQLiteStoreRead rejects pending get and has operations when closed', async () => {
  const release = vi.fn();
  const preparedStatements = makePreparedStatements();

  const read = new SQLiteStoreRead(release, preparedStatements);

  // Schedule multiple get and has operations
  const getPromise1 = read.get('key-1');
  const getPromise2 = read.get('key-2');
  const hasPromise1 = read.has('key-3');
  const hasPromise2 = read.has('key-4');

  // Close the transaction before microtask executes
  read.release();

  // Yield control to allow microtask to run
  await Promise.resolve();

  // All pending promises should be rejected with "Transaction is closed"
  await expect(getPromise1).rejects.toThrow('Transaction is closed');
  await expect(getPromise2).rejects.toThrow('Transaction is closed');
  await expect(hasPromise1).rejects.toThrow('Transaction is closed');
  await expect(hasPromise2).rejects.toThrow('Transaction is closed');

  expect(release).toHaveBeenCalledTimes(1);
  // Database statements should not have been called
  expect(preparedStatements.get.all).not.toHaveBeenCalled();
  expect(preparedStatements.has.all).not.toHaveBeenCalled();
  expect(preparedStatements.getMany.all).not.toHaveBeenCalled();
  expect(preparedStatements.hasMany.all).not.toHaveBeenCalled();
});

describe('storage failures', () => {
  afterEach(() => {
    clearAllNamedStoresForTesting();
  });

  /** A driver whose `execSync` fails on the given statement. */
  function failingDatabase(
    failOn: string | undefined,
    error: Error,
    failExecWith?: Error | undefined,
  ): SQLiteDatabase {
    return {
      close: () => undefined,
      destroy: () => undefined,
      execSync: sql => {
        if (sql === failOn) {
          throw error;
        }
      },
      prepare: () => ({
        all: () => Promise.resolve([]),
        exec: () =>
          failExecWith ? Promise.reject(failExecWith) : Promise.resolve(),
      }),
    };
  }

  test('a database that cannot be opened throws a cannot-open StorageFailureError from the constructor', () => {
    const driverError = new Error(
      '[op-sqlite] SQLite error code: 14, description: unable to open database file',
    );
    let thrown: unknown;
    try {
      new SQLiteStore('cannot-open', () => {
        throw driverError;
      });
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(StorageFailureError);
    expect((thrown as StorageFailureError).kind).toBe('cannot-open');
    expect((thrown as StorageFailureError).cause).toBe(driverError);
  });

  test('a transaction step that fails on the disk rejects with an io-error StorageFailureError', async () => {
    const driverError = new Error('disk I/O error');
    const store = new SQLiteStore('io-error', () =>
      failingDatabase('BEGIN IMMEDIATE', driverError),
    );
    const error = await store.write().catch(e => e);
    expect(error).toBeInstanceOf(StorageFailureError);
    expect(error.kind).toBe('io-error');
    expect(error.cause).toBe(driverError);
  });

  test('a statement that fails on a full disk rejects with a full StorageFailureError', async () => {
    const driverError = new Error(
      'Exception in HostFunction: [op-sqlite] SQLite error code: 13, description: database or disk is full',
    );
    const store = new SQLiteStore('full', () =>
      failingDatabase(undefined, new Error('unused'), driverError),
    );
    const error = await withWrite(store, write => write.put('k', 'v')).catch(
      e => e,
    );
    expect(error).toBeInstanceOf(StorageFailureError);
    expect(error.kind).toBe('full');
    expect(error.cause).toBe(driverError);
  });

  test('a database whose setup fails is closed', () => {
    const close = vi.fn();
    const db: SQLiteDatabase = {
      close,
      destroy: () => undefined,
      execSync: sql => {
        if (sql.startsWith('PRAGMA')) {
          throw new Error('disk I/O error');
        }
      },
      prepare: () => ({
        all: () => Promise.resolve([]),
        exec: () => Promise.resolve(),
      }),
    };
    expect(() => new SQLiteStore('setup-fails', () => db)).toThrow(
      StorageFailureError,
    );
    expect(close).toHaveBeenCalledTimes(1);
  });

  test('other SQLite errors pass through unchanged', async () => {
    const driverError = new Error(
      'SQLite error code: 5, description: database is locked',
    );
    const store = new SQLiteStore('busy', () =>
      failingDatabase('BEGIN IMMEDIATE', driverError),
    );
    await expect(store.write()).rejects.toBe(driverError);
  });
});

// A scripted `SQLiteDatabase` that fails exactly the statements a test asks it
// to, the way SQLite does: an I/O error inside a statement rolls the open
// transaction back on its own, so the store's later COMMIT/ROLLBACK is refused
// with `cannot commit - no transaction is active`.
function makeScriptedDatabase() {
  const failNextExec = new Map<string, Error>();
  let statementFailure: Error | undefined;
  let transactionOpen = false;
  const db: SQLiteDatabase = {
    close: vi.fn(),
    destroy: vi.fn(),
    execSync(sql: string) {
      const scripted = failNextExec.get(sql);
      if (scripted) {
        failNextExec.delete(sql);
        throw scripted;
      }
      if (sql === 'BEGIN' || sql === 'BEGIN IMMEDIATE') {
        transactionOpen = true;
      }
      if (sql === 'COMMIT' || sql === 'ROLLBACK') {
        if (!transactionOpen) {
          throw new Error(
            `SQLite error code: 1, description: cannot ${sql.toLowerCase()} - no transaction is active`,
          );
        }
        transactionOpen = false;
      }
    },
    prepare() {
      return {
        exec: () => Promise.resolve(),
        all: () => {
          if (statementFailure) {
            const failure = statementFailure;
            statementFailure = undefined;
            transactionOpen = false;
            return Promise.reject(failure);
          }
          return Promise.resolve([]);
        },
      };
    },
  };
  return {
    db,
    failNextStatement(error: Error) {
      statementFailure = error;
    },
    failNextExec(sql: string, error: Error) {
      failNextExec.set(sql, error);
    },
  };
}

// "granted" when the lock request is granted within `ms` (releasing the handle
// so the probe holds nothing itself), otherwise "still waiting". A rejected
// request propagates so a failed transaction start is not mistaken for a grant.
function grantedWithin(
  promise: Promise<{release(): void}>,
  ms: number,
): Promise<'granted' | 'still waiting'> {
  return Promise.race([
    promise.then(handle => {
      try {
        handle.release();
      } catch {
        // A refused ROLLBACK on the scripted database is not what is measured.
      }
      return 'granted' as const;
    }),
    sleep(ms).then(() => 'still waiting' as const),
  ]);
}

test('SQLiteStore releases the read lock when the shared read transaction cannot be committed', async () => {
  const scripted = makeScriptedDatabase();
  const store = new SQLiteStore('read-commit-refused', () => scripted.db, {});

  const read = await store.read();
  scripted.failNextStatement(
    new Error('SQLite error code: 10, description: disk I/O error'),
  );
  await expect(read.get('k')).rejects.toThrow(/disk I\/O error/);

  // SQLite already rolled the shared read transaction back, so the last
  // reader's closing COMMIT is refused. The error is the reader's to see...
  expect(() => read.release()).toThrow(
    /cannot commit - no transaction is active/,
  );

  // ...and the RWLock must still be released, or every write() on this store
  // (persist, refresh, heartbeat, GC) waits forever from here on.
  expect(await grantedWithin(store.write(), 50)).toBe('granted');
  await store.close();
});

test('SQLiteStore releases the write lock when BEGIN IMMEDIATE throws', async () => {
  const scripted = makeScriptedDatabase();
  const store = new SQLiteStore(
    'begin-immediate-refused',
    () => scripted.db,
    {},
  );

  scripted.failNextExec(
    'BEGIN IMMEDIATE',
    new Error('SQLite error code: 5, description: database is locked'),
  );
  await expect(store.write()).rejects.toThrow(/database is locked/);

  // The caller was told; the store must be usable again.
  expect(await grantedWithin(store.write(), 50)).toBe('granted');
  expect(await grantedWithin(store.read(), 50)).toBe('granted');
  await store.close();
});

test('SQLiteStore releases the read lock when BEGIN throws', async () => {
  const scripted = makeScriptedDatabase();
  const store = new SQLiteStore('begin-refused', () => scripted.db, {});

  scripted.failNextExec(
    'BEGIN',
    new Error('SQLite error code: 10, description: disk I/O error'),
  );
  await expect(store.read()).rejects.toThrow(/disk I\/O error/);

  expect(await grantedWithin(store.write(), 50)).toBe('granted');
  await store.close();
});
