import {expect, test} from 'vitest';
import {IDBOpenError} from './kv/idb-store.ts';
import {classifySQLiteError} from './kv/sqlite-store.ts';
import {StorageFailureError, getStorageFailure} from './storage-failure.ts';

test('classifies the three SQLite storage faults by errmsg, whatever the driver wrapped them in', () => {
  // op-sqlite's wrapper, as logged in production.
  expect(
    classifySQLiteError(
      new Error(
        'Exception in HostFunction: [op-sqlite] SQLite error code: 13, description: database or disk is full',
      ),
    ),
  ).toBe('full');
  expect(
    classifySQLiteError(
      new Error(
        '[op-sqlite] SQLite error code: 14, description: unable to open database file',
      ),
    ),
  ).toBe('cannot-open');
  expect(
    classifySQLiteError(
      new Error(
        '[op-sqlite] SQLite error code: 10, description: disk I/O error',
      ),
    ),
  ).toBe('io-error');
  // expo-sqlite reports the bare errmsg; better-sqlite3 style reports the
  // symbolic code.
  expect(classifySQLiteError(new Error('disk I/O error'))).toBe('io-error');
  expect(
    classifySQLiteError(new Error('SQLITE_FULL: database or disk is full')),
  ).toBe('full');
  expect(classifySQLiteError(new Error('SQLITE_CANTOPEN'))).toBe('cannot-open');
});

test('does not classify other SQLite errors', () => {
  // SQLITE_BUSY is contention, not a storage fault; busy_timeout and a retry
  // are the right answer to it.
  expect(
    classifySQLiteError(
      new Error('SQLite error code: 5, description: database is locked'),
    ),
  ).toBeUndefined();
  expect(
    classifySQLiteError(new Error('no such table: entry')),
  ).toBeUndefined();
  expect(classifySQLiteError(undefined)).toBeUndefined();
  expect(classifySQLiteError(42)).toBeUndefined();
});

test('getStorageFailure sees through the combined transaction error and a cause chain', () => {
  const failure = new StorageFailureError('io-error', 'disk I/O error');
  expect(getStorageFailure(failure)).toBe(failure);

  // `using()` in with-transactions.ts reports a failed operation whose
  // release also failed as one Error with the original attached as `cause`.
  const combined = new Error(
    `Transaction operation failed and release also failed: operation error = ${String(failure)}; release error = Error: cannot rollback - no transaction is active`,
    {cause: failure},
  );
  expect(getStorageFailure(combined)).toBe(failure);

  const wrapped = new Error('persist failed', {
    cause: new Error('outer', {cause: failure}),
  });
  expect(getStorageFailure(wrapped)).toBe(failure);
});

test('getStorageFailure finds nothing in other errors', () => {
  expect(getStorageFailure(new Error('Chunk not found abc'))).toBeUndefined();
  // A driver error the store did not report as a storage failure.
  expect(getStorageFailure(new Error('disk I/O error'))).toBeUndefined();
  expect(getStorageFailure(undefined)).toBeUndefined();
  expect(getStorageFailure(null)).toBeUndefined();
  expect(getStorageFailure('disk I/O error')).toBeUndefined();
});

test('an IndexedDB that fails to open cannot be opened, unless the browser names a full disk', () => {
  const openError = new IDBOpenError('Failed to open IndexedDB db', {
    cause: new DOMException(
      'A mutation operation was attempted on a database that did not allow mutations.',
      'InvalidStateError',
    ),
  });
  expect(openError).toBeInstanceOf(StorageFailureError);
  expect(openError.kind).toBe('cannot-open');
  // WebKit's IndexedDB is SQLite underneath.
  expect(
    new IDBOpenError('Failed to open IndexedDB db', {
      cause: new DOMException(
        'Error creating Records table (13) - database or disk is full',
        'UnknownError',
      ),
    }).kind,
  ).toBe('full');
});
