import {expect, test} from 'vitest';
import {classifyStorageFailure} from './storage-failure.ts';

test('classifies the three SQLite storage faults by errmsg, whatever the driver wrapped them in', () => {
  // op-sqlite's wrapper, as logged in production.
  expect(
    classifyStorageFailure(
      new Error(
        'Exception in HostFunction: [op-sqlite] SQLite error code: 13, description: database or disk is full',
      ),
    ),
  ).toBe('full');
  expect(
    classifyStorageFailure(
      new Error(
        '[op-sqlite] SQLite error code: 14, description: unable to open database file',
      ),
    ),
  ).toBe('cannot-open');
  expect(
    classifyStorageFailure(
      new Error(
        '[op-sqlite] SQLite error code: 10, description: disk I/O error',
      ),
    ),
  ).toBe('io-error');
  // expo-sqlite reports the bare errmsg; better-sqlite3 style reports the
  // symbolic code.
  expect(classifyStorageFailure(new Error('disk I/O error'))).toBe('io-error');
  expect(
    classifyStorageFailure(new Error('SQLITE_FULL: database or disk is full')),
  ).toBe('full');
  expect(classifyStorageFailure(new Error('SQLITE_CANTOPEN'))).toBe(
    'cannot-open',
  );
});

test('sees through the combined transaction error and a cause chain', () => {
  // `using()` in with-transactions.ts reports a failed operation whose
  // release also failed as one Error with the original attached as `cause`.
  const original = new Error(
    '[op-sqlite] SQLite error code: 10, description: disk I/O error',
  );
  const combined = new Error(
    `Transaction operation failed and release also failed: operation error = ${String(original)}; release error = Error: cannot rollback - no transaction is active`,
  );
  combined.cause = original;
  expect(classifyStorageFailure(combined)).toBe('io-error');

  const wrapped = new Error('persist failed');
  wrapped.cause = new Error('outer');
  (wrapped.cause as Error).cause = original;
  expect(classifyStorageFailure(wrapped)).toBe('io-error');
});

test('does not classify anything else', () => {
  expect(
    classifyStorageFailure(new Error('Chunk not found abc')),
  ).toBeUndefined();
  expect(
    classifyStorageFailure(
      new Error(
        'Invalid ref count -1 for abc. We expect the value to be a Uint16',
      ),
    ),
  ).toBeUndefined();
  // SQLITE_BUSY is contention, not a storage fault; busy_timeout and a retry
  // are the right answer to it.
  expect(
    classifyStorageFailure(
      new Error('SQLite error code: 5, description: database is locked'),
    ),
  ).toBeUndefined();
  expect(classifyStorageFailure(undefined)).toBeUndefined();
  expect(classifyStorageFailure(null)).toBeUndefined();
  expect(classifyStorageFailure(42)).toBeUndefined();
  const cyclic = new Error('a');
  cyclic.cause = cyclic;
  expect(classifyStorageFailure(cyclic)).toBeUndefined();
});
