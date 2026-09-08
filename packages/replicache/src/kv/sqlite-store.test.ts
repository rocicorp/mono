import {expect, test, vi} from 'vitest';
import {getOrInsertComputed} from '../../../shared/src/map.ts';
import {
  SQLiteWrite,
  SQLiteStoreRead,
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
