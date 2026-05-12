import {expect, test, vi} from 'vitest';
import {
  SQLiteWrite,
  type PreparedStatements,
  type SQLiteDatabase,
} from './sqlite-store.ts';

function makePreparedStatement() {
  return {
    all: vi.fn().mockResolvedValue([]),
    exec: vi.fn().mockResolvedValue(undefined),
  };
}

function makePreparedStatements(): PreparedStatements {
  return {
    has: makePreparedStatement(),
    get: makePreparedStatement(),
    hasMany: makePreparedStatement(),
    getMany: makePreparedStatement(),
    del: makePreparedStatement(),
    put: makePreparedStatement(),
  };
}

test('SQLiteWrite batches deletes and upserts into one statement each', async () => {
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

  // 2 deletes → cached del[1]
  expect(preparedStatements.del.exec).toHaveBeenCalledWith([
    '["delete-1","delete-2"]',
  ]);
  // 2 upserts → cached upserts[1]
  expect(preparedStatements.put.exec).toHaveBeenCalledWith([
    '[["upsert-1","value-1"],["upsert-2",{"nested":true}]]',
  ]);
  expect(db.execSync).toHaveBeenCalledWith('COMMIT');
  expect(release).toHaveBeenCalledTimes(1);
});
