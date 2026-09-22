import {rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import open from '@rocicorp/zero-sqlite3';
import {expect, test} from 'vitest';
import {withRead, withWrite} from '../../with-transactions.ts';
import {
  registerCreatedFile,
  runSQLiteStoreTests,
} from '../sqlite-store-test-util.ts';
import {
  clearAllNamedStoresForTesting,
  safeFilename,
  setupDatabase,
  type SQLiteDatabase,
} from '../sqlite-store.ts';
import {zeroSQLiteStoreProvider, type ZeroSQLiteStoreOptions} from './store.ts';

const defaultStoreOptions: ZeroSQLiteStoreOptions = {
  busyTimeout: 200,
  journalMode: 'WAL',
  synchronous: 'NORMAL',
  readUncommitted: false,
  directory: tmpdir(),
};

function createStore(name: string, opts?: ZeroSQLiteStoreOptions) {
  const provider = zeroSQLiteStoreProvider({...defaultStoreOptions, ...opts});
  name = `zero_${name}`;
  const store = provider.create(name);
  registerCreatedFile(name);
  return store;
}

// Run all shared SQLite store tests
runSQLiteStoreTests<ZeroSQLiteStoreOptions>({
  storeName: 'ZeroSQLiteStore',
  createStoreProvider: zeroSQLiteStoreProvider,
  clearAllNamedStores: clearAllNamedStoresForTesting,
  createStoreWithDefaults: createStore,
  defaultStoreOptions,
});

// ZeroSQLite-specific tests
test('ZeroSQLite specific configuration options', async () => {
  // Test ZeroSQLite-specific configuration options
  const storeWithOptions = createStore('zero-sqlite-pragma-test', {
    busyTimeout: 500,
    journalMode: 'DELETE',
    synchronous: 'FULL',
    readUncommitted: true,
  });

  await withWrite(storeWithOptions, async wt => {
    await wt.put('config-test', 'configured-value');
  });

  await withRead(storeWithOptions, async rt => {
    expect(await rt.get('config-test')).toBe('configured-value');
  });

  await storeWithOptions.close();
});

test('entry rejects NULL keys', async () => {
  // In a rowid table a TEXT PRIMARY KEY accepts NULLs unless it is declared
  // NOT NULL; WITHOUT ROWID enforced that implicitly. Guard it against the real
  // engine rather than the schema text.
  const name = 'zero-sqlite-null-key';
  const store = createStore(name);
  await withWrite(store, async wt => {
    await wt.put('k', 'v');
  });
  await store.close();

  const db = open(`${tmpdir()}/${safeFilename(`zero_${name}`)}`);
  try {
    const insert = db.prepare('INSERT INTO entry (key, value) VALUES (?, ?)');
    expect(() => insert.run(null, '"x"')).toThrow(/NOT NULL constraint failed/);
    expect(db.prepare('SELECT count(*) AS n FROM entry').get()).toEqual({n: 1});
  } finally {
    db.close();
  }
});

test('setupDatabase pragmas take effect on a real SQLite connection', () => {
  // The pragma tests in sqlite-store.test.node.ts only record the SQL sent to a
  // fake delegate. SQLite ignores page_size silently when it is issued too late,
  // and a build can cap mmap_size (SQLITE_MAX_MMAP_SIZE), so read back what the
  // engine actually applied.
  const filename = `${tmpdir()}/${safeFilename('zero_sqlite_pragma_effect')}`;
  rmSync(filename, {force: true});
  const db = open(filename);
  try {
    const delegate: SQLiteDatabase = {
      close: () => db.close(),
      destroy: () => {},
      prepare: sql => {
        const statement = db.prepare(sql);
        return {
          exec: params => {
            statement.run(params);
            return Promise.resolve();
          },
          all: params =>
            Promise.resolve(statement.raw(true).all(...params) as unknown[][]),
        };
      },
      execSync: sql => {
        db.exec(sql);
      },
    };
    setupDatabase(delegate);

    expect(db.pragma('page_size', {simple: true})).toBe(8192);
    expect(db.pragma('mmap_size', {simple: true})).toBe(268435456);
    expect(db.pragma('journal_mode', {simple: true})).toBe('wal');
  } finally {
    db.close();
    for (const suffix of ['', '-wal', '-shm']) {
      rmSync(filename + suffix, {force: true});
    }
  }
});
