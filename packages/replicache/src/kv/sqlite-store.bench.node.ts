import {afterAll, bench, describe, expect} from 'vitest';
import {withRead, withWrite} from '../with-transactions.ts';
import {getTestSQLiteDatabaseManager} from './sqlite-store-test-util.ts';
import {createSQLiteStore} from './sqlite-store.ts';

const walSQLite3DatabaseManager = getTestSQLiteDatabaseManager();
const createWalStore = createSQLiteStore(walSQLite3DatabaseManager);
const walStore = createWalStore('bench-wal', {
  readPoolSize: 2,
  journalMode: 'WAL',
  synchronous: 'NORMAL',
  readUncommitted: false,
});

const defaultSQLite3DatabaseManager = getTestSQLiteDatabaseManager();
const createDefaultStore = createSQLiteStore(defaultSQLite3DatabaseManager);
const defaultStore = createDefaultStore('bench-default', {
  readPoolSize: 2,
  synchronous: 'NORMAL',
  readUncommitted: false,
});

afterAll(() => {
  walSQLite3DatabaseManager.clearAllStoresForTesting();
  defaultSQLite3DatabaseManager.clearAllStoresForTesting();
});

describe('sqlite tx', () => {
  bench(
    `default journal mode`,
    async () => {
      await withWrite(defaultStore, async wt => {
        expect(await wt.get('bar')).equal(undefined);
        await wt.put('bar', 'baz');
        expect(await wt.get('bar')).equal('baz');
        await wt.del('bar');
        expect(await wt.get('bar')).equal(undefined);
      });
    },
    {
      throws: true,
    },
  );

  bench(
    `WAL journal mode`,
    async () => {
      await withWrite(walStore, async wt => {
        expect(await wt.get('bar')).equal(undefined);
        await wt.put('bar', 'baz');
        expect(await wt.get('bar')).equal('baz');
        await wt.del('bar');
        expect(await wt.get('bar')).equal(undefined);
      });
    },
    {
      throws: true,
    },
  );
});

describe('sqlite write contention', () => {
  bench(
    `default journal mode`,
    async () => {
      await withWrite(defaultStore, async wt => {
        await wt.put('foo', 'bar');
      });

      const readP1 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP2 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP3 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP4 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP5 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const writeP = withWrite(defaultStore, async wt => {
        await wt.put('foo', 'bar2');
      });

      await Promise.all([readP1, readP2, readP3, readP4, readP5, writeP]);
    },
    {
      throws: true,
      teardown: async () => {
        await withWrite(defaultStore, async wt => {
          await wt.del('foo');
        });
      },
    },
  );

  bench(
    `WAL journal mode`,
    async () => {
      await withWrite(walStore, async wt => {
        await wt.put('foo', 'bar');
      });

      const readP1 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP2 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP3 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP4 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP5 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const writeP = withWrite(walStore, async wt => {
        await wt.put('foo', 'bar2');
      });

      await Promise.all([readP1, readP2, readP3, readP4, readP5, writeP]);
    },
    {
      throws: true,
      setup: async () => {
        await withWrite(walStore, async wt => {
          await wt.del('foo');
        });
      },
    },
  );
});

describe('plain read', () => {
  bench(
    `default journal mode`,
    async () => {
      const readP1 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP2 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP3 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP4 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP5 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });

      await Promise.all([readP1, readP2, readP3, readP4, readP5]);
    },
    {
      throws: true,
      setup: async () => {
        await withWrite(defaultStore, async wt => {
          await wt.put('foo', 'bar');
        });
      },
    },
  );

  bench(
    `WAL journal mode`,
    async () => {
      const readP1 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP2 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP3 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP4 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP5 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });

      await Promise.all([readP1, readP2, readP3, readP4, readP5]);
    },
    {
      throws: true,
      setup: async () => {
        await withWrite(walStore, async wt => {
          await wt.put('foo', 'bar');
        });
      },
    },
  );
});
