import {afterAll, bench, describe, expect} from 'vitest';
import {withRead, withWrite} from '../with-transactions.ts';
import {getTestSQLiteDatabaseManager} from './sqlite-store-test-util.ts';
import {SQLiteStore} from './sqlite-store.ts';

const walSQLite3DatabaseManager = getTestSQLiteDatabaseManager();
const walStore = new SQLiteStore('bench-wal', walSQLite3DatabaseManager, {
  journalMode: 'WAL',
});

const defaultSQLite3DatabaseManager = getTestSQLiteDatabaseManager();
const defaultStore = new SQLiteStore('bench-default', defaultSQLite3DatabaseManager);

afterAll(() => {
  walSQLite3DatabaseManager.clearAllStoresForTesting();
  defaultSQLite3DatabaseManager.clearAllStoresForTesting();
});

describe('zero-sqlite3 WAL', () => {
  bench(
    `write`,
    async () => {
      await withWrite(walStore, async wt => {
        await wt.put('foo1', 'bar1');
      });
    },
    {
      throws: true,
      teardown: async () => {
        await withWrite(walStore, async wt => {
          await wt.del('foo1');
        });
      },
    },
  );

  bench(
    `read`,
    async () => {
      await withRead(walStore, async rt => {
        expect(await rt.get('foo2')).equal('bar2');
      });
    },
    {
      throws: true,
      setup: async () => {
        await withWrite(walStore, async wt => {
          await wt.put('foo2', 'bar2');
        });
      },
    },
  );

  bench(
    `write and read`,
    async () => {
      await withWrite(walStore, async wt => {
        await wt.put('foo3', 'bar3');
      });

      await withRead(walStore, async rt => {
        expect(await rt.get('foo3')).equal('bar3');
      });
    },
    {
      throws: true,
      teardown: async () => {
        await withWrite(walStore, async wt => {
          await wt.del('foo3');
        });
      },
    },
  );

  bench(
    `delete`,
    async () => {
      await withWrite(walStore, async wt => {
        await wt.del('foo4');
      });
    },
    {
      throws: true,
      setup: async () => {
        await withWrite(walStore, async wt => {
          await wt.put('foo4', 'bar4');
        });
      },
    },
  );
});

describe('zero-sqlite3 default journal mode', () => {
  bench(
    `write`,
    async () => {
      await withWrite(defaultStore, async wt => {
        await wt.put('foo1', 'bar1');
      });
    },
    {
      throws: true,
      teardown: async () => {
        await withWrite(defaultStore, async wt => {
          await wt.del('foo1');
        });
      },
    },
  );

  bench(
    `read`,
    async () => {
      await withRead(defaultStore, async rt => {
        expect(await rt.get('foo2')).equal('bar2');
      });
    },
    {
      throws: true,
      setup: async () => {
        await withWrite(defaultStore, async wt => {
          await wt.put('foo2', 'bar2');
        });
      },
    },
  );

  bench(
    `write and read`,
    async () => {
      await withWrite(defaultStore, async wt => {
        await wt.put('foo3', 'bar3');
      });

      await withRead(defaultStore, async rt => {
        expect(await rt.get('foo3')).equal('bar3');
      });
    },
    {
      throws: true,
      teardown: async () => {
        await withWrite(defaultStore, async wt => {
          await wt.del('foo3');
        });
      },
    },
  );

  bench(
    `delete`,
    async () => {
      await withWrite(defaultStore, async wt => {
        await wt.del('foo4');
      });
    },
    {
      throws: true,
      setup: async () => {
        await withWrite(defaultStore, async wt => {
          await wt.put('foo4', 'bar4');
        });
      },
    },
  );
});
