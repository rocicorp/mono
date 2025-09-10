import {expect, test} from 'vitest';
import {withRead, withWrite} from '../../with-transactions.ts';
import {runSQLiteStoreTests} from '../sqlite-store-test-util.ts';
import {
  clearAllNamedZeroSQLiteStoresForTesting,
  zeroSQLiteStoreProvider,
  type ZeroSQLiteStoreOptions,
} from './store.ts';

const defaultStoreOptions = {
  busyTimeout: 200,
  journalMode: 'WAL',
  synchronous: 'NORMAL',
  readUncommitted: false,
} as const;

function getNewStore(name: string) {
  const provider = zeroSQLiteStoreProvider(defaultStoreOptions);
  return provider.create(name);
}

function createStore(name: string, opts?: ZeroSQLiteStoreOptions) {
  const provider = zeroSQLiteStoreProvider(opts);
  return provider.create(name);
}

// Run all shared SQLite store tests
runSQLiteStoreTests<ZeroSQLiteStoreOptions>({
  storeName: 'ZeroSQLiteStore',
  createStoreProvider: zeroSQLiteStoreProvider,
  clearAllNamedStores: clearAllNamedZeroSQLiteStoresForTesting,
  createStoreWithDefaults: getNewStore,
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
