import {expect, test} from 'vitest';
import {withRead, withWrite} from '../../with-transactions.ts';
import {
  registerCreatedFile,
  runSQLiteStoreTests,
} from '../sqlite-store-test-util.ts';
import {clearAllNamedStoresForTesting} from '../sqlite-store.ts';
import {zeroSQLiteStoreProvider, type ZeroSQLiteStoreOptions} from './store.ts';

const defaultStoreOptions = {
  busyTimeout: 200,
  journalMode: 'WAL',
  synchronous: 'NORMAL',
  readUncommitted: false,
} as const;

function createStore(name: string, opts?: ZeroSQLiteStoreOptions) {
  const provider = zeroSQLiteStoreProvider(opts);
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
