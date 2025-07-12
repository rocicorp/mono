/* eslint-disable no-unsafe-finally */
/* eslint-disable require-await */
/* eslint-disable no-console */
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {runAll} from '../../replicache/src/kv/store-test-util.ts';
import type {Store} from '../../replicache/src/kv/store.ts';
import {withRead, withWrite} from '../../replicache/src/with-transactions.ts';
import {clearMockDbData} from '../__mocks__/expo-sqlite.ts';
import {createExpoSQLiteStore} from './store.ts';

vi.mock('expo-sqlite');

async function newRandomExpoStore() {
  const name = `expo-store-${Math.random()}`;
  clearMockDbData();
  await createExpoSQLiteStore.drop(name).catch(() => {});
  return createExpoSQLiteStore.create(name);
}

runAll('expo-store', newRandomExpoStore);

describe('ExpoSQLiteStore additional tests', () => {
  let store: Store;
  let storeName: string;

  beforeEach(async () => {
    storeName = `expo-store-${Math.random()}`;
    clearMockDbData();
    await createExpoSQLiteStore.drop(storeName).catch(() => {});
    store = createExpoSQLiteStore.create(storeName);
  });

  afterEach(async () => {
    await store.close();
    await createExpoSQLiteStore.drop(storeName);
  });

  test('Throws if store dropped while open', async () => {
    await withWrite(store, async w => {
      await w.put('initial', 'value');
    });

    await createExpoSQLiteStore.drop(storeName);

    let err;
    try {
      await withRead(store, async tx => {
        await tx.has('initial');
      });
    } catch (e) {
      err = e;
    }
    expect(err).toBeDefined();
    expect((err as Error).message).toMatch(/Expo SQLite store not found/);
  });

  test('Handles multiple concurrent writes', async () => {
    const concurrentWrites = [
      withWrite(store, async w => {
        await w.put('key1', 'value1');
      }),
      withWrite(store, async w => {
        await w.put('key2', 'value2');
      }),
      withWrite(store, async w => {
        await w.put('key3', 'value3');
      }),
    ];

    await Promise.all(concurrentWrites);

    await withRead(store, async r => {
      expect(await r.get('key1')).toBe('value1');
      expect(await r.get('key2')).toBe('value2');
      expect(await r.get('key3')).toBe('value3');
    });
  });

  test('Preserves data across store reopens', async () => {
    await withWrite(store, async w => {
      await w.put('persistent', 'data');
    });

    // Close and reopen the store
    await store.close();
    store = createExpoSQLiteStore.create(storeName);

    await withRead(store, async r => {
      expect(await r.get('persistent')).toBe('data');
    });
  });
});
