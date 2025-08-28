import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {createMemStore} from '../create-mem-store.ts';
import {dropMemStore} from '../kv/mem-store.ts';
import type {StoreProvider} from '../kv/store.ts';
import {expoSQLiteStoreProvider} from './sqlite-store-provider.ts';

export function getKVStoreProvider(
  _lc: LogContext,
  kvStore: string | StoreProvider | undefined,
): StoreProvider {
  switch (kvStore) {
    case 'expo-sqlite':
    case undefined:
      return expoSQLiteStoreProvider();
    case 'mem':
      return {
        create: createMemStore,
        drop: (name: string) => dropMemStore(name),
      };
    default:
      assert(typeof kvStore !== 'string');
      return kvStore;
  }
}
