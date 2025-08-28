import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../shared/src/asserts.ts';
import {createMemStore} from './create-mem-store.ts';
import {
  dropIDBStoreWithMemFallback,
  newIDBStoreWithMemFallback,
} from './kv/idb-store-with-mem-fallback.ts';
import {dropMemStore} from './kv/mem-store.ts';
import type {StoreProvider} from './kv/store.ts';

export type KVStoreProvider = (
  lc: LogContext,
  kvStore: string | StoreProvider | undefined,
) => StoreProvider;

export function getKVStoreProvider(
  lc: LogContext,
  kvStore: string | StoreProvider | undefined,
): StoreProvider {
  switch (kvStore) {
    case 'idb':
    case undefined:
      return {
        create: (name: string) => newIDBStoreWithMemFallback(lc, name),
        drop: dropIDBStoreWithMemFallback,
      };
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
