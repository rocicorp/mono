import type {LogContext} from '@rocicorp/logger';
import {
  dropIDBStoreWithMemFallback,
  newIDBStoreWithMemFallback,
} from './kv/idb-store-with-mem-fallback.ts';
import {dropMemStore} from './kv/mem-store.ts';
import type {StoreProvider} from './kv/store.ts';
import {createMemStore} from './replicache.ts';

export function getKVStoreProvider(
  lc: LogContext,
  kvStore: 'mem' | 'idb' | StoreProvider | undefined,
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
      return kvStore;
  }
}
