import type {LogContext} from '@rocicorp/logger';
import {
  dropIDBStoreWithMemFallback,
  newIDBStoreWithMemFallback,
} from './kv/idb-store-with-mem-fallback.ts';
import {dropMemStore, MemStore} from './kv/mem-store.ts';
import type {StoreProvider} from './kv/store.ts';

export function getKVStoreProvider(
  lc: LogContext,
  kvStore: 'mem' | 'idb' | StoreProvider | undefined,
): StoreProvider {
  switch (kvStore) {
    case 'idb':
    case undefined:
      return {
        create: name => newIDBStoreWithMemFallback(lc, name),
        drop: dropIDBStoreWithMemFallback,
      };
    case 'mem':
      return {
        create: name => new MemStore(name),
        drop: name => dropMemStore(name),
      };
    default:
      return kvStore;
  }
}
