import type {LogContext} from '@rocicorp/logger';
import {dropIDBStore, IDBStore} from './kv/idb-store.ts';
import {dropMemStore, MemStore} from './kv/mem-store.ts';
import type {StoreProvider} from './kv/store.ts';

export function getKVStoreProvider(
  _lc: LogContext,
  kvStore: 'mem' | 'idb' | StoreProvider | undefined,
): StoreProvider {
  switch (kvStore) {
    case 'idb':
    case undefined:
      return {
        create: name => new IDBStore(name),
        drop: dropIDBStore,
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
