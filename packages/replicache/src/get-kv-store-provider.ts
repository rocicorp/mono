import type {LogContext} from '@rocicorp/logger';
import {createMemStore} from './create-mem-store.ts';
import {dropExpoStore, ExpoStore} from './expo/store.ts';
import {
  dropIDBStoreWithMemFallback,
  newIDBStoreWithMemFallback,
} from './kv/idb-store-with-mem-fallback.ts';
import {dropMemStore} from './kv/mem-store.ts';
import type {StoreProvider} from './kv/store.ts';
import type {KVStoreOption} from './replicache-options.ts';

export type KVStoreProvider = (
  lc: LogContext,
  kvStore: KVStoreOption,
) => StoreProvider;

export function getKVStoreProvider(
  lc: LogContext,
  kvStore: KVStoreOption = defaultKVStore(),
): StoreProvider {
  switch (kvStore) {
    case 'idb':
      return {
        create: (name: string) => newIDBStoreWithMemFallback(lc, name),
        drop: dropIDBStoreWithMemFallback,
      };
    case 'mem':
      return {
        create: createMemStore,
        drop: (name: string) => dropMemStore(name),
      };
    case 'expo-sqlite':
      return {
        create: (name: string) => new ExpoStore(name),
        drop: (name: string) => dropExpoStore(name),
      };

    default:
      return kvStore;
  }
}

function defaultKVStore(): KVStoreOption {
  return navigator?.product === 'ReactNative' ? 'expo-sqlite' : 'idb';
}
