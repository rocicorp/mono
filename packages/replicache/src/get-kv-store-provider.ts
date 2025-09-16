/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
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
