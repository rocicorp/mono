import {expect, test, vi} from 'vitest';
import {MemStore, dropMemStore} from '../../../replicache/src/kv/mem-store.ts';
import type {Read, Store, Write} from '../../../replicache/src/kv/store.ts';
import {StorageFailureError} from '../../../replicache/src/storage-failure.ts';
import {zeroForTest} from './test-utils.ts';

/**
 * A MemStore whose writes fail the way the SQLite store's do when its disk
 * has failed, so the open fails on its first write.
 */
class WriteFailingStore implements Store {
  readonly #inner: MemStore;

  constructor(name: string) {
    this.#inner = new MemStore(name);
  }

  read(): Promise<Read> {
    return this.#inner.read();
  }

  write(): Promise<Write> {
    return Promise.reject(
      new StorageFailureError('io-error', 'disk I/O error'),
    );
  }

  close(): Promise<void> {
    return this.#inner.close();
  }

  get closed(): boolean {
    return this.#inner.closed;
  }
}

test('connects after a storage failure at open moved the instance onto memory', async () => {
  const failures: StorageFailureError[] = [];
  const z = zeroForTest({
    kvStore: {
      create: name => new WriteFailingStore(name),
      drop: dropMemStore,
    },
    onStorageFailure: failure => failures.push(failure),
  });

  await vi.waitFor(() => expect(failures).toHaveLength(1));
  expect(failures[0].kind).toBe('io-error');

  // Connecting reads the deleted clients from the replica store, which is
  // the in-memory one now, not the failed store it replaced.
  await z.triggerConnected();
  await z.close();
});
