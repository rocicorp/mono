import type {LogContext} from '@rocicorp/logger';
import type {Downstream, Upstream} from 'zero-protocol';
import type {DurableStorage} from '../../storage/durable-storage.js';
import {initStorageSchema} from '../../storage/schema.js';
import type {CancelableAsyncIterable} from '../../types/streams.js';
import type {InvalidationWatcherRegistry} from '../invalidation-watcher/registry.js';
import type {Service} from '../service.js';
import {SCHEMA_MIGRATIONS} from './schema/migrations.js';
import {schemaRoot} from './schema/paths.js';

export type SyncContext = {
  clientID: string;
  baseCookie: string | null;
};

export interface ViewSyncer {
  // The SyncContext comes from query parameters.
  sync(
    ctx: SyncContext,
    updates: CancelableAsyncIterable<Upstream>,
  ): Promise<CancelableAsyncIterable<Downstream>>;
}

export class ViewSyncerService implements ViewSyncer, Service {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #storage: DurableStorage;
  readonly #registry: InvalidationWatcherRegistry;

  constructor(
    lc: LogContext,
    clientGroupID: string,
    storage: DurableStorage,
    registry: InvalidationWatcherRegistry,
  ) {
    this.id = clientGroupID;
    this.#lc = lc
      .withContext('component', 'view-syncer')
      .withContext('serviceID', this.id);
    this.#storage = storage;
    this.#registry = registry;
  }

  async run(): Promise<void> {
    await initStorageSchema(
      this.#lc,
      this.#storage,
      schemaRoot,
      SCHEMA_MIGRATIONS,
    );
    // TODO: Implement
    this.#registry;
  }

  sync(
    _ctx: SyncContext,
    _updates: CancelableAsyncIterable<Upstream>,
  ): Promise<CancelableAsyncIterable<Downstream>> {
    throw new Error('todo');
  }

  stop(): Promise<void> {
    throw new Error('todo');
  }
}
