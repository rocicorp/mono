import type {LogContext} from '@rocicorp/logger';
import * as v from 'shared/src/valita.js';
import type {DurableStorage} from '../../storage/durable-storage.js';
import {initStorageSchema} from '../../storage/schema.js';
import type {InvalidationWatcherRegistry} from '../invalidation-watcher/registry.js';
import type {Service} from '../service.js';
import {SCHEMA_MIGRATIONS} from './schema/migrations.js';
import {schemaRoot} from './schema/paths.js';

export const viewShapeUpdateSchema = v.object({
  // TODO: Define
});

export type ViewShapeUpdate = v.Infer<typeof viewShapeUpdateSchema>;

export const viewContentsUpdateSchema = v.object({
  // TODO: Define
});

export type ViewContentsUpdate = v.Infer<typeof viewContentsUpdateSchema>;

export interface ViewSyncer {
  sync(
    shapeUpdates: AsyncIterable<ViewShapeUpdate>,
  ): AsyncIterable<ViewContentsUpdate>;
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
      schemaRoot(this.id),
      SCHEMA_MIGRATIONS,
    );
    // TODO: Implement
    this.#registry;
  }

  sync(
    _shapeUpdates: AsyncIterable<ViewShapeUpdate>,
  ): AsyncIterable<ViewContentsUpdate> {
    throw new Error('todo');
  }

  stop(): Promise<void> {
    throw new Error('todo');
  }
}
