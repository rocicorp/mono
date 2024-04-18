import type {LogContext} from '@rocicorp/logger';
import * as v from 'shared/src/valita.js';
import type {Storage} from '../../storage/storage.js';
import type {InvalidationWatcherRegistry} from '../invalidation-watcher/registry.js';
import type {Service} from '../service.js';

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
  readonly #storage: Storage;
  readonly #registry: InvalidationWatcherRegistry;

  constructor(
    lc: LogContext,
    clientGroupID: string,
    storage: Storage,
    registry: InvalidationWatcherRegistry,
  ) {
    this.id = clientGroupID;
    this.#lc = lc
      .withContext('component', 'view-syncer')
      .withContext('serviceID', this.id);
    this.#storage = storage;
    this.#registry = registry;
  }

  run(): Promise<void> {
    // TODO: Implement
    this.#lc;
    this.#storage;
    this.#registry;

    throw new Error('todo');
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
