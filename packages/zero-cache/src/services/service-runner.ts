import {ViewSyncerService} from './view-syncer/view-syncer.js';
import {Replicator, ReplicatorService} from './replicator/replicator.js';
import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {DurableStorage} from '../storage/durable-storage.js';
import type {InvalidationWatcherRegistry} from './invalidation-watcher/registry.js';
import {
  InvalidationWatcher,
  InvalidationWatcherService,
} from './invalidation-watcher/invalidation-watcher.js';
import type {ReplicatorRegistry} from './replicator/registry.js';
import postgres from 'postgres';
import {postgresTypeConfig} from '../types/pg.js';
import type {Service} from './service.js';
import type {DurableObjectLocationHint} from '@cloudflare/workers-types';

export interface ServiceRunnerEnv {
  runnerDO: DurableObjectNamespace;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  UPSTREAM_URI: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  SYNC_REPLICA_URI: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  DO_LOCATION_HINT: DurableObjectLocationHint;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  LOG_LEVEL: LogLevel;
}

const REPLICATOR_ID = 'r1';
const INVALIDATION_WATCHER_ID = 'iw1';

export class ServiceRunner
  implements ReplicatorRegistry, InvalidationWatcherRegistry
{
  readonly #viewSyncers: Map<string, ViewSyncerService> = new Map();
  readonly #replicators: Map<string, ReplicatorService> = new Map();
  readonly #invalidationWatchers: Map<string, InvalidationWatcherService> =
    new Map();

  #storage: DurableStorage;
  #env: ServiceRunnerEnv;
  readonly #lc: LogContext;

  constructor(
    logSink: LogSink,
    logLevel: LogLevel,
    state: DurableObjectState,
    env: ServiceRunnerEnv,
  ) {
    this.#lc = new LogContext(logLevel, undefined, logSink).withContext(
      'component',
      'ServiceRunnerDO',
    );
    this.#storage = new DurableStorage(state.storage);
    this.#env = env;

    // start the replicator
    void this.getReplicator();
  }

  getInvalidationWatcher(): Promise<InvalidationWatcher> {
    return Promise.resolve(
      this.#getService(
        INVALIDATION_WATCHER_ID,
        this.#invalidationWatchers,
        id =>
          new InvalidationWatcherService(
            id,
            this.#lc,
            this,
            postgres(this.#env.SYNC_REPLICA_URI, {
              ...postgresTypeConfig(),
            }),
          ),
        'InvalidationWatcherService',
      ),
    );
  }

  getReplicator(): Promise<Replicator> {
    return Promise.resolve(
      this.#getService(
        REPLICATOR_ID,
        this.#replicators,
        id =>
          new ReplicatorService(
            this.#lc,
            id,
            this.#env.UPSTREAM_URI,
            this.#env.SYNC_REPLICA_URI,
          ),
        'ReplicatorService',
      ),
    );
  }

  getViewSyncer(clientGroupID: string): ViewSyncerService {
    return this.#getService(
      'viewSyncer:' + clientGroupID,
      this.#viewSyncers,
      id => new ViewSyncerService(this.#lc, id, this.#storage, this),
      'ReplicatorService',
    );
  }

  #getService<S extends Service>(
    id: string,
    registry: Map<string, S>,
    create: (id: string) => S,
    description: string,
  ): S {
    const existing = registry.get(id);
    if (existing) {
      return existing;
    }
    this.#lc.debug?.('Creating and running service', description);
    const service = create(id);
    registry.set(id, service);
    void service
      .run()
      .catch(e => {
        this.#lc.info?.('Error in run of', description, e);
        this.#lc.info?.(e.toString());
      })
      .finally(() => {
        registry.delete(id);
      });
    return service;
  }
}
