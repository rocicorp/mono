import type {DurableObjectLocationHint} from '@cloudflare/workers-types';
import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import postgres from 'postgres';
import {DurableStorage} from '../storage/durable-storage.js';
import type {JSONObject} from '../types/bigint-json.js';
import {PostgresDB, postgresTypeConfig} from '../types/pg.js';
import {
  InvalidationWatcher,
  InvalidationWatcherService,
} from './invalidation-watcher/invalidation-watcher.js';
import type {InvalidationWatcherRegistry} from './invalidation-watcher/registry.js';
import {Mutagen, MutagenService} from './mutagen/mutagen.js';
import type {ReplicatorRegistry} from './replicator/registry.js';
import {Replicator, ReplicatorService} from './replicator/replicator.js';
import type {Service} from './service.js';
import {ViewSyncer, ViewSyncerService} from './view-syncer/view-syncer.js';

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
  // eslint-disable-next-line @typescript-eslint/naming-convention
  DATADOG_LOGS_API_KEY?: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  DATADOG_SERVICE_LABEL?: string;
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

  readonly #storage: DurableStorage;
  readonly #env: ServiceRunnerEnv;
  readonly #upstream: PostgresDB;
  readonly #replica: PostgresDB;
  readonly #lc: LogContext;

  #warmedUpConnections = false;

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
    // TODO: We should have separate upstream URIs for the Replicator (direct connection)
    //       vs mutagen (can be a pooled connection).
    this.#upstream = postgres(this.#env.UPSTREAM_URI, {
      ...postgresTypeConfig(),
    });
    this.#replica = postgres(this.#env.SYNC_REPLICA_URI, {
      ...postgresTypeConfig(),
    });
  }

  getInvalidationWatcher(): Promise<InvalidationWatcher> {
    return Promise.resolve(
      this.#getService(
        INVALIDATION_WATCHER_ID,
        this.#invalidationWatchers,
        id => new InvalidationWatcherService(id, this.#lc, this, this.#replica),
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
            this.#upstream,
            this.#replica,
          ),
        'ReplicatorService',
      ),
    );
  }

  getViewSyncer(clientGroupID: string): ViewSyncer {
    this.#warmUpConnections();
    return this.#getService(
      clientGroupID,
      this.#viewSyncers,
      id =>
        new ViewSyncerService(this.#lc, id, this.#storage, this, this.#replica),
      'ViewSyncer',
    );
  }

  #warmUpConnections() {
    if (!this.#warmedUpConnections) {
      this.#warmedUpConnections = true;
      const start = Date.now();
      void Promise.all([
        // Warm up 1 upstream connection for mutagen, and 2 replica connections for view syncing.
        // Note: These can be much larger when not limited to 6 TCP connections per DO.
        this.#upstream`SELECT 1`.simple().execute(),
        ...Array.from({length: 2}, () =>
          this.#replica`SELECT 1`.simple().execute(),
        ),
      ])
        .then(
          () =>
            this.#lc.info?.(
              `warmed up db connections (${Date.now() - start} ms)`,
            ),
        )
        .catch(e => this.#lc.error?.(`error warming up db connections`, e));
    }
  }

  getMutagen(clientGroupID: string): Mutagen {
    // The MutagenService implementation is stateless. No need to keep a map or share instances.
    return new MutagenService(this.#lc, clientGroupID, this.#upstream);
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

  async status(): Promise<JSONObject> {
    // One ping to warm up the connections
    await Promise.all([this.#replica`SELECT 1`, this.#upstream`SELECT 1`]);

    const start = Date.now();
    const replicaPingMs = this.#replica`SELECT 1`
      .simple()
      .then(() => Date.now() - start);
    const upstreamPingMs = this.#upstream`SELECT 1`
      .simple()
      .then(() => Date.now() - start);
    return {
      status: 'OK',
      replicaPingMs: await replicaPingMs,
      upstreamPingMs: await upstreamPingMs,
    };
  }
}
