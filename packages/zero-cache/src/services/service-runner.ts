import type {
  DurableObjectLocationHint,
  Fetcher,
} from '@cloudflare/workers-types';
import type {LogContext, LogLevel} from '@rocicorp/logger';
import postgres from 'postgres';
import {jsonObjectSchema} from 'shared/src/json-schema.js';
import * as v from 'shared/src/valita.js';
import {DurableStorage} from '../storage/durable-storage.js';
import type {JSONObject} from '../types/bigint-json.js';
import {PostgresDB, postgresTypeConfig} from '../types/pg.js';
import {streamIn, type CancelableAsyncIterable} from '../types/streams.js';
import {
  InvalidationWatcher,
  InvalidationWatcherService,
} from './invalidation-watcher/invalidation-watcher.js';
import type {InvalidationWatcherRegistry} from './invalidation-watcher/registry.js';
import {getDOLocation} from './location.js';
import {Mutagen, MutagenService} from './mutagen/mutagen.js';
import {
  REGISTER_FILTERS_PATTERN,
  REPLICATOR_STATUS_PATTERN,
  VERSION_CHANGES_PATTERN,
} from './paths.js';
import type {ReplicatorRegistry} from './replicator/registry.js';
import {
  RegisterInvalidationFiltersRequest,
  RegisterInvalidationFiltersResponse,
  Replicator,
  ReplicatorService,
  VersionChange,
  registerInvalidationFiltersResponse,
  versionChangeSchema,
} from './replicator/replicator.js';
import type {Service} from './service.js';
import {ViewSyncer, ViewSyncerService} from './view-syncer/view-syncer.js';

export interface ServiceRunnerEnv {
  runnerDO: DurableObjectNamespace;
  replicatorDO: DurableObjectNamespace;
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
  readonly #runReplicator: boolean;

  constructor(
    lc: LogContext,
    state: DurableObjectState,
    env: ServiceRunnerEnv,
    runReplicator: boolean,
  ) {
    this.#lc = lc;
    this.#storage = new DurableStorage(state.storage);
    this.#env = env;
    // Connections are capped to stay within the DO limit of 6 TCP connections.
    // Note that the Replicator uses one extra connection for replication.
    //
    // TODO: We should have separate upstream URIs for the Replicator (direct connection)
    //       vs mutagen (can be a pooled connection).
    this.#upstream = postgres(this.#env.UPSTREAM_URI, {
      ...postgresTypeConfig(),
      max: 1,
    });
    this.#replica = postgres(this.#env.SYNC_REPLICA_URI, {
      ...postgresTypeConfig(),
      max: 4,
    });
    this.#runReplicator = runReplicator;

    if (!runReplicator) {
      // Ping the replicator to bootstrap the replication process for new replicas.
      void this.#getReplicatorStub()
        .status()
        .then(res => lc.info?.(`Replicator status`, res))
        .catch(e => lc.error?.('Error pinging replicator', e));
    }
    this.#warmUpConnections();
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

  // eslint-disable-next-line require-await
  async getReplicator(): Promise<Replicator> {
    if (this.#runReplicator) {
      return this.#getService(
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
      );
    }
    return this.#getReplicatorStub();
  }

  #getReplicatorStub(): ReplicatorStub {
    const id = this.#env.replicatorDO.idFromName(REPLICATOR_ID);
    const stub = this.#env.replicatorDO.get(id, getDOLocation(this.#env));
    return new ReplicatorStub(this.#lc, stub);
  }

  getViewSyncer(clientGroupID: string): ViewSyncer {
    return this.#getService(
      clientGroupID,
      this.#viewSyncers,
      id =>
        new ViewSyncerService(this.#lc, id, this.#storage, this, this.#replica),
      'ViewSyncer',
    );
  }

  #warmUpConnections() {
    const start = Date.now();
    void Promise.all([
      // Warm up 1 upstream connection for mutagen, and 4 replica connections for view syncing.
      // Note: These can be much larger when not limited to 6 TCP connections per DO.
      this.#upstream`SELECT 1`.simple().execute(),
      ...Array.from({length: 4}, () =>
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

class ReplicatorStub implements Replicator {
  readonly #lc: LogContext;
  readonly #stub: Fetcher;

  constructor(lc: LogContext, stub: Fetcher) {
    this.#lc = lc.withContext('stub', 'Replicator');
    this.#stub = stub;
  }

  async status() {
    const lc = this.#lc.withContext('method', 'status');
    const res = await this.#stub.fetch(
      `https://unused.dev${REPLICATOR_STATUS_PATTERN.replace(
        ':version',
        'v0',
      )}`,
      {method: 'POST'},
    );
    const data = await res.json();
    lc.debug?.('received', data);
    return v.parse(data, jsonObjectSchema);
  }

  async registerInvalidationFilters(
    req: RegisterInvalidationFiltersRequest,
  ): Promise<RegisterInvalidationFiltersResponse> {
    const lc = this.#lc.withContext('method', 'registerInvalidationFilters');
    const res = await this.#stub.fetch(
      `https://unused.dev${REGISTER_FILTERS_PATTERN.replace(':version', 'v0')}`,
      {
        method: 'POST',
        body: JSON.stringify(req),
      },
    );
    if (!res.ok) {
      throw new Error(
        `registerInvalidationFilters: ${res.status}: ${await res.text()}`,
      );
    }
    const data = await res.json();
    lc.debug?.('received', data);
    return v.parse(data, registerInvalidationFiltersResponse);
  }

  async versionChanges(): Promise<CancelableAsyncIterable<VersionChange>> {
    const lc = this.#lc.withContext('method', 'versionChanges');
    const res = await this.#stub.fetch(
      `https://unused.dev${VERSION_CHANGES_PATTERN.replace(':version', 'v0')}`,
      {headers: {upgrade: 'websocket'}},
    );

    const ws = res.webSocket;
    if (!ws) {
      throw new Error(
        `server did not accept WebSocket: ${res.status}: ${await res.text()}`,
      );
    }
    ws.accept();

    return streamIn(lc, ws, versionChangeSchema);
  }
}
