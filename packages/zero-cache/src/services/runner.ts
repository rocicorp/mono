import {
  ViewSyncer,
  ViewSyncerRegistry,
  ViewSyncerService,
} from './view-syncer/view-syncer.js';
import {Replicator, ReplicatorService} from './replicator/replicator.js';
import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {DurableStorage} from '../storage/durable-storage.js';
import type {InvalidationWatcherRegistry} from './invalidation-watcher/registry.js';
import type {ReplicatorRegistry} from './replicator/registry.js';
import type {DurableObjectNamespace} from '@cloudflare/workers-types';

export interface ServiceRunnerEnv {
  runnerDO: DurableObjectNamespace;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  UPSTREAM_URI: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  SYNC_REPLICA_URI: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  LOG_LEVEL: LogLevel;
}
export class ServiceRunnerDO implements ViewSyncerRegistry, ReplicatorRegistry {
  #viewSyncers: Map<string, ViewSyncerService>;
  #replicator: Map<string, ReplicatorService>;
  #storage: DurableStorage;
  #env: ServiceRunnerEnv;
  #registry: InvalidationWatcherRegistry;
  readonly #lc: LogContext;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  #REPLICATOR_ID = 'r1';
  constructor(
    registry: InvalidationWatcherRegistry,
    logSink: LogSink,
    logLevel: LogLevel,
    state: DurableObjectState,
    env: ServiceRunnerEnv,
  ) {
    this.#lc = new LogContext(logLevel, undefined, logSink).withContext(
      'component',
      'ServiceRunnerDO',
    );
    this.#viewSyncers = new Map();
    this.#replicator = new Map();
    this.#storage = new DurableStorage(state.storage);
    this.#registry = registry;
    this.#env = env;
  }

  getReplicator(): Promise<Replicator> {
    const r = this.#replicator.get(this.#REPLICATOR_ID);
    if (r) {
      return Promise.resolve(r);
    }
    const rep = new ReplicatorService(
      this.#lc,
      this.#REPLICATOR_ID,
      this.#env.UPSTREAM_URI,
      this.#env.SYNC_REPLICA_URI,
    );
    void rep.run().then(() => {
      this.#replicator.delete(this.#REPLICATOR_ID);
    });
    return Promise.resolve(rep);
  }

  getViewSyncer(id: string): ViewSyncer {
    const v = this.#viewSyncers.get(id);
    if (v) {
      return v;
    }
    const vsync = new ViewSyncerService(
      this.#lc,
      id,
      this.#storage,
      this.#registry,
    );
    this.#viewSyncers.set(id, vsync);
    void vsync.run().then(() => {
      this.#viewSyncers.delete(id);
    });
    return vsync;
  }

  async fetch(request: Request): Promise<Response> {
    const lc = this.#lc.withContext('url', request.url);
    lc.info?.('Handling request:', request.url);
    try {
      await this.getReplicator();
      return new Response('OK', {status: 200});
    } catch (e) {
      lc.error?.('Unhandled exception in fetch', e);
      return new Response(e instanceof Error ? e.message : String(e), {
        status: 500,
      });
    }
  }
}
