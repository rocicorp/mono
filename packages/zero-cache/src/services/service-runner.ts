import {ViewSyncerService} from './view-syncer/view-syncer.js';
import {Replicator, ReplicatorService} from './replicator/replicator.js';
import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {DurableStorage} from '../storage/durable-storage.js';
import type {InvalidationWatcherRegistry} from './invalidation-watcher/registry.js';
import {MutagenService} from './mutagen/mutagen-service.js';

export interface ServiceRunnerEnv {
  runnerDO: DurableObjectNamespace;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  UPSTREAM_URI: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  SYNC_REPLICA_URI: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  LOG_LEVEL: LogLevel;
}

export class ServiceRunner {
  readonly #viewSyncers: Map<string, ViewSyncerService>;
  readonly #replicator: Map<string, ReplicatorService>;
  readonly #mutagens: Map<string, MutagenService>;

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
    this.#mutagens = new Map();
    this.#storage = new DurableStorage(state.storage);
    this.#registry = registry;
    this.#env = env;

    // start the replicator
    void this.getReplicator();
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
    this.#replicator.set(this.#REPLICATOR_ID, rep);
    void rep.run().then(() => {
      this.#replicator.delete(this.#REPLICATOR_ID);
    });
    return Promise.resolve(rep);
  }

  getViewSyncer(clientGroupID: string): ViewSyncerService {
    const v = this.#viewSyncers.get(clientGroupID);
    if (v) {
      return v;
    }
    const vsync = new ViewSyncerService(
      this.#lc,
      clientGroupID,
      this.#storage,
      this.#registry,
    );
    this.#viewSyncers.set(clientGroupID, vsync);
    void vsync.run().then(() => {
      this.#viewSyncers.delete(clientGroupID);
    });
    return vsync;
  }

  getMutagen(clientGroupID: string): MutagenService {
    const m = this.#mutagens.get(clientGroupID);
    if (m) {
      return m;
    }
    const mut = new MutagenService(
      this.#lc,
      clientGroupID,
      this.#env.UPSTREAM_URI,
    );
    this.#mutagens.set(clientGroupID, mut);
    return mut;
  }
}
