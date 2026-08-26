import {
  changeLogFile,
  replicaFile,
  replicationManagerEnv,
  taskIDFor,
  viewSyncerEnv,
  type ChangeLogSettings,
  type SoakConfig,
} from './config.ts';
import type {SoakLog} from './logs.ts';
import {SoakNode} from './node.ts';
import type {ReplicaHandle} from './oracle.ts';

/**
 * The topology of plan section 2: one replication-manager with
 * `NUM_SYNC_WORKERS=0`, and N view-syncers, each with its own replica
 * directory, its own task ID, and its own `litestream restore`.
 *
 * Three view-syncers rather than one, because the route census needs
 * concurrent tasks so that a demotion of one is visibly *not* a demotion of
 * the others, and so the purge floor has more than one ack to be held by.
 */
export class SoakCluster {
  readonly rm: SoakNode;
  readonly viewSyncers: readonly SoakNode[];
  readonly #config: SoakConfig;
  #rmSettings: Partial<ChangeLogSettings> = {};

  constructor(config: SoakConfig, log: SoakLog) {
    this.#config = config;
    const rmName = taskIDFor('rm');
    this.rm = new SoakNode({
      name: rmName,
      env: replicationManagerEnv(config),
      logsDir: config.logsDir,
      log,
      replicaFile: replicaFile(config, rmName),
    });
    this.viewSyncers = Array.from({length: config.viewSyncers}, (_, i) => {
      const name = taskIDFor(i);
      return new SoakNode({
        name,
        env: viewSyncerEnv(config, i),
        logsDir: config.logsDir,
        log,
        replicaFile: replicaFile(config, name),
      });
    });
  }

  get nodes(): SoakNode[] {
    return [this.rm, ...this.viewSyncers];
  }

  get changeLogFile(): string {
    return changeLogFile(this.#config, this.rm.name);
  }

  /** The change-log settings the replication-manager is currently running. */
  get rmSettings(): ChangeLogSettings {
    return {...this.#config.changeLog, ...this.#rmSettings};
  }

  node(name: string): SoakNode {
    const node = this.nodes.find(n => n.name === name);
    if (!node) {
      throw new Error(`no such node: ${name}`);
    }
    return node;
  }

  replicaHandles(): ReplicaHandle[] {
    return this.nodes.map(n => ({node: n.name, replicaFile: n.replicaFile}));
  }

  async startReplicationManager(
    overrides?: Partial<ChangeLogSettings>,
  ): Promise<void> {
    if (overrides) {
      this.#rmSettings = overrides;
    }
    // The change-log mode is a startup option, so both the flip and the
    // rollback are restarts. Rebuild the environment on every start rather
    // than caching it.
    this.rm.env = replicationManagerEnv(this.#config, this.#rmSettings);
    await this.rm.start();
  }

  async startViewSyncers(): Promise<void> {
    // Serially, so that the reservation/restore sequence of each is legible
    // in the logs and the first one does not race the RM's first backup.
    for (const vs of this.viewSyncers) {
      await vs.start();
    }
  }

  async stopAll(signal: NodeJS.Signals = 'SIGTERM'): Promise<void> {
    await Promise.all(this.viewSyncers.map(vs => vs.stop(signal)));
    await this.rm.stop(signal);
  }
}
