import {assert} from 'console';
import {clearInterval} from 'timers';
import type {LogContext} from '@rocicorp/logger';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import {getReplicationState} from '../replicator/schema/replication-state.ts';
import type {BackedUpWatermark} from './backup-monitor.ts';

const CHECK_INTERVAL_MS = 10 * 1000;

/**
 * The single-node equivalent of a backup monitor polls the replica file every
 * 30 seconds and schedules cleanup when the watermark
 * (i.e. stateVersion) moves forward.
 */
export class ReplicaPoller {
  readonly id = 'replica-poller';
  readonly #lc: LogContext;
  readonly #replicaFile: string;
  readonly #stream: Subscription<BackedUpWatermark>;
  #pollReplicaTimer: NodeJS.Timeout | undefined;

  #lastWatermark: string = '';

  constructor(lc: LogContext, replicaFile: string) {
    this.#lc = lc.withContext('component', this.id);
    this.#replicaFile = replicaFile;
    this.#stream = Subscription.create<BackedUpWatermark>({
      cleanup: () => clearInterval(this.#pollReplicaTimer),
    });
  }

  start(): Source<BackedUpWatermark> {
    assert(this.#pollReplicaTimer === undefined, `Already called start()`);
    this.#lc.info?.(`starting replica monitor`);
    // Perform the first poll immediately to avoid unnecessarily waiting
    // during single-node development.
    this.#pollReplica();
    this.#pollReplicaTimer = setInterval(this.#pollReplica, CHECK_INTERVAL_MS);
    return this.#stream;
  }

  readonly #pollReplica = () => {
    const db = new Database(this.#lc, this.#replicaFile);
    try {
      const {stateVersion} = getReplicationState(new StatementRunner(db));
      if (stateVersion !== this.#lastWatermark) {
        this.#lastWatermark = stateVersion;
        this.#lc.debug?.(`replicated up to watermark ${stateVersion}`);
        this.#stream.push({watermark: stateVersion, backupTimeMs: Date.now()});
      }
    } catch (e) {
      this.#lc.error?.(`Unable to read watermark from replica`, e);
    } finally {
      db.close();
    }
  };
}
