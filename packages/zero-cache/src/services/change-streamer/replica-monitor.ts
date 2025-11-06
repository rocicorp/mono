import type {LogContext} from '@rocicorp/logger';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {getReplicationState} from '../replicator/schema/replication-state.ts';
import {RunningState} from '../running-state.ts';
import type {Service} from '../service.ts';
import type {ChangeStreamerService} from './change-streamer.ts';

const CHECK_INTERVAL_MS = 30 * 1000;

/**
 * The single-node equivalent of the {@link BackupMonitor} polls the replica
 * file every 30 seconds and schedules cleanup when the watermark
 * (i.e. stateVersion) moves forward.
 */
export class ReplicaMonitor implements Service {
  readonly id = 'replica-monitor';
  readonly #lc: LogContext;
  readonly #replicaFile: string;
  readonly #changeStreamer: ChangeStreamerService;
  readonly #state = new RunningState(this.id);

  #lastWatermark: string = '';

  constructor(
    lc: LogContext,
    replicaFile: string,
    changeStreamer: ChangeStreamerService,
  ) {
    this.#lc = lc.withContext('component', this.id);
    this.#replicaFile = replicaFile;
    this.#changeStreamer = changeStreamer;
  }

  async run() {
    this.#lc.info?.(`starting replica monitor`);
    await this.#state.sleep(CHECK_INTERVAL_MS);

    while (this.#state.shouldRun()) {
      const db = new Database(this.#lc, this.#replicaFile);
      try {
        const {stateVersion} = getReplicationState(new StatementRunner(db));
        if (stateVersion !== this.#lastWatermark) {
          this.#lastWatermark = stateVersion;
          this.#lc.debug?.(`replicated up to watermark ${stateVersion}`);
          this.#changeStreamer.scheduleCleanup(stateVersion);
        }
      } catch (e) {
        this.#lc.error?.(`Unable to read watermark from replica`, e);
      } finally {
        db.close();
      }

      await this.#state.sleep(CHECK_INTERVAL_MS);
    }
    this.#lc.info?.(`replica monitor stopped`);
  }

  stop() {
    this.#state.stop(this.#lc);
    return promiseVoid;
  }
}
