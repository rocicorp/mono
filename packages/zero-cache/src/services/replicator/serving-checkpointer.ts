import type {LogContext} from '@rocicorp/logger';
import {Database} from '../../../../zqlite/src/db.ts';
import {LogThrottle} from '../../observability/log-throttle.ts';
import {RunningState} from '../running-state.ts';
import type {Service} from '../service.ts';

const CHECKPOINT_INTERVAL_MS = 100;

// A checkpoint copies at most one wal file (~4 MB with the default 1000 page
// limit and 4 KB pages), so only a disk that is struggling makes one slow.
const SLOW_CHECKPOINT_MS = 1000;

// The wal files normally hold about two files' worth of frames (~8 MB). A wal
// several times that is not being checkpointed as fast as it is written.
const WAL_WARNING_BYTES = 64 * 1024 * 1024;

const LOG_WINDOW_MS = 60 * 1000;

const MB = 1024 * 1024;

type Options = {
  intervalMs?: number | undefined;
  walWarningBytes?: number | undefined;
};

/**
 * Checkpoints the serving (wal2) replica from its own connection, so that the
 * replicator's writer, which runs with `wal_autocheckpoint = 0`, never waits
 * on a checkpoint's disk syncs.
 *
 * In wal2 mode the writer switches to the other wal file once the current
 * one reaches its size limit, provided the other one has been checkpointed.
 * Until then, it keeps appending to the current file rather than waiting. A
 * `PASSIVE` checkpoint copies the whole non-current file, or does nothing
 * (without any I/O) if that file is already checkpointed, empty, or still
 * being read (e.g. by an old snapshot). Polling it on a short interval thus
 * keeps the wal files near their size limit without any coordination with
 * the writer.
 *
 * A failed checkpoint is logged and retried rather than stopping the
 * replicator. This matches the writer's automatic checkpoints that this
 * replaces, whose errors SQLite ignores: a persistent failure (e.g. a full or
 * failing disk) surfaces as the writer's own failure once the wal can no
 * longer grow.
 */
export class ServingCheckpointer implements Service {
  readonly id = 'serving-checkpointer';
  readonly #lc: LogContext;
  readonly #db: Database;
  readonly #pageSize: number;
  readonly #intervalMs: number;
  readonly #walWarningBytes: number;
  readonly #state = new RunningState(this.id);
  readonly #logThrottle = new LogThrottle({windowMs: LOG_WINDOW_MS});

  constructor(lc: LogContext, dbPath: string, opts: Options = {}) {
    this.#lc = lc.withContext('component', this.id);
    this.#db = new Database(
      this.#lc,
      dbPath,
      {fileMustExist: true},
      SLOW_CHECKPOINT_MS,
    );
    [{page_size: this.#pageSize}] = this.#db.pragma<{page_size: number}>(
      'page_size',
    );
    this.#intervalMs = opts.intervalMs ?? CHECKPOINT_INTERVAL_MS;
    this.#walWarningBytes = opts.walWarningBytes ?? WAL_WARNING_BYTES;
  }

  async run(): Promise<void> {
    try {
      while (this.#state.shouldRun()) {
        this.#checkpoint();
        await this.#state.sleep(this.#intervalMs);
      }
    } finally {
      this.#db.close();
    }
  }

  #checkpoint() {
    let result;
    try {
      [result] = this.#db.pragma<{busy: number; log: number}>(
        'wal_checkpoint(PASSIVE)',
      );
    } catch (e) {
      const suppressed = this.#logThrottle.admit('error');
      if (suppressed !== undefined) {
        this.#lc.error?.(
          `error checkpointing the replica` +
            (suppressed ? ` (${suppressed} more since last logged)` : ''),
          e,
        );
      }
      return;
    }

    // In wal2 mode `log` counts the frames in both wal files, and `busy` is
    // set if a reader prevented the checkpoint.
    const {busy, log} = result;
    const walBytes = log * this.#pageSize;
    if (walBytes >= this.#walWarningBytes) {
      const reason = busy
        ? 'a reader (e.g. a long-running snapshot) is still reading the older wal file'
        : 'checkpoints are not keeping up with writes';
      const suppressed = this.#logThrottle.admit(busy ? 'busy' : 'behind');
      if (suppressed !== undefined) {
        this.#lc.warn?.(
          `replica wal is ${(walBytes / MB).toFixed(1)} MB: ${reason}`,
          {walBytes, busy: !!busy, suppressed},
        );
      }
    }
  }

  stop(): Promise<void> {
    this.#state.stop(this.#lc);
    return Promise.resolve();
  }
}
