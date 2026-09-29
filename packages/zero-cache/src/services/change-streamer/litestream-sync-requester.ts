import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import * as v from '../../../../shared/src/valita.ts';
import {Database, type Statement} from '../../../../zqlite/src/db.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import {LitestreamController} from '../litestream/litestream-controller.ts';
import {RunningState} from '../running-state.ts';
import type {BackedUpWatermark} from './backup-monitor.ts';

/**
 * How long litestream waits for a requested sync (and its upload) before
 * ending the request. A request that times out is simply retried on a later
 * tick: litestream only abandons the *wait*, never the sync or upload itself,
 * so this bounds request lifetime without affecting backup progress.
 */
const SERVER_TIMEOUT_SECONDS = 60;

/**
 * Client-side timeout, slightly above the server timeout so that litestream,
 * rather than the client, ends a slow request.
 */
const CLIENT_TIMEOUT_MS = (SERVER_TIMEOUT_SECONDS + 5) * 1000;

const ERROR_LOG_INTERVAL_MS = 60_000;

const localWatermarkSchema = v.object({
  stateVersion: v.string(),
  writeTimeMs: v.number(),
});

type LocalWatermark = v.Infer<typeof localWatermarkSchema>;

/** The subset of {@link LitestreamController} used by the requester. */
export type SyncController = Pick<LitestreamController, 'sync' | 'close'>;

export type SyncRequesterConfig = {
  /**
   * Interval between sync requests. This paces how often litestream seals
   * (and uploads) a new LTX file.
   */
  intervalMs: number;
};

/**
 * The LitestreamSyncRequester drives litestream v5 backups and tracks which
 * replica watermark is durably backed up, emitting it as a stream of
 * {@link BackedUpWatermark}s.
 *
 * It periodically reads the replica's current watermark `W` and then asks
 * litestream to sync and wait for the upload (`POST /sync {wait: true}`).
 * litestream satisfies the request with a sync pass that *starts after* the
 * request, and a pass syncs up to the end of the WAL, so a successful response
 * means that every transaction committed before the request, including the
 * one that wrote `W`, has been uploaded to the backup. No part of the backup
 * needs to be read to determine this.
 *
 * Requests continue even when `W` is already backed up, so that changes that
 * don't advance the watermark (e.g. change-log cleanup or `PRAGMA optimize`)
 * are also backed up at this cadence rather than only at litestream's slower
 * backstop interval. A request on an idle replica is a local no-op for
 * litestream: there is nothing to seal or upload, and no backup reads.
 *
 * At most one request is outstanding at a time. Failed or timed-out requests
 * are retried on the next tick, and nothing is published until one succeeds.
 */
export class LitestreamSyncRequester {
  readonly #lc: LogContext;
  readonly #state = new RunningState('litestream-sync-requester');
  readonly #config: SyncRequesterConfig;
  readonly #litestream: SyncController;
  readonly #readLocalWatermark: Statement;
  readonly #stream: Subscription<BackedUpWatermark>;

  #timer: NodeJS.Timeout | undefined;
  #inFlight: Promise<void> | undefined;
  #backedUp: string | undefined;
  #lastErrorLogMs = 0;

  constructor(
    lc: LogContext,
    replicaFile: string,
    config: SyncRequesterConfig,
    litestream: SyncController = new LitestreamController(lc, replicaFile),
  ) {
    this.#lc = lc.withContext('component', 'litestream-sync-requester');
    this.#config = config;
    this.#litestream = litestream;

    const db = new Database(this.#lc, replicaFile, {readonly: true});
    this.#readLocalWatermark = db.prepare(
      `SELECT stateVersion, writeTimeMs FROM "_zero.replicationState"`,
    );
    this.#stream = Subscription.create<BackedUpWatermark>({
      cleanup: () => {
        clearInterval(this.#timer);
        this.#state.stop(this.#lc);
        this.#litestream.close();
        db.close();
      },
    });
  }

  /**
   * Starts requesting syncs and pushes backed-up watermarks to the returned
   * stream. Must only be called once.
   */
  start(): Source<BackedUpWatermark> {
    assert(this.#timer === undefined, `Already called start()`);
    this.#lc.info?.(
      `requesting litestream syncs every ${this.#config.intervalMs} ms`,
    );
    this.#timer = setInterval(this.tick, this.#config.intervalMs);
    void this.tick();
    return this.#stream;
  }

  /**
   * Requests a sync unless a request is already outstanding. Resolves when
   * this tick's request (or the outstanding one) completes.
   * Exported for testing.
   */
  readonly tick = (): Promise<void> => {
    if (!this.#state.shouldRun() || this.#inFlight) {
      return this.#inFlight ?? Promise.resolve();
    }
    let local: LocalWatermark;
    try {
      local = v.parse(this.#readLocalWatermark.get(), localWatermarkSchema);
    } catch (e) {
      this.#lc.warn?.(`unable to read local watermark`, e);
      return Promise.resolve();
    }
    this.#inFlight = this.#requestSync(local).finally(() => {
      this.#inFlight = undefined;
    });
    return this.#inFlight;
  };

  async #requestSync(local: LocalWatermark) {
    // `local` must be read before the request is issued: litestream only
    // guarantees that transactions committed before the request are synced.
    try {
      const result = await this.#litestream.sync(
        {
          wait: true,
          timeoutMs: CLIENT_TIMEOUT_MS,
          serverTimeoutSeconds: SERVER_TIMEOUT_SECONDS,
        },
        this.#state.signal,
      );
      this.#publish(local, Date.now());
      this.#lc.debug?.(`backed up watermark ${local.stateVersion}`, result);
    } catch (e) {
      if (!this.#state.shouldRun()) {
        return; // shutting down
      }
      // Expected while litestream is starting up or busy with a long sync or
      // upload (e.g. the initial snapshot of a large replica); the request is
      // retried on the next tick.
      const now = Date.now();
      if (now - this.#lastErrorLogMs >= ERROR_LOG_INTERVAL_MS) {
        this.#lastErrorLogMs = now;
        this.#lc.info?.(
          `backup of watermark ${local.stateVersion} not yet confirmed. retrying`,
          e,
        );
      }
    }
  }

  #publish({stateVersion, writeTimeMs}: LocalWatermark, backupTimeMs: number) {
    if (this.#backedUp !== undefined && stateVersion <= this.#backedUp) {
      return;
    }
    this.#backedUp = stateVersion;
    this.#stream.push({watermark: stateVersion, writeTimeMs, backupTimeMs});
  }
}
