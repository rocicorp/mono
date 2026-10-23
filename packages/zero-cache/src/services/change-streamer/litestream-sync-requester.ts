import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import {must} from '../../../../shared/src/must.ts';
import * as v from '../../../../shared/src/valita.ts';
import {Database, type Statement} from '../../../../zqlite/src/db.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import {LitestreamController} from '../litestream/litestream-controller.ts';
import {RunningState} from '../running-state.ts';
import type {BackedUpWatermark, BackupPoller} from './backup-monitor.ts';

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

/**
 * Initial delay before retrying a failed request, doubled on each consecutive
 * failure until it reaches the configured interval, after which the regular
 * ticks alone retry, so that a busy litestream sees no more requests than the
 * interval allows. The first request is typically issued before litestream's
 * control socket is listening; retrying quickly (rather than after a full
 * interval) keeps the initial backup confirmation, which gates readiness, from
 * waiting out an interval.
 */
const MIN_RETRY_DELAY_MS = 250;

/**
 * How often the local replica is polled while a requested sync (see
 * {@link LitestreamSyncRequester.requestSync}) waits for the replica to reach
 * the requested watermark. Each poll is a single read of the replica's
 * watermark, which the replica typically reaches within a poll or two.
 */
export const EXPEDITED_POLL_INTERVAL_MS = 50;

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
 * are retried with a backoff that starts short and, once it grows to the
 * interval, defers to the regular ticks. Nothing is published until a request
 * succeeds.
 *
 * A sync through a specific watermark can also be requested out of band (see
 * {@link requestSync}), e.g. when a replica fork waits on its backup.
 */
export class LitestreamSyncRequester implements BackupPoller {
  readonly #lc: LogContext;
  readonly #state = new RunningState('litestream-sync-requester');
  readonly #config: SyncRequesterConfig;
  readonly #litestream: SyncController;
  readonly #replicaFile: string;
  readonly #stream: Subscription<BackedUpWatermark>;

  #db: Database | undefined;
  #readLocalWatermark: Statement | undefined;
  #timer: NodeJS.Timeout | undefined;
  #retryTimer: NodeJS.Timeout | undefined;
  // The target of a requested sync, and when it stops being expedited.
  #expedited: {target: string; deadlineMs: number} | undefined;
  #expediteTimer: NodeJS.Timeout | undefined;
  #retryDelayMs = MIN_RETRY_DELAY_MS;
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
    this.#replicaFile = replicaFile;
    this.#stream = Subscription.create<BackedUpWatermark>({
      cleanup: () => {
        clearInterval(this.#timer);
        clearTimeout(this.#retryTimer);
        clearTimeout(this.#expediteTimer);
        this.#state.stop(this.#lc);
        this.#litestream.close();
        this.#db?.close();
      },
    });
  }

  /**
   * Starts requesting syncs and pushes backed-up watermarks to the returned
   * stream. Must only be called once.
   *
   * The replica is only opened here, so that a requester that is never
   * started holds no resources.
   */
  start(): Source<BackedUpWatermark> {
    assert(this.#timer === undefined, `Already called start()`);
    this.#db = new Database(this.#lc, this.#replicaFile, {readonly: true});
    this.#readLocalWatermark = this.#db.prepare(
      `SELECT stateVersion, writeTimeMs FROM "_zero.replicationState"`,
    );
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
    const local = this.#readLocal();
    if (!local) {
      return Promise.resolve();
    }
    this.#inFlight = this.#requestSync(local).finally(() => {
      this.#inFlight = undefined;
    });
    return this.#inFlight;
  };

  #readLocal(): LocalWatermark | undefined {
    try {
      return v.parse(
        must(this.#readLocalWatermark, `start() was not called`).get(),
        localWatermarkSchema,
      );
    } catch (e) {
      this.#lc.warn?.(`unable to read local watermark`, e);
      return undefined;
    }
  }

  /**
   * Requests a sync through `watermark` as soon as the local replica has
   * reached it, rather than at the next regular tick.
   *
   * The replica is polled every {@link EXPEDITED_POLL_INTERVAL_MS} until its
   * watermark reaches the target, after which a sync is requested
   * immediately (or once the outstanding request, which may predate the
   * target, completes). A failed request is retried with the usual backoff.
   * Concurrent requests are coalesced into one, for the latest target.
   *
   * If the replica does not reach the target within the configured interval,
   * the request is dropped, as the regular ticks then cover it anyway.
   */
  requestSync(watermark: string) {
    if (
      !this.#state.shouldRun() ||
      (this.#backedUp !== undefined && this.#backedUp >= watermark)
    ) {
      return;
    }
    const prev = this.#expedited?.target;
    this.#expedited = {
      target: prev !== undefined && prev > watermark ? prev : watermark,
      deadlineMs: Date.now() + this.#config.intervalMs,
    };
    this.#scheduleExpedite(0);
  }

  #scheduleExpedite(delayMs: number) {
    clearTimeout(this.#expediteTimer);
    this.#expediteTimer = setTimeout(this.#expedite, delayMs);
  }

  readonly #expedite = () => {
    this.#expediteTimer = undefined;
    const expedited = this.#expedited;
    if (!expedited || !this.#state.shouldRun()) {
      return;
    }
    const {target, deadlineMs} = expedited;
    if (this.#backedUp !== undefined && this.#backedUp >= target) {
      this.#expedited = undefined; // e.g. by a regular tick
      return;
    }
    if (Date.now() >= deadlineMs) {
      this.#lc.info?.(
        `replica did not reach ${target} within ${this.#config.intervalMs} ms. ` +
          `Deferring its backup to the regular syncs.`,
      );
      this.#expedited = undefined;
      return;
    }
    if (this.#inFlight) {
      // The outstanding request may have read the local watermark before the
      // replica reached the target. Check again once it completes.
      void this.#inFlight.finally(() => this.#scheduleExpedite(0));
      return;
    }
    const local = this.#readLocal();
    if (!local || local.stateVersion < target) {
      this.#scheduleExpedite(EXPEDITED_POLL_INTERVAL_MS);
      return;
    }
    // A single request covers the target. If it fails, the usual backoff
    // retries it.
    this.#expedited = undefined;
    this.#lc.info?.(`requesting sync through ${target}`);
    void this.tick();
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
      this.#retryDelayMs = MIN_RETRY_DELAY_MS;
      this.#publish(local, Date.now());
      this.#lc.debug?.(`backed up watermark ${local.stateVersion}`, result);
    } catch (e) {
      if (!this.#state.shouldRun()) {
        return; // shutting down
      }
      // Expected while litestream is starting up or busy with a long sync or
      // upload (e.g. the initial snapshot of a large replica); the request is
      // retried after a backoff, or on the next tick if that comes first.
      this.#scheduleRetry();
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

  #scheduleRetry() {
    clearTimeout(this.#retryTimer);
    const delayMs = this.#retryDelayMs;
    if (delayMs >= this.#config.intervalMs) {
      return; // backed off to the interval; the regular ticks retry
    }
    this.#retryDelayMs = delayMs * 2;
    this.#retryTimer = setTimeout(this.tick, delayMs);
  }

  #publish({stateVersion, writeTimeMs}: LocalWatermark, backupTimeMs: number) {
    if (this.#backedUp !== undefined && stateVersion <= this.#backedUp) {
      return;
    }
    this.#backedUp = stateVersion;
    this.#stream.push({watermark: stateVersion, writeTimeMs, backupTimeMs});
  }
}
