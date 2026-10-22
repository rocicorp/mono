import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {getOrCreateGauge} from '../../observability/metrics.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import type {SingletonService} from '../service.ts';

export type BackedUpWatermark = {
  watermark: string;
  writeTimeMs?: number | undefined; // optional for debugging; available on v5
  backupTimeMs: number;
};

/**
 * Determines which watermarks of the replica are backed up, by whatever means
 * the backup (or its absence) allows.
 */
export interface BackupPoller {
  /**
   * Starts polling, returning the stream of backed-up watermarks. Must only
   * be called once.
   */
  start(): Source<BackedUpWatermark>;

  /**
   * Requests that the replica be backed up through `watermark` promptly,
   * rather than at the poller's regular cadence, once the replica has reached
   * it. The resulting watermark is emitted on the stream as usual. Pollers
   * that cannot initiate backups do not implement this.
   */
  requestSync?(watermark: string): void;
}

/**
 * The interface through which the change-streamer consumes backed-up
 * watermarks and requests prompt backups (see {@link BackupMonitor}).
 */
export type BackupSource = Pick<BackupMonitor, 'backups' | 'requestSync'>;

/**
 * Relays the watermarks backed up by a {@link BackupPoller} to the
 * change-streamer, which consumes them (see {@link backups}) and requests
 * prompt backups (see {@link requestSync}) as needed.
 */
export class BackupMonitor implements SingletonService {
  readonly id = 'backup-monitor';
  readonly #lc: LogContext;
  readonly #poller: BackupPoller;
  readonly #replicaFile: string;
  readonly #firstBackupReceived = resolver();
  // Backed-up watermarks only move forward, so the consumer only needs the
  // latest one. Coalescing keeps at most one outstanding, so that the monitor
  // neither waits on, nor queues for, the consumer.
  readonly #backups = Subscription.create<BackedUpWatermark>({
    coalesce: curr => curr,
  });

  #watermarks: Source<BackedUpWatermark> | undefined;
  #stopped = false;
  #latestBackup: BackedUpWatermark | undefined;
  #firstBackupResolved = false;
  /**
   * The replica's `stateVersion` when the monitor started, i.e. the point the
   * backup has to reach before it covers what this task is about to serve.
   * `undefined` when the replica could not be read, which degrades the gate to
   * "any watermark" rather than hanging startup forever.
   */
  #coverageTarget: string | undefined;

  constructor(lc: LogContext, poller: BackupPoller, replicaFile: string) {
    this.#lc = lc;
    this.#poller = poller;
    this.#replicaFile = replicaFile;
  }

  firstBackupReceived() {
    return this.#firstBackupReceived.promise;
  }

  /**
   * The stream of backed-up watermarks, each later than the last, coalesced
   * to the latest one that has not been consumed. It ends when the monitor
   * stops.
   */
  backups(): Source<BackedUpWatermark> {
    return this.#backups;
  }

  /**
   * Requests that the replica be backed up through `watermark` promptly, if
   * the poller supports it (see {@link BackupPoller.requestSync}). A no-op
   * unless the monitor is running.
   */
  requestSync(watermark: string) {
    if (
      !this.#watermarks ||
      this.#stopped ||
      (this.#latestBackup && this.#latestBackup.watermark >= watermark)
    ) {
      return;
    }
    if (this.#poller.requestSync) {
      this.#lc.info?.(`requesting backup through ${watermark}`);
      this.#poller.requestSync(watermark);
    }
  }

  async run() {
    if (this.#stopped) {
      return;
    }
    this.#lc.info?.('starting backup monitor');
    this.#initBackupLagMetric();
    this.#coverageTarget = this.#readReplicaStateVersion();

    this.#watermarks = this.#poller.start();
    try {
      for await (const backedUp of this.#watermarks) {
        if (this.#latestBackup) {
          if (backedUp.watermark < this.#latestBackup.watermark) {
            this.#lc.warn?.(
              `ignoring earlier backup watermark ${backedUp.watermark}`,
              {backedUp},
            );
            continue;
          }
          if (backedUp.watermark === this.#latestBackup.watermark) {
            this.#lc.debug?.(
              `ignoring redundant backup watermark ${backedUp.watermark}`,
              {backedUp},
            );
            continue;
          }
        }
        this.#lc.info?.(`received backup watermark ${backedUp.watermark}`, {
          backedUp,
        });
        this.#latestBackup = backedUp;
        this.#backups.push(backedUp);
        this.#checkFirstBackupCovers(backedUp);
      }
      this.#lc.info?.('watermark stream closed. BackupMonitor stopped.');
    } finally {
      this.#backups.cancel();
    }
  }

  /**
   * Resolves {@link firstBackupReceived} once the backup actually covers the
   * replica, rather than on the first watermark of any value.
   *
   * A new backup destination starts empty and is filled by re-uploading the
   * local LTX chain, so the first watermarks read back out of it are real but
   * far behind the replica. Releasing the readiness gate on one of those lets
   * the task serve against a backup that does not yet cover it, which is what
   * demotes a restoring view-syncer to Postgres catchup.
   *
   * The target is pinned at startup rather than compared against the replica's
   * current position, which keeps the gate satisfiable under sustained writes.
   */
  #checkFirstBackupCovers(backedUp: BackedUpWatermark) {
    if (this.#firstBackupResolved) {
      return;
    }
    const target = this.#coverageTarget;
    if (target !== undefined && backedUp.watermark < target) {
      this.#lc.info?.(`backup does not yet cover the replica`, {
        backedUp,
        coverageTarget: target,
      });
      return;
    }
    this.#firstBackupResolved = true;
    this.#firstBackupReceived.resolve();
  }

  #readReplicaStateVersion(): string | undefined {
    let db;
    try {
      db = new Database(this.#lc, this.#replicaFile, {readonly: true});
      const {stateVersion} = db
        .prepare(/*sql*/ `SELECT stateVersion FROM "_zero.replicationState"`)
        .get<{stateVersion: string}>();
      return stateVersion;
    } catch (e) {
      // Without a target the gate degrades to its previous behavior. That is
      // strictly better than blocking startup on a replica we cannot read.
      this.#lc.warn?.(
        `unable to read the replica's stateVersion; ` +
          `the initial backup gate will accept the first watermark`,
        e,
      );
      return undefined;
    } finally {
      db?.close();
    }
  }

  stop(): Promise<void> {
    this.#stopped = true;
    this.#watermarks?.cancel();
    this.#backups.cancel();
    return promiseVoid;
  }

  #initBackupLagMetric() {
    getOrCreateGauge('replica', 'backup_lag', {
      description:
        'Latency from when a change is written to the replica ' +
        'to when it is backed up to litestream. It is expected to create a saw ' +
        'pattern from 0 to the configured ZERO_LITESTREAM_INCREMENTAL_BACKUP_INTERVAL_MINUTES.',
      unit: 'millisecond',
    }).addCallback(o => {
      const latestBackup = this.#latestBackup;
      if (!latestBackup) {
        this.#lc.warn?.(
          `no backed up watermarks. unable to report replica.backup_lag`,
        );
        return;
      }
      const db = new Database(this.#lc, this.#replicaFile, {readonly: true});
      try {
        const {writeTimeMs} = db
          .prepare(/*sql*/ `SELECT writeTimeMs FROM "_zero.replicationState"`)
          .get<{writeTimeMs: number}>();
        const backupLag = Math.max(0, writeTimeMs - latestBackup.backupTimeMs);
        o.observe(backupLag);
      } catch (e) {
        this.#lc.warn?.(`error measuring replica.backup_lag metric`, e);
      } finally {
        db.close();
      }
    });
  }
}
