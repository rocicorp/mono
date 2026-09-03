import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {getOrCreateGauge} from '../../observability/metrics.ts';
import type {Source} from '../../types/streams.ts';
import type {SingletonService} from '../service.ts';
import type {ChangeStreamerService} from './change-streamer.ts';

export type BackedUpWatermark = {
  watermark: string;
  writeTimeMs?: number | undefined; // optional for debuggin; available on v5
  backupTimeMs: number;
};

export class BackupMonitor implements SingletonService {
  readonly id = 'backup-monitor';
  readonly #lc: LogContext;
  readonly #watermarks: Source<BackedUpWatermark>;
  readonly #changeStreamer: ChangeStreamerService;
  readonly #replicaFile: string;
  readonly #firstBackupReceived = resolver();

  #latestBackup: BackedUpWatermark | undefined;
  #firstBackupResolved = false;
  /**
   * The replica's `stateVersion` when the monitor started, i.e. the point the
   * backup has to reach before it covers what this task is about to serve.
   * `undefined` when the replica could not be read, which degrades the gate to
   * "any watermark" rather than hanging startup forever.
   */
  #coverageTarget: string | undefined;

  constructor(
    lc: LogContext,
    watermarks: Source<BackedUpWatermark>,
    changeStreamer: ChangeStreamerService,
    replicaFile: string,
  ) {
    this.#lc = lc;
    this.#watermarks = watermarks;
    this.#changeStreamer = changeStreamer;
    this.#replicaFile = replicaFile;
  }

  firstBackupReceived() {
    return this.#firstBackupReceived.promise;
  }

  async run() {
    this.#lc.info?.('starting backup monitor');
    this.#initBackupLagMetric();
    this.#coverageTarget = this.#readReplicaStateVersion();

    for await (const backedUp of this.#watermarks) {
      if (this.#latestBackup) {
        if (backedUp.watermark < this.#latestBackup.watermark) {
          this.#lc.warn?.(`ignoring earlier backup watermark`, {backedUp});
          continue;
        }
        if (backedUp.watermark === this.#latestBackup.watermark) {
          this.#lc.debug?.(`ignoring redundant backup watermark`, {backedUp});
          continue;
        }
      }
      this.#lc.info?.(`received backup watermark`, {backedUp});
      this.#latestBackup = backedUp;
      this.#changeStreamer.trackBackupWatermark(backedUp.watermark);
      this.#checkFirstBackupCovers(backedUp);
    }
    this.#lc.info?.('watermark stream closed. BackupMonitor stopped.');
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
    this.#watermarks.cancel();
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
