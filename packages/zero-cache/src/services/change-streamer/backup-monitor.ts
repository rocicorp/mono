import type {LogContext} from '@rocicorp/logger';
import parsePrometheusTextFormat from 'parse-prometheus-text-format';
import {must} from '../../../../shared/src/must.ts';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {
  getOrCreateCounter,
  getOrCreateGauge,
} from '../../observability/metrics.ts';
import {Subscription} from '../../types/subscription.ts';
import {RunningState} from '../running-state.ts';
import type {Service} from '../service.ts';
import type {ChangeStreamerService} from './change-streamer.ts';
import type {SnapshotMessage} from './snapshot.ts';

export const CHECK_INTERVAL_MS = 60_000;
const MIN_CLEANUP_DELAY_MS = 30_000;

/**
 * Allowance for clock skew between the machine reporting litestream metrics
 * and the timestamps reported by the backup destination (e.g. S3).
 */
export const BACKUP_VERIFICATION_SLACK_MS = 60_000;

/**
 * Returns the time of the most recent object actually uploaded to the
 * backup replica destination (e.g. as determined by listing the snapshots
 * and WAL segments in S3). Rejects if the backup state cannot be determined.
 *
 * See `getLastBackupTime()` in `../litestream/commands.ts` for the
 * production implementation.
 */
export type BackupStateVerifier = () => Promise<Date>;

type Reservation = {
  start: Date;
  sub: Subscription<SnapshotMessage>;
};

/**
 * The BackupMonitor polls the litestream "/metrics" endpoint to track the
 * watermark (label) value of the `litestream_replica_progress` gauge and
 * schedules cleanup of change log entries that can be purged as a result.
 *
 * See: https://github.com/rocicorp/litestream/pull/3
 *
 * Note that change log entries cannot simply be purged as soon as they
 * have been applied and backed up by litestream. Consider the case in which
 * litestream backs up new wal segments every minute, but it takes 5 minutes
 * to restore a replica: if a zero-cache starts restoring a replica at
 * minute 0, and new watermarks are replicated at minutes 1, 2, 3, 4, and 5,
 * purging changelog records as soon as those watermarks are replicated would
 * result in the zero-cache not being able to catch up from minute 0 once it
 * has finished restoring the replica.
 *
 * The `/snapshot` reservation protocol is used to prevent premature change
 * log cleanup:
 * - Clients restoring a snapshot initiate a `/snapshot` request and hold that
 *   request open while it restores its snapshot, prepares it, and
 *   starts its subscription to the change stream. During this time, no
 *   cleanups are scheduled.
 * - When the subscription is started, the interval since the beginning of
 *   of the reservation is tracked to increase the background cleanup delay
 *   interval if needed. The reservation is ended (and request closed), and
 *   cleanup scheduling is resumed with the current delay interval.
 *
 * Note that the reservation request is the primary mechanism by which
 * premature change log cleanup is prevented. The cleanup delay interval is
 * a secondary safeguard.
 *
 * Additionally, because the watermarks reported by litestream metrics
 * reflect what litestream *believes* has been backed up (which has been
 * observed to diverge from reality when uploads silently fail), the cleanup
 * watermark is only advanced after verifying it against the actual backup
 * state in the replica destination via a {@link BackupStateVerifier}.
 */
export class BackupMonitor implements Service {
  readonly id = 'backup-monitor';
  readonly #lc: LogContext;
  readonly #replicaFile: string;
  readonly #backupURL: string;
  readonly #metricsEndpoint: string;
  readonly #changeStreamer: ChangeStreamerService;
  readonly #state = new RunningState(this.id);

  readonly #reservations = new Map<string, Reservation>();
  readonly #watermarks = new Map<string, Date>();

  readonly #verifyBackupState: BackupStateVerifier;
  readonly #purgesBlocked = getOrCreateCounter('replica', 'purge_blocked', {
    description:
      'Number of change-log purges blocked because the actual backup state ' +
      '(as listed from the replica destination) could not be verified, or ' +
      'is older than the backup progress claimed by litestream metrics. ' +
      'A steadily increasing value indicates a wedged or failing backup.',
  });

  #lastWatermark: string = '';
  #latestBackupTime: Date | null = null;
  #lastVerifiedUploadTime: Date | null = null;
  #cleanupDelayMs: number;
  #checkMetricsTimer: NodeJS.Timeout | undefined;

  constructor(
    lc: LogContext,
    replicaFile: string,
    backupURL: string,
    metricsEndpoint: string,
    changeStreamer: ChangeStreamerService,
    initialCleanupDelayMs: number,
    verifyBackupState: BackupStateVerifier,
  ) {
    this.#lc = lc.withContext('component', this.id);
    this.#replicaFile = replicaFile;
    this.#backupURL = backupURL;
    this.#metricsEndpoint = metricsEndpoint;
    this.#changeStreamer = changeStreamer;
    this.#verifyBackupState = verifyBackupState;
    this.#cleanupDelayMs = Math.max(
      initialCleanupDelayMs,
      MIN_CLEANUP_DELAY_MS, // purely for peace of mind
    );

    this.#lc.info?.(
      `backup monitor started ${initialCleanupDelayMs} ms after snapshot restore`,
    );
  }

  run(): Promise<void> {
    this.#lc.info?.(
      `monitoring backups at ${this.#metricsEndpoint} with ` +
        `${this.#cleanupDelayMs} ms cleanup delay`,
    );
    this.#checkMetricsTimer = setInterval(
      this.checkWatermarksAndScheduleCleanup,
      CHECK_INTERVAL_MS,
    );
    this.#initBackupLagMetric();
    return this.#state.stopped();
  }

  startSnapshotReservation(taskID: string): Subscription<SnapshotMessage> {
    this.#lc.info?.(`pausing change-log cleanup while ${taskID} snapshots`);
    // In the case of retries, only track the last reservation.
    this.#reservations.get(taskID)?.sub.cancel();

    const sub = Subscription.create<SnapshotMessage>({
      // If the reservation still exists when the connection closes
      // (e.g. subscriber crashed), clean it up without updating the
      // cleanup delay.
      cleanup: () => this.endReservation(taskID, false),
    });
    this.#reservations.set(taskID, {start: new Date(), sub});
    // Note: the Subscription must be returned immediately so that the
    //       websocket can begin sending liveness pings.
    void this.#changeStreamer
      .getChangeLogState()
      .then(changeLogState => {
        sub.push([
          'status',
          {tag: 'status', backupURL: this.#backupURL, ...changeLogState},
        ]);
      })
      .catch(e => {
        this.#lc.warn?.(`failing snapshot reservation`, e);
        sub.fail(e);
      });
    return sub;
  }

  endReservation(taskID: string, updateCleanupDelay = true) {
    const res = this.#reservations.get(taskID);
    if (res === undefined) {
      return;
    }
    this.#reservations.delete(taskID);
    const {start, sub} = res;
    sub.cancel(); // closes the connection if still open

    if (updateCleanupDelay) {
      const duration = Date.now() - start.getTime();
      this.#lc.info?.(`snapshot initialized by ${taskID} in ${duration} ms`);
      if (duration > this.#cleanupDelayMs) {
        this.#cleanupDelayMs = duration;
        this.#lc.info?.(`increased cleanup delay to ${duration} ms`);
      }
    }
  }

  // Exported for testing
  readonly checkWatermarksAndScheduleCleanup = async () => {
    try {
      await this.#checkWatermarks();
    } catch (e) {
      this.#lc.warn?.(`unable to fetch metrics at ${this.#metricsEndpoint}`, e);
    }
    try {
      await this.#scheduleCleanup();
    } catch (e) {
      this.#lc.warn?.(`error scheduling cleanup`, e);
    }
  };

  async *#fetchWatermarks(): AsyncGenerator<{
    watermark: string;
    time: Date;
    name?: string | undefined;
  }> {
    const metricsEndpoint = this.#metricsEndpoint;
    const signal = this.#state.signal;
    let resp;
    try {
      resp = await fetch(metricsEndpoint, {signal});
    } catch (e) {
      if (signal.aborted) {
        // not an error.
        return;
      }
      // Treat exceptions from fetch (e.g. network errors) as non-fatal, and simply
      // log them and skip the watermark check until the next interval.
      this.#lc.warn?.(`unable to fetch metrics at ${this.#metricsEndpoint}`, e);
      return;
    }
    if (!resp.ok) {
      this.#lc.warn?.(
        `unable to fetch metrics at ${this.#metricsEndpoint}: ${await resp.text()}`,
      );
      return;
    }

    const families = parsePrometheusTextFormat(await resp.text());
    for (const family of families) {
      if (
        family.type === 'GAUGE' &&
        family.name === 'litestream_replica_progress'
      ) {
        for (const metric of family.metrics) {
          const watermark = metric.labels?.watermark;
          const name = metric.labels?.name;
          const time = new Date(parseFloat(metric.value) * 1000);

          if (watermark) {
            yield {watermark, time, name};
          }
        }
      }
    }
  }

  async #checkWatermarks() {
    for await (const {watermark, name, time} of this.#fetchWatermarks()) {
      if (watermark > this.#lastWatermark && !this.#watermarks.has(watermark)) {
        this.#lc.info?.(
          `replicated watermark=${watermark} to ${name}` +
            ` at ${time.toISOString()}.`,
        );
        this.#watermarks.set(watermark, time);
        this.#latestBackupTime = time;
      }
    }
    return this.#latestBackupTime;
  }

  async #scheduleCleanup() {
    if (this.#reservations.size > 0) {
      this.#lc.info?.(
        `watermark cleanup paused for snapshot(s): ${[...this.#reservations.keys()]}`,
      );
      return;
    }
    const latestCleanupTime = Date.now() - this.#cleanupDelayMs;
    const maxWatermark = this.#maxWatermarkUpTo(latestCleanupTime);
    if (maxWatermark.length === 0) {
      return;
    }
    // Purge guard: the watermarks (and their backup times) come from
    // litestream metrics, which are exported when litestream *believes*
    // an upload succeeded, and have been observed to advance even when
    // nothing was actually written to the backup destination. Purging the
    // change-log based on a falsely advancing watermark permanently breaks
    // the ability to restore + catch up. Before advancing the cleanup
    // watermark, verify it against the actual backup state: a claimed
    // backup time is only trusted if an object was actually uploaded to
    // the replica destination at (or after) that time, modulo clock skew.
    const claimedTime = must(this.#watermarks.get(maxWatermark));
    if (!this.#confirmedDurable(claimedTime)) {
      try {
        this.#lastVerifiedUploadTime = await this.#verifyBackupState();
      } catch (e) {
        this.#purgesBlocked.add(1, {reason: 'verification-failed'});
        // Skipping the purge is safe: the change-log just grows.
        this.#lc.warn?.(
          `unable to verify backup state. skipping change-log cleanup ` +
            `up to watermark ${maxWatermark} ` +
            `(claimed backup time ${claimedTime.toISOString()})`,
          e,
        );
        return;
      }
    }
    // Watermarks whose backup time isn't yet confirmed durable remain in the
    // map and are re-evaluated at the next check.
    const lastUpload = must(this.#lastVerifiedUploadTime);
    const verifiedWatermark = this.#maxWatermarkUpTo(
      Math.min(
        latestCleanupTime,
        lastUpload.getTime() + BACKUP_VERIFICATION_SLACK_MS,
      ),
    );
    if (verifiedWatermark.length === 0) {
      this.#purgesBlocked.add(1, {reason: 'backup-stale'});
      this.#lc.warn?.(
        `blocked change-log cleanup up to watermark ${maxWatermark}: ` +
          `litestream claims it was backed up at ` +
          `${claimedTime.toISOString()}, but the last object actually ` +
          `uploaded to ${this.#backupURL} was at ` +
          `${lastUpload.toISOString()}. ` +
          `The backup may be wedged.`,
      );
      return;
    }
    this.#changeStreamer.scheduleCleanup(verifiedWatermark);
    for (const watermark of this.#watermarks.keys()) {
      if (watermark <= verifiedWatermark) {
        this.#watermarks.delete(watermark);
      }
    }
    this.#lastWatermark = verifiedWatermark;
  }

  /**
   * Returns the newest watermark whose backup time is at or before `cutoff`
   * (epoch ms), or `''` if there is none.
   */
  #maxWatermarkUpTo(cutoff: number): string {
    let max = '';
    for (const [watermark, backupTime] of this.#watermarks) {
      if (backupTime.getTime() <= cutoff && watermark > max) {
        max = watermark;
      }
    }
    return max;
  }

  /**
   * Returns `true` if the actual backup state, as last verified against the
   * backup destination, confirms that data claimed to be backed up at
   * `claimedTime` is durable (i.e. an object was actually uploaded at or
   * after `claimedTime`, allowing {@link BACKUP_VERIFICATION_SLACK_MS} of
   * clock skew).
   */
  #confirmedDurable(claimedTime: Date): boolean {
    const lastUpload = this.#lastVerifiedUploadTime;
    return (
      lastUpload !== null &&
      claimedTime.getTime() <=
        lastUpload.getTime() + BACKUP_VERIFICATION_SLACK_MS
    );
  }

  stop(): Promise<void> {
    clearInterval(this.#checkMetricsTimer);
    for (const {sub} of this.#reservations.values()) {
      // Close any pending reservations. This commonly happens when a new
      // replication-manager makes a `/snapshot` reservation on the existing
      // replication-manager, and then shuts it down when it takes over the
      // replication slot.
      sub.cancel();
    }
    this.#state.stop(this.#lc);
    return promiseVoid;
  }

  #initBackupLagMetric() {
    getOrCreateGauge('replica', 'backup_lag', {
      description:
        'Latency from when a change is written to the replica ' +
        'to when it is backed up to litestream. It is expected to create a saw ' +
        'pattern from 0 to the configured ZERO_LITESTREAM_INCREMENTAL_BACKUP_INTERVAL_MINUTES.',
      unit: 'millisecond',
    }).addCallback(async o => {
      // For legacy litestream, we use the watermark metric (and its associated
      // backup time) exported by litestream metrics to determine the time of
      // of the backed up watermark. This is technically imprecise--it would be
      // more correct to use the committed writeTimeMs--but it is good enough
      // in that it serves the purpose of detecting a non-functioning backup.
      // With litestream v5, this can be made more precise by querying the
      // _zero.replicationState row from the backup directly using an LTX-based
      // database reader.
      const latestBackup = await this.#checkWatermarks();
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
        const backupLag = Math.max(0, writeTimeMs - latestBackup.getTime());
        o.observe(backupLag);
      } catch (e) {
        this.#lc.warn?.(`error measuring replica.backup_lag metric`, e);
      } finally {
        db.close();
      }
    });
  }
}
