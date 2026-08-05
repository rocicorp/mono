import {statSync} from 'node:fs';
import type {LogContext} from '@rocicorp/logger';
import parsePrometheusTextFormat from 'parse-prometheus-text-format';
import {assert} from '../../../../shared/src/asserts.ts';
import {must} from '../../../../shared/src/must.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import {
  litestreamBackupVerificationDuration,
  litestreamMonitorMetricAttrs,
} from '../litestream/metrics.ts';
import {RunningState, UnrecoverableError} from '../running-state.ts';
import type {BackedUpWatermark} from './backup-monitor.ts';

export const CHECK_INTERVAL_MS = 10_000;

/**
 * Allowance for clock skew between the machine reporting litestream metrics
 * and the timestamps reported by the backup destination (e.g. S3).
 */
export const BACKUP_VERIFICATION_SLACK_MS = 60_000;

/**
 * How long the actual backup state may remain *continuously* behind the
 * backup progress claimed by litestream metrics before the backup is
 * considered genuinely wedged and the process is shut down.
 *
 * A wedged backup is dangerous: litestream believes it is making durable
 * progress when it is not, which can silently corrupt the backup (the WAL
 * format only makes *some* such gaps detectable). Once that is confirmed to
 * be the persistent state, continuing to run is worse than crashing, so the
 * process exits loudly (non-zero, with a logged error) and the wedged backup
 * destination becomes the priority to investigate.
 *
 * The change-log is conservatively *not* purged for the entire time the
 * backup is stale, so the only cost of a generous grace period is unbounded
 * change-log growth. We therefore err well on the side of slack: the backup
 * must stay stuck across many {@link CHECK_INTERVAL_MS} checks before we tear
 * the server down, so that a transient hiccup (a slow or briefly unreachable
 * destination, or litestream restarting) does not trigger a shutdown. The
 * staleness clock is reset the moment a purge is confirmed against a real
 * upload.
 */
export const WEDGED_SHUTDOWN_GRACE_MS = 15 * 60_000; // 15 minutes

const GiB = 1024 ** 3;

/**
 * How long the BackupMonitor waits for the *first* restorable backup to appear
 * before concluding that the backup pipeline is broken and shutting the process
 * down (see {@link BackupMonitor.#shutDownOnMissingInitialBackup}).
 *
 * On a fresh stack the first backup is not restorable until the initial
 * Postgres->replica sync completes and litestream finishes uploading the
 * initial snapshot — a multipart upload that is not listable in the destination
 * until it is committed. The producer cannot observe that sub-snapshot progress,
 * so this is necessarily a generous timeout. It is scaled by replica size to
 * mirror the platform's storage-scaled startup-probe allowance (~1 hour per
 * 50GB) so that it never fires before the platform would restart a view-syncer
 * that is waiting on the backup.
 */
export const INITIAL_BACKUP_GRACE_MS_PER_UNIT = 60 * 60_000; // 1 hour ...
const INITIAL_BACKUP_GRACE_UNIT_BYTES = 50 * GiB; // ... per 50 GiB

/**
 * How often the BackupMonitor re-checks whether the first restorable backup has
 * appeared while a view-syncer holds a snapshot reservation open during a cold
 * start.
 */
export const RESTORABLE_BACKUP_POLL_INTERVAL_MS = 10_000;

/**
 * Returns the time of the most recent object actually uploaded to the
 * backup replica destination (e.g. as determined by listing the snapshots
 * and WAL segments in S3). Rejects if the backup state cannot be determined.
 *
 * See `getLastBackupTime()` in `../litestream/commands.ts` for the
 * production implementation.
 */
export type BackupStateVerifier = () => Promise<Date>;

/**
 * The Litestream3PrometheusPoller polls the litestream "/metrics" endpoint to
 * track the watermark (label) value of the `litestream_replica_progress` gauge
 * and emits verified watermarks to a stream of {@link BackedUpWatermark}s.
 *
 * See: https://github.com/rocicorp/litestream/pull/3
 *
 * Because the watermarks reported by litestream metrics reflect what
 * litestream *believes* has been backed up (which has been observed to
 * diverge from reality when uploads silently fail), the cleanup watermark is
 * only published after verifying it against the actual backup state in the
 * replica destination via a {@link BackupStateVerifier}. If the
 * actual backup state stays behind the claimed progress for longer than
 * {@link WEDGED_SHUTDOWN_GRACE_MS}, the backup is treated as wedged and the
 * process is shut down rather than risk corrupting the backup.
 */
export class Litestream3PrometheusPoller {
  readonly #lc: LogContext;
  readonly #replicaFile: string;
  readonly #backupURL: string;
  readonly #metricsEndpoint: string;
  readonly #state = new RunningState('litestream3-prometheus-poller');

  readonly #stream: Subscription<BackedUpWatermark>;
  readonly #watermarks = new Map<string, Date>();

  readonly #verifyBackupState: BackupStateVerifier;
  readonly #purgesBlocked = getOrCreateCounter('replica', 'purge_blocked', {
    description:
      'Number of change-log purges blocked because the actual backup state ' +
      '(as listed from the replica destination) could not be verified, or ' +
      'is older than the backup progress claimed by litestream metrics. ' +
      'A steadily increasing value indicates a wedged or failing backup. ' +
      'The "backup-wedged" reason is emitted once just before the process ' +
      'shuts itself down due to a persistently stale backup.',
  });

  #lastWatermark: string = '';
  #latestBackupTime: Date | null = null;
  // The time of the most recent object actually verified in the backup
  // destination, or `null` if none has ever been verified. Only assigned from a
  // successful verifyBackupState() and never cleared, so a non-null value also
  // means a restorable backup has been confirmed to exist (which is what gates
  // the snapshot `status` signal and satisfies the initial-backup deadline).
  #lastVerifiedUploadTime: Date | null = null;
  // Epoch ms at which the actual backup state was first observed to be
  // continuously behind the watermark litestream claims to have backed up,
  // or `null` while the backup is keeping up. Reset whenever a purge is
  // confirmed against a real upload.
  #backupStaleSince: number | null = null;
  // Epoch ms at which run() started, used to bound how long to wait for the
  // first restorable backup to appear. `null` until run() is called.
  #runStartTime: number | null = null;
  #checkMetricsTimer: NodeJS.Timeout | undefined;

  constructor(
    lc: LogContext,
    replicaFile: string,
    backupURL: string,
    metricsEndpoint: string,
    verifyBackupState: BackupStateVerifier,
  ) {
    this.#lc = lc.withContext('component', 'litestream3-prometheus-poller');
    this.#replicaFile = replicaFile;
    this.#backupURL = backupURL;
    this.#metricsEndpoint = metricsEndpoint;
    this.#verifyBackupState = verifyBackupState;
    this.#stream = Subscription.create<BackedUpWatermark>({
      cleanup: () => {
        clearInterval(this.#checkMetricsTimer);
        this.#state.stop(this.#lc);
      },
    });
  }

  /**
   * Starts polling for watermarks and pushes new ones to returned stream.
   * Must only be called once.
   */
  start(): Source<BackedUpWatermark> {
    assert(this.#runStartTime === null, `Already called start()`);
    this.#runStartTime = Date.now();
    this.#lc.info?.(`monitoring backups at ${this.#metricsEndpoint}`);
    this.#checkMetricsTimer = setInterval(
      this.checkVerifyAndPublishWatermarks,
      CHECK_INTERVAL_MS,
    );
    return this.#stream;
  }

  /**
   * Verifies whether a restorable backup actually exists in the destination
   * (as opposed to what litestream metrics merely claim). On success the
   * verified upload time is cached — seeding the cleanup-verification fast path
   * ({@link #confirmedDurable}) — and the initial-backup deadline is satisfied.
   * Returns false if no backup is listable yet (e.g. the initial snapshot is
   * still uploading).
   */
  async #confirmRestorableBackup(): Promise<boolean> {
    try {
      this.#lastVerifiedUploadTime = await this.#verifyBackupStateTimed();
      return true;
    } catch (e) {
      this.#lc.info?.(`backup not yet restorable at ${this.#backupURL}`, e);
      return false;
    }
  }

  // Exported for testing
  readonly checkVerifyAndPublishWatermarks = async () => {
    try {
      await this.#checkWatermarks();
    } catch (e) {
      this.#lc.warn?.(`unable to fetch metrics at ${this.#metricsEndpoint}`, e);
    }
    try {
      await this.#publishVerifiedWatermarks();
    } catch (e) {
      this.#lc.warn?.(`error scheduling cleanup`, e);
    }
    try {
      await this.#checkInitialBackupDeadline();
    } catch (e) {
      this.#lc.warn?.(`error checking initial backup deadline`, e);
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

  async #publishVerifiedWatermarks() {
    const maxBackup = this.#maxBackupBefore(Date.now());
    if (!maxBackup) {
      return;
    }
    const {watermark: maxWatermark, backupTime: claimedTime} = maxBackup;
    // Purge guard: the watermarks (and their backup times) come from
    // litestream metrics, which are exported when litestream *believes*
    // an upload succeeded, and have been observed to advance even when
    // nothing was actually written to the backup destination. Purging the
    // change-log based on a falsely advancing watermark permanently breaks
    // the ability to restore + catch up. Before advancing the cleanup
    // watermark, verify it against the actual backup state: a claimed
    // backup time is only trusted if an object was actually uploaded to
    // the replica destination at (or after) that time, modulo clock skew.
    if (!this.#confirmedDurable(claimedTime)) {
      try {
        this.#lastVerifiedUploadTime = await this.#verifyBackupStateTimed();
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
    const verifiedBackup = this.#maxBackupBefore(
      lastUpload.getTime() + BACKUP_VERIFICATION_SLACK_MS,
    );
    if (!verifiedBackup) {
      this.#purgesBlocked.add(1, {reason: 'backup-stale'});
      const now = Date.now();
      if (this.#backupStaleSince === null) {
        this.#backupStaleSince = now;
      }
      const staleForMs = now - this.#backupStaleSince;
      this.#lc.warn?.(
        `blocked change-log cleanup up to watermark ${maxWatermark}: ` +
          `litestream claims it was backed up at ` +
          `${claimedTime.toISOString()}, but the last object actually ` +
          `uploaded to ${this.#backupURL} was at ` +
          `${lastUpload.toISOString()}. ` +
          `The backup has been stale for ${staleForMs} ms.`,
      );
      if (staleForMs >= WEDGED_SHUTDOWN_GRACE_MS) {
        this.#shutDownOnWedgedBackup(staleForMs, claimedTime, lastUpload);
      }
      return;
    }
    // The cleanup watermark advanced past a real upload, so the backup is
    // keeping up: clear the staleness clock.
    this.#backupStaleSince = null;
    const {watermark: verifiedWatermark, backupTime} = verifiedBackup;
    for (const watermark of this.#watermarks.keys()) {
      if (watermark <= verifiedWatermark) {
        this.#watermarks.delete(watermark);
      }
    }
    this.#stream.push({
      watermark: verifiedWatermark,
      backupTimeMs: backupTime.getTime(),
    });
    this.#lastWatermark = verifiedWatermark;
  }

  async #verifyBackupStateTimed(): Promise<Date> {
    const start = performance.now();
    let result: 'success' | 'error' = 'error';
    try {
      const ret = await this.#verifyBackupState();
      result = 'success';
      return ret;
    } finally {
      litestreamBackupVerificationDuration().recordMs(
        performance.now() - start,
        {
          ...litestreamMonitorMetricAttrs(
            this.#backupURL,
            'legacy',
            'replication_manager',
          ),
          result,
        },
      );
    }
  }

  /**
   * Called when the backup has been continuously stale for longer than
   * {@link WEDGED_SHUTDOWN_GRACE_MS}. Continuing to run risks corrupting the
   * backup, so the process is shut down by rejecting the {@link run()}
   * promise. The exit is non-zero and logged at ERROR for alerting; on
   * restart the monitor re-verifies, so a still-wedged backup keeps the
   * process down until the destination is fixed.
   */
  #shutDownOnWedgedBackup(
    staleForMs: number,
    claimedTime: Date,
    lastUpload: Date,
  ) {
    this.#purgesBlocked.add(1, {reason: 'backup-wedged'});
    const err = new UnrecoverableError(
      `backup at ${this.#backupURL} is wedged: litestream claims a backup ` +
        `at ${claimedTime.toISOString()}, but the last object actually ` +
        `uploaded was at ${lastUpload.toISOString()}, and the backup has ` +
        `not advanced for ${staleForMs} ms (grace period ` +
        `${WEDGED_SHUTDOWN_GRACE_MS} ms). Shutting down to avoid corrupting ` +
        `the backup; investigate the backup destination.`,
    );
    this.#lc.error?.(err.message);
    this.#stream.fail(err);
  }

  /**
   * On a fresh stack, fails loudly if no restorable backup has appeared within
   * a generous, replica-size-scaled grace period (see
   * {@link INITIAL_BACKUP_GRACE_MS_PER_UNIT}). This moves the "give up" decision
   * onto the replication-manager — the producer responsible for creating the
   * backup — instead of leaving view-syncers to wait (and the platform to keep
   * restarting them) while the real fault is here. Does nothing once a
   * restorable backup has been confirmed.
   */
  async #checkInitialBackupDeadline() {
    if (this.#lastVerifiedUploadTime !== null || this.#runStartTime === null) {
      return;
    }
    const elapsed = Date.now() - this.#runStartTime;
    if (elapsed < this.#initialBackupGraceMs()) {
      return;
    }
    // The deadline has elapsed without a confirmed backup. Do one definitive
    // check against the destination before giving up, in case a backup landed
    // but was never exercised by a reservation or a cleanup verification.
    if (await this.#confirmRestorableBackup()) {
      return;
    }
    this.#shutDownOnMissingInitialBackup(elapsed);
  }

  /**
   * The grace period for the first restorable backup, scaled by replica size to
   * mirror the platform's storage-scaled startup-probe allowance. Falls back to
   * the minimum (one unit) if the replica file size cannot be determined.
   */
  #initialBackupGraceMs(): number {
    let bytes = 0;
    try {
      bytes = statSync(this.#replicaFile).size;
    } catch {
      // Replica file not present/readable yet; fall back to the minimum grace.
    }
    const units = Math.max(
      1,
      Math.ceil(bytes / INITIAL_BACKUP_GRACE_UNIT_BYTES),
    );
    return units * INITIAL_BACKUP_GRACE_MS_PER_UNIT;
  }

  /**
   * Called when no restorable backup has appeared within
   * {@link #initialBackupGraceMs}. Continuing to run is pointless — no
   * view-syncer can restore — and the fault is the backup pipeline, so the
   * process exits non-zero with a logged error for alerting, mirroring
   * {@link #shutDownOnWedgedBackup}.
   */
  #shutDownOnMissingInitialBackup(elapsedMs: number) {
    const err = new UnrecoverableError(
      `no restorable backup has appeared at ${this.#backupURL} within ` +
        `${elapsedMs} ms of startup (grace period ` +
        `${this.#initialBackupGraceMs()} ms). The initial backup pipeline ` +
        `appears to be broken; shutting down so that the replication-manager ` +
        `is flagged as the cause rather than the view-syncers waiting on it. ` +
        `Investigate litestream replication to ${this.#backupURL}.`,
    );
    this.#lc.error?.(err.message);
    this.#stream.fail(err);
  }

  /**
   * Returns the newest watermark whose backup time is at or before `cutoff`
   * (epoch ms), or `''` if there is none.
   */
  #maxBackupBefore(
    cutoff: number,
  ): {watermark: string; backupTime: Date} | null {
    let max = '';
    let maxBackupTime: Date | undefined;
    for (const [watermark, backupTime] of this.#watermarks) {
      if (backupTime.getTime() <= cutoff && watermark > max) {
        max = watermark;
        maxBackupTime = backupTime;
      }
    }
    if (!maxBackupTime) {
      return null;
    }
    return {watermark: max, backupTime: maxBackupTime};
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
}
