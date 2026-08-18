import type {ReplicaState} from '../replicator/replicator.ts';

export type PendingUpstreamCommit = {
  readonly watermark: string;
  readonly commitTimeMs: number;
};

/**
 * Pairs the upstream commit timestamps carried by `version-ready`
 * notifications with the moment the corresponding change is poked to clients,
 * yielding the end-to-end serving lag.
 *
 * This measures completion, not backlog: an observation is produced only when
 * work actually reaches clients, so the resulting histogram is a latency
 * distribution rather than a periodic snapshot of how far behind things are.
 * A ViewSyncer that is stuck contributes nothing until it recovers, instead of
 * re-reporting its age on every sample tick.
 *
 * "Reaches clients" includes an advancement that had nothing to send this
 * client group. Such an advancement still establishes that the group is
 * current as of that commit, so it must clear the pending commit; a tracker
 * that waited for a *data* poke would instead accumulate the oldest commit
 * across every quiet interval and report the whole interval as lag the next
 * time the group happened to match a transaction. The caller is responsible
 * for passing the replica version it advanced to rather than the CVR version,
 * which only moves when there is something to write.
 */
export class E2EServingLagTracker {
  #pending: PendingUpstreamCommit | null = null;

  get pending(): PendingUpstreamCommit | null {
    return this.#pending;
  }

  /**
   * Records the upstream commit behind a `version-ready` notification.
   *
   * Notifications coalesce when the ViewSyncer is busy, so one state may stand
   * in for several commits. The *oldest* commit time is kept, since it bounds
   * the lag of everything the notification subsumed, while the watermark
   * advances to the newest — that is the one that must be served for all of
   * the subsumed commits to have been delivered.
   */
  onVersionReady({watermark, upstreamCommitTimeMs}: ReplicaState): void {
    if (watermark === undefined || upstreamCommitTimeMs === undefined) {
      return;
    }
    const pending = this.#pending;
    this.#pending = {
      watermark,
      commitTimeMs:
        pending === null
          ? upstreamCommitTimeMs
          : Math.min(pending.commitTimeMs, upstreamCommitTimeMs),
    };
  }

  /**
   * Called once a version has been poked to clients.
   *
   * @return the observation to record, or `null` if the served version does
   *     not yet cover an outstanding upstream commit.
   */
  onVersionServed(servedVersion: string, nowMs: number): Observation | null {
    const pending = this.#pending;
    if (pending === null || servedVersion < pending.watermark) {
      return null;
    }
    this.#pending = null;

    const lagMs = nowMs - pending.commitTimeMs;
    if (lagMs >= 0) {
      return {lagMs, clamped: false};
    }
    // The commit time is on the upstream database's clock while `nowMs` is
    // local, so a negative duration means upstream's clock is running ahead of
    // ours by more than the entire pipeline latency. Clamp, because a negative
    // value would corrupt the histogram's sum -- but report the clamp, since
    // it is proof of gross clock skew, and skew in this direction biases the
    // metric *low*, which reads as healthy serving rather than as a broken
    // measurement. See replication.upstream_clock_skew for the magnitude.
    return {lagMs: 0, clamped: true};
  }
}

export type Observation = {
  /** End-to-end lag in milliseconds, clamped to be non-negative. */
  readonly lagMs: number;
  /** Whether the clamp was applied, i.e. the raw measurement was negative. */
  readonly clamped: boolean;
};
