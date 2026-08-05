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
   * @return the end-to-end lag in milliseconds to record, or `null` if the
   *     served version does not yet cover an outstanding upstream commit.
   */
  onVersionServed(servedVersion: string, nowMs: number): number | null {
    const pending = this.#pending;
    if (pending === null || servedVersion < pending.watermark) {
      return null;
    }
    this.#pending = null;
    // The commit time comes from the upstream database's clock while `nowMs`
    // is local, so clamp rather than let skew put a negative duration into the
    // histogram's sum.
    return Math.max(0, nowMs - pending.commitTimeMs);
  }
}
