import type {ObservableResult} from '@opentelemetry/api';
import type {LogContext} from '@rocicorp/logger';
import {getOrCreateGauge} from '../../../observability/metrics.ts';
import type {ReplicationReport} from './report-schema.ts';

// Hook for sanity checking lag reports in development.
const LOG_ALL_REPLICATION_REPORTS_AT_DEBUG =
  process.env.ZERO_LOG_ALL_REPLICATION_REPORTS_AT_DEBUG === '1';

export class ReplicationReportRecorder {
  readonly #lc: LogContext;
  #last: ReplicationReport | null = null;

  constructor(lc: LogContext) {
    this.#lc = lc;
  }

  record(report: ReplicationReport) {
    const first = this.#last === null;
    this.#last = report;

    const {lastTimings} = report;
    if (lastTimings) {
      const total = lastTimings.replicateTimeMs - lastTimings.sendTimeMs;
      if (total > 10_000) {
        this.#lc.warn?.(`high replication lag: ${total} ms`, report);
      } else if (total > 1_000) {
        this.#lc.info?.(`replication lag: ${total} ms`, report);
      }
      if (LOG_ALL_REPLICATION_REPORTS_AT_DEBUG) {
        this.#lc.debug?.(`replication lag ${total} ms`, report);
      }
    }

    if (first) {
      getOrCreateGauge('replication', 'upstream_lag', {
        description:
          'Latency from sending an upstream replication report ' +
          'to receiving it in the replication stream',
        unit: 'millisecond',
      }).addCallback(this.reportUpstreamLag);

      getOrCreateGauge('replication', 'replica_lag', {
        description:
          'Latency from receiving an upstream replication report ' +
          'to its reaching the replica',
        unit: 'millisecond',
      }).addCallback(this.reportReplicaLag);

      getOrCreateGauge('replication', 'total_lag', {
        description:
          'Latency from sending an upstream replication report to its ' +
          'reaching the replica. This reflects the actual measured ' +
          'round-trip of the most recently received report and does not ' +
          'grow when reports stop arriving.',
        unit: 'millisecond',
      }).addCallback(this.reportTotalLag);

      getOrCreateGauge('replication', 'last_total_lag', {
        description:
          'Latency from sending the most recently received upstream ' +
          'replication report to its reaching the replica. This is an alias ' +
          'of replication.total_lag retained for dashboards that explicitly ' +
          'want the non-extrapolated value.',
        unit: 'millisecond',
      }).addCallback(this.reportLastTotalLag);

      getOrCreateGauge('replication', 'upstream_clock_skew', {
        description:
          'Estimated offset of the upstream database clock relative to ' +
          "zero-cache's. Positive means upstream is running ahead. " +
          'Derived from the lag report round-trip, whose send and receive ' +
          'times are stamped on our clock either side of the upstream ' +
          'commit timestamp. This matters because ' +
          'sync.e2e_serving_lag subtracts an upstream commit timestamp from ' +
          'a local clock, so a skewed upstream clock biases it directly -- ' +
          'and an upstream clock running ahead biases it *low*, which looks ' +
          'like healthy serving rather than a broken measurement.',
        unit: 'millisecond',
      }).addCallback(this.reportUpstreamClockSkew);
    }
  }

  readonly reportUpstreamLag = (o: ObservableResult) => {
    const last = this.#last?.lastTimings;
    if (last) {
      o.observe(last.receiveTimeMs - last.sendTimeMs);
    }
  };

  readonly reportReplicaLag = (o: ObservableResult) => {
    const last = this.#last?.lastTimings;
    if (last) {
      o.observe(last.replicateTimeMs - last.receiveTimeMs);
    }
  };

  readonly reportTotalLag = (o: ObservableResult) => {
    const last = this.#last?.lastTimings;
    if (last) {
      o.observe(last.replicateTimeMs - last.sendTimeMs);
    }
  };

  readonly reportLastTotalLag = (o: ObservableResult) => {
    const last = this.#last?.lastTimings;
    if (last) {
      o.observe(last.replicateTimeMs - last.sendTimeMs);
    }
  };

  readonly reportUpstreamClockSkew = (o: ObservableResult) => {
    const last = this.#last?.lastTimings;
    if (last) {
      o.observe(estimateUpstreamClockSkewMs(last));
    }
  };
}

/**
 * Estimates how far the upstream database's clock is from ours, in
 * milliseconds, positive meaning upstream is ahead.
 *
 * `sendTimeMs` and `receiveTimeMs` are both stamped in the
 * replication-manager, either side of `commitTimeMs`, which upstream stamps.
 * Assuming the upstream commit fell midway through that round trip, the
 * difference between where it actually landed and that midpoint is the clock
 * offset -- the same round-trip estimate NTP uses.
 *
 * The estimate is biased by any asymmetry between the two legs: the outbound
 * leg is a SQL round trip, while the return leg additionally waits on a WAL
 * flush and the walsender (which the pre-PG-17 path notes can add 50~100ms on
 * an idle database). So treat this as a detector of gross skew -- the kind
 * that invalidates sync.e2e_serving_lag -- and not as a sub-100ms correction.
 *
 * The rigorous bound, if a tighter reading is ever needed: the commit
 * genuinely happened between send and receive, so a `commitTimeMs` outside
 * that window proves skew of at least the amount by which it falls outside,
 * independent of any latency assumption.
 */
export function estimateUpstreamClockSkewMs({
  sendTimeMs,
  commitTimeMs,
  receiveTimeMs,
}: {
  sendTimeMs: number;
  commitTimeMs: number;
  receiveTimeMs: number;
}): number {
  return commitTimeMs - (sendTimeMs + receiveTimeMs) / 2;
}
