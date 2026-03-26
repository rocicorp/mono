import type {ObservableResult} from '@opentelemetry/api';
import type {LogContext} from '@rocicorp/logger';
import {getOrCreateGauge} from '../../../observability/metrics.ts';
import type {ReplicationReport} from './report-schema.ts';

// Hook for sanity checking lag reports in development.
const LOG_ALL_REPLICATION_REPORTS_AT_DEBUG =
  process.env.ZERO_LOG_ALL_REPLICATION_REPORTS_AT_DEBUG === '1';

export class ReplicationReportRecorder {
  readonly #lc: LogContext;
  readonly #now: () => number;
  #last: ReplicationReport | null = null;

  constructor(lc: LogContext, now = Date.now) {
    this.#lc = lc;
    this.#now = now;
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
          'reaching the replica. This will be a (growing) estimate if the ' +
          'next expected report has yet to be received and the elapsed ' +
          `time has exceeded the previous report's total lag.`,
        unit: 'millisecond',
      }).addCallback(this.reportTotalLag);
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
    const last = this.#last;
    if (last) {
      const nextLagEstimate = this.#now() - last.nextSendTimeMs;
      const timings = last.lastTimings;
      const lastLag = timings
        ? timings.replicateTimeMs - timings.sendTimeMs
        : 0;
      o.observe(Math.max(lastLag, nextLagEstimate));
    }
  };
}
