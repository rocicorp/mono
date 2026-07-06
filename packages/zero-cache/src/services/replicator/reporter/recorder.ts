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
}
