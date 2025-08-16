import {assert} from '../../../shared/src/asserts.ts';
import {mapValues} from '../../../shared/src/objects.ts';
import {TDigest} from '../../../shared/src/tdigest.ts';
import type {ServerMetrics as ServerMetricsJSON} from '../../../zero-protocol/src/inspect-down.ts';
import {
  isServerMetric,
  type MetricMap,
  type MetricsDelegate,
} from '../../../zql/src/query/metrics-delegate.ts';

/**
 * Server-side metrics collected for queries during materialization and update.
 * These metrics are reported via the inspector and complement client-side metrics.
 */
export type ServerMetrics = {
  'query-materialization-server': TDigest;
  'query-update-server': TDigest;
};

export class InspectMetricsDelegate implements MetricsDelegate {
  readonly #globalMetrics: ServerMetrics = newMetrics();
  readonly #perQueryServerMetrics = new Map<string, ServerMetrics>();

  addMetric<K extends keyof MetricMap>(
    metric: K,
    value: number,
    ...args: MetricMap[K]
  ): void {
    assert(isServerMetric(metric), `Invalid server metric: ${metric}`);
    const queryID = args[0];

    let serverMetrics = this.#perQueryServerMetrics.get(queryID);
    if (!serverMetrics) {
      serverMetrics = newMetrics();
      this.#perQueryServerMetrics.set(queryID, serverMetrics);
    }

    serverMetrics[metric].add(value);
    this.#globalMetrics[metric].add(value);
  }

  getMetricsJSONForQuery(queryID: string): ServerMetricsJSON | null {
    const serverMetrics = this.#perQueryServerMetrics.get(queryID);
    return serverMetrics ? mapValues(serverMetrics, v => v.toJSON()) : null;
  }

  getMetricsJSON() {
    return mapValues(this.#globalMetrics, v => v.toJSON());
  }

  deleteMetricsForQuery(queryID: string): void {
    this.#perQueryServerMetrics.delete(queryID);
  }
}

function newMetrics(): ServerMetrics {
  return {
    'query-materialization-server': new TDigest(),
    'query-update-server': new TDigest(),
  };
}
