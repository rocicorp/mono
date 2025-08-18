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
  readonly #hashToIDs: Map<string, Set<string>> = new Map();

  addMetric<K extends keyof MetricMap>(
    metric: K,
    value: number,
    ...args: MetricMap[K]
  ): void {
    assert(isServerMetric(metric), `Invalid server metric: ${metric}`);
    const transformationHash = args[0];

    for (const queryID of this.#hashToIDs.get(transformationHash) ?? []) {
      let serverMetrics = this.#perQueryServerMetrics.get(queryID);
      if (!serverMetrics) {
        serverMetrics = newMetrics();
        this.#perQueryServerMetrics.set(queryID, serverMetrics);
      }
      serverMetrics[metric].add(value);
    }
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
    // Remove queryID from all hash-to-ID mappings
    for (const [hash, idSet] of this.#hashToIDs.entries()) {
      idSet.delete(queryID);
      if (idSet.size === 0) {
        this.#hashToIDs.delete(hash);
      }
    }
  }

  addQueryMapping(transformationHash: string, queryID: string): void {
    const existing = this.#hashToIDs.get(transformationHash);
    if (existing === undefined) {
      this.#hashToIDs.set(transformationHash, new Set([queryID]));
    } else {
      existing.add(queryID);
    }
  }
}

function newMetrics(): ServerMetrics {
  return {
    'query-materialization-server': new TDigest(),
    'query-update-server': new TDigest(),
  };
}
