import {assert} from '../../../shared/src/asserts.ts';
import {mapValues} from '../../../shared/src/objects.ts';
import {TDigest} from '../../../shared/src/tdigest.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {ServerMetrics as ServerMetricsJSON} from '../../../zero-protocol/src/inspect-down.ts';
import {
  isServerMetric,
  type MetricMap,
  type MetricsDelegate,
} from '../../../zql/src/query/metrics-delegate.ts';
import {isDevelopmentMode} from '../config/normalize.ts';

/**
 * Server-side metrics collected for queries during materialization and update.
 * These metrics are reported via the inspector and complement client-side metrics.
 */
export type ServerMetrics = {
  'query-materialization-server': TDigest;
  'query-update-server': TDigest;
};

type ClientGroupID = string;

/**
 * Set of authenticated client IDs. We keep this outside of the class to share this state
 * across all instances of the InspectorDelegate.
 */
const authenticatedClientIDs = new Set<ClientGroupID>();

export class InspectorDelegate implements MetricsDelegate {
  readonly #globalMetrics: ServerMetrics = newMetrics();
  readonly #perQueryServerMetrics = new Map<string, ServerMetrics>();
  readonly #hashToIDs = new Map<string, Set<string>>();
  readonly #queryIDToTransformationHash = new Map<string, string>();
  readonly #transformationASTs: Map<string, AST> = new Map();

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

  getASTForQuery(queryID: string): AST | undefined {
    const transformationHash = this.#queryIDToTransformationHash.get(queryID);
    return transformationHash
      ? this.#transformationASTs.get(transformationHash)
      : undefined;
  }

  removeQuery(queryID: string): void {
    this.#perQueryServerMetrics.delete(queryID);
    this.#queryIDToTransformationHash.delete(queryID);
    // Remove queryID from all hash-to-ID mappings
    for (const [transformationHash, idSet] of this.#hashToIDs.entries()) {
      idSet.delete(queryID);
      if (idSet.size === 0) {
        this.#hashToIDs.delete(transformationHash);
        this.#transformationASTs.delete(transformationHash);
      }
    }
  }

  addQuery(transformationHash: string, queryID: string, ast: AST): void {
    const existing = this.#hashToIDs.get(transformationHash);
    if (existing === undefined) {
      this.#hashToIDs.set(transformationHash, new Set([queryID]));
    } else {
      existing.add(queryID);
    }
    this.#queryIDToTransformationHash.set(queryID, transformationHash);
    this.#transformationASTs.set(transformationHash, ast);
  }

  /**
   * Check if the client is authenticated. We only require authentication once
   * per "worker".
   */
  isAuthenticated(clientGroupID: ClientGroupID): boolean {
    if (isDevelopmentMode()) {
      return true;
    }
    return authenticatedClientIDs.has(clientGroupID);
  }

  setAuthenticated(clientGroupID: ClientGroupID): void {
    authenticatedClientIDs.add(clientGroupID);
  }

  clearAuthenticated(clientGroupID: ClientGroupID) {
    authenticatedClientIDs.delete(clientGroupID);
  }
}

function newMetrics(): ServerMetrics {
  return {
    'query-materialization-server': new TDigest(),
    'query-update-server': new TDigest(),
  };
}
