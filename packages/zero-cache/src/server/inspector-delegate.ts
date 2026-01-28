import {assert} from '../../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {mapValues} from '../../../shared/src/objects.ts';
import {TDigest} from '../../../shared/src/tdigest.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {ServerMetrics as ServerMetricsJSON} from '../../../zero-protocol/src/inspect-down.ts';
import {hashOfNameAndArgs} from '../../../zero-protocol/src/query-hash.ts';
import {
  isServerMetric,
  type MetricMap,
  type MetricsDelegate,
} from '../../../zql/src/query/metrics-delegate.ts';
import {isDevelopmentMode} from '../config/normalize.ts';
import type {CustomQueryTransformer} from '../custom-queries/transform-query.ts';
import type {HeaderOptions} from '../custom/fetch.ts';
import type {CustomQueryRecord} from '../services/view-syncer/schema/types.ts';
import {ProtocolErrorWithLevel} from '../types/error-with-level.ts';

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
 * Set of authenticated client group IDs. We keep this outside of the class to
 * share this state across all instances of the InspectorDelegate.
 */
const authenticatedClientGroupIDs = new Set<ClientGroupID>();

export class InspectorDelegate implements MetricsDelegate {
  readonly #globalMetrics: ServerMetrics = newMetrics();
  readonly #perQueryServerMetrics = new Map<string, ServerMetrics>();
  readonly #queryIDToAST: Map<string, AST> = new Map();
  readonly #customQueryTransformer: CustomQueryTransformer | undefined;

  constructor(customQueryTransformer: CustomQueryTransformer | undefined) {
    this.#customQueryTransformer = customQueryTransformer;
  }

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

  getASTForQuery(queryID: string): AST | undefined {
    return this.#queryIDToAST.get(queryID);
  }

  removeQuery(queryID: string): void {
    this.#perQueryServerMetrics.delete(queryID);
    this.#queryIDToAST.delete(queryID);
  }

  addQuery(queryID: string, ast: AST): void {
    this.#queryIDToAST.set(queryID, ast);
  }

  /**
   * Check if the client is authenticated. We only require authentication once
   * per "worker".
   */
  isAuthenticated(clientGroupID: ClientGroupID): boolean {
    return (
      isDevelopmentMode() || authenticatedClientGroupIDs.has(clientGroupID)
    );
  }

  setAuthenticated(clientGroupID: ClientGroupID): void {
    authenticatedClientGroupIDs.add(clientGroupID);
  }

  clearAuthenticated(clientGroupID: ClientGroupID) {
    authenticatedClientGroupIDs.delete(clientGroupID);
  }

  /**
   * Transforms a single custom query by name and args using the configured
   * CustomQueryTransformer. This is primarily used by the inspector to transform
   * queries for analysis.
   */
  async transformCustomQuery(
    name: string,
    args: readonly ReadonlyJSONValue[],
    headerOptions: HeaderOptions,
    userQueryURL: string | undefined,
  ): Promise<AST> {
    assert(
      this.#customQueryTransformer,
      'Custom query transformation requested but no CustomQueryTransformer is configured',
    );

    // Create a fake CustomQueryRecord for the single query
    const queryID = hashOfNameAndArgs(name, args);
    const queries: CustomQueryRecord[] = [
      {
        id: queryID,
        type: 'custom',
        name,
        args,
        clientState: {},
      },
    ];

    const results = await this.#customQueryTransformer.transform(
      headerOptions,
      queries,
      userQueryURL,
    );

    if ('kind' in results) {
      throw new ProtocolErrorWithLevel(results, 'warn');
    }

    const result = results[0];
    if (!result) {
      throw new Error('No transformation result returned');
    }

    if ('error' in result) {
      const message =
        result.message ?? 'Unknown application error from custom query';
      throw new Error(
        `Error transforming custom query ${name} (${result.error}): ${message} ${JSON.stringify(result.details)}`,
      );
    }

    return result.transformedAst;
  }
}

function newMetrics(): ServerMetrics {
  return {
    'query-materialization-server': new TDigest(),
    'query-update-server': new TDigest(),
  };
}
