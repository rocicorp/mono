import {astToZQL} from '../../../../ast-to-zql/src/ast-to-zql.ts';
import type {ReadonlyJSONValue} from '../../../../shared/src/json.ts';
import {must} from '../../../../shared/src/must.ts';
import type {AnalyzeQueryResult} from '../../../../zero-protocol/src/analyze-query-result.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {
  type InspectQueryRow,
  inspectAnalyzeQueryDownSchema,
} from '../../../../zero-protocol/src/inspect-down.ts';
import type {AnalyzeQueryOptions} from '../../../../zero-protocol/src/inspect-up.ts';
import {type TTL, normalizeTTL} from '../../../../zql/src/query/ttl.ts';
import {
  type ExtendedInspectorDelegate,
  type GetWebSocket,
  type Metrics,
  mergeMetrics,
  rpc,
} from './lazy-inspector.ts';

export class Query {
  readonly #socket: GetWebSocket;

  readonly name: string | null;
  readonly args: ReadonlyArray<ReadonlyJSONValue> | null;
  readonly got: boolean;
  readonly ttl: TTL;
  readonly inactivatedAt: Date | null;
  readonly rowCount: number;
  readonly deleted: boolean;
  readonly id: string;
  readonly clientID: string;
  readonly metrics: Metrics | null;
  readonly clientZQL: string | null;
  readonly serverZQL: string | null;
  readonly #serverAST: AST | null;

  readonly hydrateClient: number | null;
  readonly hydrateServer: number | null;
  readonly hydrateTotal: number | null;

  readonly updateClientP50: number | null;
  readonly updateClientP95: number | null;
  readonly updateServerP50: number | null;
  readonly updateServerP95: number | null;

  constructor(
    row: InspectQueryRow,
    delegate: ExtendedInspectorDelegate,
    socket: GetWebSocket,
  ) {
    this.#socket = socket;

    const {ast, queryID, inactivatedAt} = row;
    // Use own properties to make this more useful in dev tools. For example, in
    // Chrome dev tools, if you do console.table(queries) you'll see the
    // properties in the table, if these were getters you would not see them in the table.
    this.clientID = row.clientID;
    this.id = queryID;
    this.inactivatedAt =
      inactivatedAt === null ? null : new Date(inactivatedAt);
    this.ttl = normalizeTTL(row.ttl);
    this.name = row.name;
    this.args = row.args;
    this.got = row.got;
    this.rowCount = row.rowCount;
    this.deleted = row.deleted;
    this.#serverAST = ast;
    this.serverZQL = ast ? ast.table + astToZQL(ast) : null;
    const clientAST = delegate.getAST(queryID);
    this.clientZQL = clientAST ? clientAST.table + astToZQL(clientAST) : null;

    // Merge client and server metrics
    const clientMetrics = delegate.getQueryMetrics(queryID);
    const serverMetrics = row.metrics;

    const merged = mergeMetrics(clientMetrics, serverMetrics);
    this.metrics = merged;

    const percentile = (
      name: keyof typeof merged,
      percentile: number,
    ): number | null => {
      if (!merged?.[name]) {
        return null;
      }
      const n = merged[name].quantile(percentile);
      return Number.isNaN(n) ? null : n;
    };

    // Extract hydration metrics (median values) - handle NaN by defaulting to 0
    this.hydrateClient = percentile('query-materialization-client', 0.5);
    this.hydrateServer = percentile('query-materialization-server', 0.5);
    this.hydrateTotal = percentile('query-materialization-end-to-end', 0.5);

    // Extract update metrics (P50 and P95) - handle NaN by defaulting to 0
    this.updateClientP50 = percentile('query-update-client', 0.5);
    this.updateClientP95 = percentile('query-update-client', 0.95);

    this.updateServerP50 = percentile('query-update-server', 0.5);
    this.updateServerP95 = percentile('query-update-server', 0.95);
  }

  async analyze(options?: AnalyzeQueryOptions): Promise<AnalyzeQueryResult> {
    const details =
      this.name && this.args
        ? {
            name: this.name,
            args: this.args,
          }
        : {value: must(this.#serverAST, 'AST is required for unnamed queries')};

    return rpc(
      await this.#socket(),
      {
        op: 'analyze-query',
        ...details,
        options,
      },
      inspectAnalyzeQueryDownSchema,
    );
  }
}
