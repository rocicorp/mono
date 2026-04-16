import type {ReadonlyJSONValue} from '../../../../shared/src/json.ts';
import type {
  AnalyzeQueryResult,
  PlanDebugEventJSON,
} from '../../../../zero-protocol/src/analyze-query-result.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {AnalyzeQueryOptions} from '../../../../zero-protocol/src/inspect-up.ts';
import {formatPlannerEvents} from '../../../../zql/src/planner/planner-debug.ts';
import type {QueryDelegate} from '../../../../zql/src/query/query-delegate.ts';
import type {AnyQuery} from '../../../../zql/src/query/query.ts';
import type {ClientGroup} from './client-group.ts';
import {Client} from './client.ts';
import type {
  ExtendedInspectorDelegate,
  InspectorDelegate,
  Metrics,
  Rep,
} from './lazy-inspector.ts';

export type {InspectorDelegate};

// oxlint-disable-next-line consistent-type-imports
export type Lazy = typeof import('./lazy-inspector.ts');

export class Inspector {
  readonly #delegate: ExtendedInspectorDelegate;
  readonly client: Client;
  readonly clientGroup: ClientGroup;

  constructor(
    rep: Rep,
    inspectorDelegate: InspectorDelegate,
    queryDelegate: QueryDelegate,
    getSocket: () => Promise<WebSocket>,
  ) {
    this.#delegate = {
      getQueryMetrics:
        inspectorDelegate.getQueryMetrics.bind(inspectorDelegate),
      getAST: inspectorDelegate.getAST.bind(inspectorDelegate),
      mapClientASTToServer:
        inspectorDelegate.mapClientASTToServer.bind(inspectorDelegate),
      get metrics() {
        return inspectorDelegate.metrics;
      },
      queryDelegate,
      rep,
      getSocket,
      lazy: import('./lazy-inspector.ts'),
    };

    this.client = new Client(this.#delegate, rep.clientID, rep.clientGroupID);
    this.clientGroup = this.client.clientGroup;
  }

  async metrics(): Promise<Metrics> {
    return (await this.#delegate.lazy).inspectorMetrics(this.#delegate);
  }

  async clients(): Promise<Client[]> {
    return (await this.#delegate.lazy).inspectorClients(this.#delegate);
  }

  async clientsWithQueries(): Promise<Client[]> {
    return (await this.#delegate.lazy).inspectorClientsWithQueries(
      this.#delegate,
    );
  }

  async serverVersion(): Promise<string> {
    return (await this.#delegate.lazy).serverVersion(this.#delegate);
  }

  async analyzeQuery(
    query: AnyQuery,
    options?: AnalyzeQueryOptions,
  ): Promise<AnalyzeQueryResult> {
    return (await this.#delegate.lazy).analyzeQuery(
      this.#delegate,
      query,
      options,
    );
  }

  /**
   * Analyze a query specified by a server-side AST. Unlike {@link analyzeQuery}
   * the AST is sent to the server verbatim with no client-to-server name
   * mapping; callers should provide an AST already in the server shape.
   */
  async analyzeServerAST(
    ast: AST,
    options?: AnalyzeQueryOptions,
  ): Promise<AnalyzeQueryResult> {
    return (await this.#delegate.lazy).analyzeServerAST(
      this.#delegate,
      ast,
      options,
    );
  }

  /**
   * Analyze a server-registered named (custom) query. The server resolves
   * the name and args to an AST using its registered custom-query handler.
   */
  async analyzeNamedQuery(
    name: string,
    args: ReadonlyArray<ReadonlyJSONValue>,
    options?: AnalyzeQueryOptions,
  ): Promise<AnalyzeQueryResult> {
    return (await this.#delegate.lazy).analyzeNamedQuery(
      this.#delegate,
      name,
      args,
      options,
    );
  }

  /**
   * Authenticate with the server's admin password. Other inspector RPCs
   * (e.g. {@link analyzeQuery}) fall back to an interactive HTML password
   * prompt when authentication is needed, which is unavailable in non-DOM
   * environments. Call this first from Node contexts to establish the
   * session.
   *
   * Returns `true` if the password is accepted (or the server runs in a
   * development mode that bypasses the check), `false` otherwise.
   */
  async authenticate(password: string): Promise<boolean> {
    return (await this.#delegate.lazy).authenticate(this.#delegate, password);
  }

  /**
   * Format planner debug events as a human-readable string.
   */
  formatPlannerEvents(events: PlanDebugEventJSON[]): string {
    return formatPlannerEvents(events);
  }
}
