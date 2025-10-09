import type {AnalyzeQueryResult} from '../../../../zero-protocol/src/analyze-query-result.ts';
import type {AnalyzeQueryOptions} from '../../../../zero-protocol/src/inspect-up.ts';
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

export type Lazy = typeof import('./lazy-inspector.ts');

export class Inspector {
  readonly #delegate: ExtendedInspectorDelegate;
  readonly client: Client;
  readonly clientGroup: ClientGroup;

  constructor(
    rep: Rep,
    inspectorDelegate: InspectorDelegate,
    queryDelegate: QueryDelegate<unknown>,
    getSocket: () => Promise<WebSocket>,
  ) {
    this.#delegate = {
      getQueryMetrics:
        inspectorDelegate.getQueryMetrics.bind(inspectorDelegate),
      getAST: inspectorDelegate.getAST.bind(inspectorDelegate),
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
}
