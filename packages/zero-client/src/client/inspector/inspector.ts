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
    delegate: InspectorDelegate,
    getSocket: () => Promise<WebSocket>,
  ) {
    this.#delegate = {
      getQueryMetrics: delegate.getQueryMetrics.bind(delegate),
      getAST: delegate.getAST.bind(delegate),
      get metrics() {
        return delegate.metrics;
      },
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
}
