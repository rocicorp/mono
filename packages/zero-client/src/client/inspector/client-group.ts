import type {Client} from './client.ts';
import type {ExtendedInspectorDelegate} from './lazy-inspector.ts';
import type {Query} from './query.ts';

export class ClientGroup {
  readonly #delegate: ExtendedInspectorDelegate;
  readonly id: Promise<string> | string;

  constructor(
    delegate: ExtendedInspectorDelegate,
    clientGroupID: Promise<string> | string,
  ) {
    this.#delegate = delegate;
    this.id = clientGroupID;
  }

  async clients(): Promise<Client[]> {
    return (await this.#delegate.lazy).clientGroupClients(
      this.#delegate,
      this.id,
    );
  }

  async clientsWithQueries(): Promise<Client[]> {
    return (await this.#delegate.lazy).clientGroupClientsWithQueries(
      this.#delegate,
      this.id,
    );
  }

  async queries(): Promise<Query[]> {
    return (await this.#delegate.lazy).clientGroupQueries(this.#delegate);
  }
}
