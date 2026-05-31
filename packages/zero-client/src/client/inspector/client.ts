import type {ReadonlyJSONValue} from '../../../../shared/src/json.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import {ClientGroup} from './client-group.ts';
import type {ExtendedInspectorDelegate} from './lazy-inspector.ts';
import type {Query} from './query.ts';

export class Client {
  readonly #delegate: ExtendedInspectorDelegate;
  readonly id: string;
  readonly clientGroup: ClientGroup;

  constructor(
    delegate: ExtendedInspectorDelegate,
    clientID: string,
    clientGroupID: Promise<string> | string,
  ) {
    this.#delegate = delegate;
    this.id = clientID;

    this.clientGroup = new ClientGroup(this.#delegate, clientGroupID);
  }

  async queries(): Promise<Query[]> {
    return (await this.#delegate.lazy).clientQueries(this.#delegate, this.id);
  }

  async map(): Promise<Map<string, ReadonlyJSONValue>> {
    return (await this.#delegate.lazy).clientMap(this.#delegate, this.id);
  }

  async rows(tableName: string): Promise<Row[]> {
    return (await this.#delegate.lazy).clientRows(
      this.#delegate,
      this.id,
      tableName,
    );
  }
}
