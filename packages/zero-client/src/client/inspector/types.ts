import type {AST} from '../../../../zero-protocol/src/ast.ts';

export interface GetInspector {
  inspect(): Promise<Inspector>;
}

export interface Inspector {
  readonly client: Client;
  readonly clientGroup: ClientGroup;
  clients(): Promise<Client[]>;
  clientsWithQueries(): Promise<Client[]>;
  clientGroups(): Promise<ClientGroup[]>;
}

export interface Client {
  readonly id: string;
  readonly clientGroup: ClientGroup;
  queries(): Promise<Query[]>;
}

export interface ClientGroup {
  readonly id: string;
  clients(): Promise<Client[]>;
  queries(): Promise<Query[]>;
}

export interface Query {
  readonly id: string;
  readonly ast: AST;
  readonly got: boolean;
  readonly sql: string;
  readonly zql: string;
}
