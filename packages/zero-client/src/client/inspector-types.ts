import type {AST} from '../../../zero-protocol/src/ast.ts';

export interface GetInspector {
  inspect(): Promise<Inspector>;
}

export interface Inspector {
  readonly clientID: string;
  readonly clientGroupID: string;
  readonly client: Client;
  readonly clientGroup: ClientGroup;
  clients(): Promise<Client[]>;
  clientsWithQueries(): Promise<Client[]>;
  clientGroups(): Promise<ClientGroup[]>;
}

export interface Client {
  readonly clientID: string;
  readonly clientGroupID: string;
  readonly clientGroup: ClientGroup;
  queries(): Promise<Query[]>;
}

export interface ClientGroup {
  readonly clientGroupID: string;
  clients(): Promise<Client[]>;
  queries(): Promise<Query[]>;
}

export interface Query {
  readonly id: string;
  readonly ast: AST;
  readonly got: boolean;
  readonly sql: string;
}
