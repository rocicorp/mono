import type {ReadonlyJSONValue} from '../../../../shared/src/json.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../../zql/src/ivm/data.ts';
import type {TTL} from '../../../../zql/src/query/ttl.ts';

export interface GetInspector {
  inspect(): Promise<Inspector>;
}

export interface Inspector {
  readonly client: Client;
  readonly clientGroup: ClientGroup;
  clients(): Promise<Client[]>;
  clientsWithQueries(): Promise<Client[]>;
}

export interface Client {
  readonly id: string;
  readonly clientGroup: ClientGroup;
  queries(): Promise<Query[]>;
  map(): Promise<Map<string, ReadonlyJSONValue>>;
  rows(tableName: string): Promise<Row[]>;
}

export interface ClientGroup {
  readonly id: string;
  clients(): Promise<Client[]>;
  clientsWithQueries(): Promise<Client[]>;
  queries(): Promise<Query[]>;
}

export interface Query {
  readonly ast: AST;
  readonly clientID: string;
  readonly deleted: boolean;
  readonly got: boolean;
  readonly id: string;
  readonly inactivatedAt: Date | null;
  readonly rowCount: number;
  readonly ttl: TTL;
  readonly zql: string;
}
