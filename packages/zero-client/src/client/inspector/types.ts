/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/await-thenable, @typescript-eslint/no-misused-promises, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-floating-promises, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/require-await, @typescript-eslint/no-empty-object-type, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error */
import type {ReadonlyJSONValue} from '../../../../shared/src/json.ts';
import type {ReadonlyTDigest} from '../../../../shared/src/tdigest.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {TTL} from '../../../../zql/src/query/ttl.ts';

export interface GetInspector {
  inspect(): Promise<Inspector>;
}

export type Metrics = {
  'query-materialization-client': ReadonlyTDigest;
  'query-materialization-end-to-end': ReadonlyTDigest;
  'query-update-client': ReadonlyTDigest;
  'query-materialization-server': ReadonlyTDigest;
  'query-update-server': ReadonlyTDigest;
};

export interface Inspector {
  readonly client: Client;
  readonly clientGroup: ClientGroup;
  clients(): Promise<Client[]>;
  clientsWithQueries(): Promise<Client[]>;
  metrics(): Promise<Metrics>;
  serverVersion(): Promise<string>;
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
  readonly name: string | null;
  readonly args: ReadonlyArray<ReadonlyJSONValue> | null;
  readonly clientID: string;
  readonly deleted: boolean;
  readonly got: boolean;
  readonly id: string;
  readonly inactivatedAt: Date | null;
  readonly rowCount: number;
  readonly ttl: TTL;
  readonly clientZQL: string | null;
  readonly serverZQL: string | null;
  readonly metrics: Metrics | null;
}
