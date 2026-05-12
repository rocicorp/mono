import type {
  ServerTransaction as ZeroServerTransaction,
  Transaction as ZeroTransaction,
} from '@rocicorp/zero';
import type {PostgresJsTransaction} from '@rocicorp/zero/server/adapters/postgresjs';
import type {schema} from '../shared/schema.ts';

export type Transaction = ZeroTransaction<typeof schema, PostgresJsTransaction>;
export type ServerTransaction = ZeroServerTransaction<
  typeof schema,
  PostgresJsTransaction
>;
