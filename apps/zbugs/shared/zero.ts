import {
  initZero,
  type Row as ZeroRow,
  type ServerTransaction as ZeroServerTransaction,
  type Transaction as ZeroTransaction,
  type Zero as ZeroClient,
} from '@rocicorp/zero';
import type {AuthData} from './auth.ts';
import {schema} from './schema.ts';

export type Row = ZeroRow<typeof schema>;
export type Zero = ZeroClient<typeof schema, undefined, AuthData | undefined>;
export type Transaction = ZeroTransaction<typeof schema>;
export type ServerTransaction = ZeroServerTransaction<typeof schema>;

export const {defineMutator, defineMutators, defineQuery, defineQueries} =
  initZero<typeof schema, AuthData | undefined>();
