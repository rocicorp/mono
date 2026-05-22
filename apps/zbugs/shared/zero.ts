import {initZero} from '@rocicorp/zero';
import type postgres from 'postgres';
import type {AuthData} from './auth.ts';
import {schema} from './schema.ts';

export const zero = initZero<
  typeof schema,
  AuthData | undefined,
  postgres.TransactionSql
>({schema});

export const {Zero, defineMutator, defineMutators, defineQuery, defineQueries} =
  zero;

export type ZeroTypes = typeof zero;
export type Row = ZeroTypes['~']['$row'];
export type Transaction = ZeroTypes['~']['$transaction'];
export type ServerTransaction = ZeroTypes['~']['$serverTransaction'];
export type ZeroClient = ZeroTypes['~']['$zero'];
