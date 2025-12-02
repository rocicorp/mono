import {zeroPostgresJS} from '@rocicorp/zero/server/adapters/postgresjs';
import postgres from 'postgres';
import {must} from '../../../packages/shared/src/must.ts';
import {schema} from '../shared/schema.ts';

export const sql = postgres(
  must(process.env.ZERO_UPSTREAM_DB, 'ZERO_UPSTREAM_DB is required'),
);

export const dbProvider = zeroPostgresJS(schema, sql);

declare module '@rocicorp/zero' {
  interface DefaultTypes {
    dbProvider: typeof dbProvider;
  }
}
