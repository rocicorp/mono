import {zeroPostgresJS} from '@rocicorp/zero/server/adapters/postgresjs';
import postgres from 'postgres';
import {schema} from '../shared/schema.ts';

export const sql = postgres(process.env.ZERO_UPSTREAM_DB as string);

export const dbProvider = zeroPostgresJS(schema, sql);

declare module '@rocicorp/zero' {
  interface DefaultTypes {
    dbProvider: typeof dbProvider;
  }
}
