import {drizzle} from 'drizzle-orm/node-postgres';
import {must} from '../../../packages/shared/src/must.ts';
import {config} from '@dotenvx/dotenvx';

config();

const dbUrl = must(
  process.env.ZERO_UPSTREAM_DB,
  'ZERO_UPSTREAM_DB is required',
);

export const db = drizzle(dbUrl);
