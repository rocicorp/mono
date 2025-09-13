import {drizzle} from 'drizzle-orm/node-postgres';
import {must} from '../../../packages/shared/src/must.ts';
import {config} from '@dotenvx/dotenvx';

config();

const dbUrl = must(
  process.env.DRIZZLE_DATABASE_URL,
  'DRIZZLE_DATABASE_URL is required',
);

export const db = drizzle(dbUrl);
