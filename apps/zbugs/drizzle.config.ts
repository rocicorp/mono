import {defineConfig} from 'drizzle-kit';
import {must} from '../../packages/shared/src/must.ts';

const dbUrl = must(
  process.env.DRIZZLE_DATABASE_URL,
  'DRIZZLE_DATABASE_URL is required',
);

console.log(dbUrl);

export default defineConfig({
  out: './db/migrations',
  schema: './db/schema.ts',
  dialect: 'postgresql',
  strict: true,
  dbCredentials: {
    url: dbUrl,
  },
});
