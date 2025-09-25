import {defineConfig} from 'drizzle-kit';
import {must} from '../../packages/shared/src/must.ts';

const dbUrl = must(
  process.env.ZERO_UPSTREAM_DB,
  'ZERO_UPSTREAM_DB is required',
);

// eslint-disable-next-line no-console
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
