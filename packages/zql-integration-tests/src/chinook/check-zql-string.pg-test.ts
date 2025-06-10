import {bootstrap, runAndCompare} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';
import {test} from 'vitest';
import '../helpers/comparePg.ts';
import {defaultFormat} from '../../../zql/src/query/query-impl.ts';
import type {AnyStaticQuery} from '../../../zql/src/query/test/util.ts';
import {StaticQuery} from '../../../zql/src/query/static-query.ts';
import {staticToRunnable} from '../helpers/static.ts';

const QUERY_STRING = `track
  .whereExists('invoiceLines', q =>
    q
      .limit(0),
  ).limit(1)`;

const pgContent = await getChinook();

const harness = await bootstrap({
  suiteName: 'frontend_analysis',
  zqlSchema: schema,
  pgContent,
});

const z = {
  query: Object.fromEntries(
    Object.entries(schema.tables).map(([name]) => [
      name,
      new StaticQuery(
        schema,
        name as keyof typeof schema.tables,
        {table: name},
        defaultFormat,
      ),
    ]),
  ),
};

const f = new Function('z', `return z.query.${QUERY_STRING};`);
const query: AnyStaticQuery = f(z);

test('manual zql string', async () => {
  await runAndCompare(
    schema,
    staticToRunnable({
      query,
      schema,
      harness,
    }),
    undefined,
  );
});
