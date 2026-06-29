import {expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {must} from '../../shared/src/must.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import {makeSourceChangeAdd} from '../../zql/src/ivm/source.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import {Database} from './db.ts';
import {newQueryDelegate} from './test/source-factory.ts';

const lc = createSilentLogContext();

// A column stored as epoch millis (number) but read as a Date.
const schema = createSchema({
  tables: [
    table('event')
      .columns({
        id: string(),
        at: number().codec<Date>({
          decode: (n: number) => new Date(n),
          encode: (d: Date) => d.getTime(),
        }),
      })
      .primaryKey('id'),
  ],
});

test('zqlite applies the column codec to query results', async () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  const queryDelegate = newQueryDelegate(lc, testLogConfig, db, schema);

  const source = must(queryDelegate.getSource('event'));
  // Stored values are the encoded (number) form, as on the SQLite replica.
  consume(source.push(makeSourceChangeAdd({id: 'a', at: 1000})));
  consume(source.push(makeSourceChangeAdd({id: 'b', at: 2000})));

  const rows = await queryDelegate.run(
    newQuery(schema, 'event').orderBy('id', 'asc'),
  );

  expect(rows[0].at).toBeInstanceOf(Date);
  expect((rows[0].at as Date).getTime()).toBe(1000);
  expect((rows[1].at as Date).getTime()).toBe(2000);
  expect(rows[0].id).toBe('a');
});
