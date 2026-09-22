import {expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {relationships} from '../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import {Debug} from '../../zql/src/builder/debug-delegate.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import {Database} from './db.ts';
import {newQueryDelegate} from './test/source-factory.ts';

const foo = table('foo')
  .columns({
    id: string(),
  })
  .primaryKey('id');

const bar = table('bar')
  .columns({
    id: string(),
    fooID: string().from('foo_id'),
    primary: boolean(),
  })
  .primaryKey('id');

const fooRelationships = relationships(foo, ({many}) => ({
  bars: many({
    sourceField: ['id'],
    destField: ['fooID'],
    destSchema: bar,
  }),
}));

const schema = createSchema({
  tables: [foo, bar],
  relationships: [fooRelationships],
});

test('a filter on a related table uses its partial index', async () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  const queryDelegate = newQueryDelegate(
    createSilentLogContext(),
    testLogConfig,
    db,
    schema,
  );
  const debug = new Debug(false);
  queryDelegate.debug = debug;

  // Lazily create the tables before adding the replica-style partial index.
  queryDelegate.getSource('foo');
  queryDelegate.getSource('bar');
  db.exec(`
    CREATE INDEX primary_bar ON bar(foo_id) WHERE "primary" = 1;
    INSERT INTO foo VALUES ('foo-1');
    WITH RECURSIVE ids(id) AS (
      VALUES(1)
      UNION ALL
      SELECT id + 1 FROM ids WHERE id < 5000
    )
    INSERT INTO bar
      SELECT printf('not-primary-%04d', id), 'foo-1', 0 FROM ids;
    INSERT INTO bar VALUES ('primary', 'foo-1', 1);
  `);

  const result = await queryDelegate.run(
    newQuery(schema, 'foo')
      .where('id', 'foo-1')
      .related('bars', q => q.where('primary', true)),
  );
  expect(result).toHaveLength(1);
  expect(result[0].bars).toHaveLength(1);
  expect(result[0].bars[0].id).toBe('primary');

  const relatedPlan = Object.entries(debug.getSQLitePlans()).find(
    ([sql]) => sql.includes('FROM "bar"') && sql.includes('"primary" = ?'),
  );
  expect(relatedPlan?.[1]).toContain(
    'SEARCH bar USING INDEX primary_bar (foo_id=?)',
  );
});
