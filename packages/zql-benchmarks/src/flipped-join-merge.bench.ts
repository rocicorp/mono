import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {bench, describe} from '../../shared/src/bench.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {relationships} from '../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {number, table} from '../../zero-schema/src/builder/table-builder.ts';
import {buildPipeline} from '../../zql/src/builder/builder.ts';
import {TestBuilderDelegate} from '../../zql/src/builder/test-builder-delegate.ts';
import {Catch} from '../../zql/src/ivm/catch.ts';
import {asQueryImpl, newQuery} from '../../zql/src/query/query-impl.ts';
import {Database} from '../../zqlite/src/db.ts';
import {TableSource} from '../../zqlite/src/table-source.ts';

const lc = createSilentLogContext();
const ROW_COUNTS = [100, 500, 1_000, 2_500, 5_000, 10_000] as const;

const parentTable = table('parent')
  .columns({
    id: number(),
    bucket: number(),
  })
  .primaryKey('id');

const childTable = table('child')
  .columns({
    id: number(),
    bucket: number(),
  })
  .primaryKey('id');

const parentRelationships = relationships(parentTable, ({many}) => ({
  children: many({
    sourceField: ['bucket'],
    destField: ['bucket'],
    destSchema: childTable,
  }),
}));

const schema = createSchema({
  tables: [parentTable, childTable],
  relationships: [parentRelationships],
});

function benchOptionsForRows(rows: number) {
  const samples = rows < 1_000 ? 25 : rows < 5_000 ? 10 : 5;
  return {
    min_cpu_time: 1,
    min_samples: samples,
    max_samples: samples,
  };
}

function setup(rows: number) {
  const db = new Database(lc, ':memory:');
  db.exec(/* sql */ `
    CREATE TABLE parent (
      id INTEGER NOT NULL,
      bucket INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX parent_id_idx ON parent (id);
    CREATE INDEX parent_bucket_idx ON parent (bucket);
    CREATE TABLE child (
      id INTEGER NOT NULL,
      bucket INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX child_id_idx ON child (id);
    CREATE INDEX child_bucket_idx ON child (bucket);
  `);

  const insertParent = db.prepare(
    'INSERT INTO parent (id, bucket) VALUES (?,?)',
  );
  const insertChild = db.prepare('INSERT INTO child (id, bucket) VALUES (?,?)');
  db.transaction(() => {
    for (let i = 1; i <= rows; i++) {
      insertParent.run(i, i);
      insertChild.run(i, i);
    }
  });

  const parent = new TableSource(
    lc,
    testLogConfig,
    db,
    'parent',
    {id: {type: 'number'}, bucket: {type: 'number'}},
    ['id'],
  );
  const child = new TableSource(
    lc,
    testLogConfig,
    db,
    'child',
    {id: {type: 'number'}, bucket: {type: 'number'}},
    ['id'],
  );
  const delegate = new TestBuilderDelegate({parent, child});
  const query = newQuery(schema, 'parent').whereExists('children', {
    flip: true,
  });
  const input = buildPipeline(asQueryImpl(query).ast, delegate, 'bench');
  return {db, out: new Catch(input)};
}

describe('flipped join merge path', () => {
  for (const rows of ROW_COUNTS) {
    bench(
      `fetch ${rows} one-to-one children through ZQL pipeline`,
      function* () {
        const {db, out} = setup(rows);
        const warmup = out.fetch({});
        if (warmup.length !== rows) {
          throw new Error(`Expected ${rows} rows, got ${warmup.length}`);
        }

        yield () => {
          const result = out.fetch({});
          if (result.length !== rows) {
            throw new Error(`Expected ${rows} rows, got ${result.length}`);
          }
        };

        out.destroy();
        db.close();
      },
      benchOptionsForRows(rows),
    );
  }
});
