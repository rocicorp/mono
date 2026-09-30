import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {bench, describe} from '../../shared/src/bench.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Catch} from '../../zql/src/ivm/catch.ts';
import {FlippedJoin} from '../../zql/src/ivm/flipped-join.ts';
import {Database} from '../../zqlite/src/db.ts';
import {TableSource} from '../../zqlite/src/table-source.ts';

const lc = createSilentLogContext();
const ROW_COUNTS = [100, 500, 1_000, 2_500, 5_000, 10_000] as const;

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
    CREATE TABLE parent (id INTEGER NOT NULL, label TEXT NOT NULL);
    CREATE UNIQUE INDEX parent_id_idx ON parent (id);
    CREATE TABLE child (id INTEGER NOT NULL, parentId INTEGER NOT NULL);
    CREATE UNIQUE INDEX child_id_idx ON child (id);
    CREATE INDEX child_parent_idx ON child (parentId);
  `);

  const insertParent = db.prepare(
    'INSERT INTO parent (id, label) VALUES (?,?)',
  );
  const insertChild = db.prepare(
    'INSERT INTO child (id, parentId) VALUES (?,?)',
  );
  db.transaction(() => {
    for (let i = 1; i <= rows; i++) {
      insertParent.run(i, `p${i}`);
      insertChild.run(i, i);
    }
  });

  const parent = new TableSource(
    lc,
    testLogConfig,
    db,
    'parent',
    {id: {type: 'number'}, label: {type: 'string'}},
    ['id'],
  );
  const child = new TableSource(
    lc,
    testLogConfig,
    db,
    'child',
    {id: {type: 'number'}, parentId: {type: 'number'}},
    ['id'],
  );
  const flippedJoin = new FlippedJoin({
    parent: parent.connect([['id', 'asc']]),
    child: child.connect([['id', 'asc']]),
    parentKey: ['id'],
    childKey: ['parentId'],
    relationshipName: 'children',
    hidden: false,
    system: 'client',
  });
  return {db, out: new Catch(flippedJoin)};
}

describe('flipped join batching', () => {
  for (const rows of ROW_COUNTS) {
    bench(
      `fetch ${rows} one-to-one children`,
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
