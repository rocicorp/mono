import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {bench, describe} from '../../shared/src/bench.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Catch} from '../../zql/src/ivm/catch.ts';
import {FlippedJoin} from '../../zql/src/ivm/flipped-join.ts';
import {Database} from '../../zqlite/src/db.ts';
import {TableSource} from '../../zqlite/src/table-source.ts';

// `flipped-join-batching.bench.ts` covers integer keys at a 1:1 ratio.
// This one varies what `#fetchBatched` actually keys on: string keys (the
// common id shape), how many children share one parent key, and compound
// keys, which take a different path than single keys.
const lc = createSilentLogContext();
const ROWS = 1_000;

const benchOptions = {
  min_cpu_time: 1,
  min_samples: 25,
  max_samples: 25,
};

type Shape = {
  name: string;
  distinctKeys: number;
  compound: boolean;
};

const shapes: Shape[] = [
  {name: '1000 unique string keys', distinctKeys: ROWS, compound: false},
  {name: '50 string keys x 20 children', distinctKeys: 50, compound: false},
  {
    name: '1000 compound (string,string) keys',
    distinctKeys: ROWS,
    compound: true,
  },
];

function setup({distinctKeys, compound}: Shape) {
  const db = new Database(lc, ':memory:');
  db.exec(/* sql */ `
    CREATE TABLE parent (id TEXT NOT NULL, bucket TEXT NOT NULL);
    CREATE UNIQUE INDEX parent_id_idx ON parent (id);
    CREATE INDEX parent_bucket_idx ON parent (bucket, id);
    CREATE TABLE child (id TEXT NOT NULL, parentBucket TEXT NOT NULL, parentId TEXT NOT NULL);
    CREATE UNIQUE INDEX child_id_idx ON child (id);
    CREATE INDEX child_parent_idx ON child (parentBucket, parentId);
  `);

  const insertParent = db.prepare(
    'INSERT INTO parent (id, bucket) VALUES (?,?)',
  );
  const insertChild = db.prepare(
    'INSERT INTO child (id, parentBucket, parentId) VALUES (?,?,?)',
  );
  const parentId = (i: number) => `parent_row_id_${String(i).padStart(6, '0')}`;
  const bucket = (i: number) => `bucket_${String(i % distinctKeys)}`;
  db.transaction(() => {
    for (let i = 0; i < ROWS; i++) {
      insertParent.run(parentId(i), bucket(i));
      insertChild.run(
        `child_row_id_${String(i).padStart(6, '0')}`,
        bucket(i),
        parentId(i),
      );
    }
  });

  const parent = new TableSource(
    lc,
    testLogConfig,
    db,
    'parent',
    {id: {type: 'string'}, bucket: {type: 'string'}},
    ['id'],
  );
  const child = new TableSource(
    lc,
    testLogConfig,
    db,
    'child',
    {
      id: {type: 'string'},
      parentBucket: {type: 'string'},
      parentId: {type: 'string'},
    },
    ['id'],
  );
  const flippedJoin = new FlippedJoin({
    parent: parent.connect([['id', 'asc']]),
    child: child.connect([['id', 'asc']]),
    parentKey: compound ? ['bucket', 'id'] : ['bucket'],
    childKey: compound ? ['parentBucket', 'parentId'] : ['parentBucket'],
    relationshipName: 'children',
    hidden: false,
    system: 'client',
  });
  return {db, out: new Catch(flippedJoin)};
}

describe('flipped join keys', () => {
  for (const shape of shapes) {
    bench(
      `fetch ${shape.name}`,
      function* () {
        const {db, out} = setup(shape);
        const expected = out.fetch({}).length;
        if (expected !== ROWS) {
          throw new Error(`Expected ${ROWS} rows, got ${expected}`);
        }

        yield () => {
          const result = out.fetch({});
          if (result.length !== expected) {
            throw new Error(`Expected ${expected} rows, got ${result.length}`);
          }
        };

        out.destroy();
        db.close();
      },
      benchOptions,
    );
  }
});
