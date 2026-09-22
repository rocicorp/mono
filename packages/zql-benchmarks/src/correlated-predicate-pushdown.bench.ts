import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {bench, describe} from '../../shared/src/bench.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import {buildPipeline} from '../../zql/src/builder/builder.ts';
import {TestBuilderDelegate} from '../../zql/src/builder/test-builder-delegate.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeRemove,
} from '../../zql/src/ivm/source.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import {Database} from '../../zqlite/src/db.ts';
import {TableSource} from '../../zqlite/src/table-source.ts';

// One `covers` push into
// user.where(userID).related(reading.related(works.related(covers))), where
// every reader has read the pushed cover's work. Without the pass the push
// reads every reading of the work. With it, only the pinned user's.

const lc = createSilentLogContext();
const READER_COUNTS = [100, 10_000, 1_000_000] as const;

const ast: AST = {
  table: 'user',
  where: {
    type: 'simple',
    left: {type: 'column', name: 'userID'},
    op: '=',
    right: {type: 'literal', value: 0},
  },
  related: [
    {
      correlation: {parentField: ['userID'], childField: ['userID']},
      subquery: {
        table: 'reading',
        alias: 'reading',
        related: [
          {
            correlation: {parentField: ['workID'], childField: ['id']},
            subquery: {
              table: 'works',
              alias: 'works',
              related: [
                {
                  correlation: {parentField: ['id'], childField: ['workID']},
                  subquery: {table: 'covers', alias: 'covers'},
                },
              ],
            },
          },
        ],
      },
    },
  ],
};

function setup(readers: number, pushdown: boolean) {
  const db = new Database(lc, ':memory:');
  db.exec(/* sql */ `
    CREATE TABLE user (userID INTEGER NOT NULL);
    CREATE UNIQUE INDEX user_pk ON user (userID);
    CREATE TABLE reading (
      id INTEGER NOT NULL,
      userID INTEGER NOT NULL,
      workID INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX reading_pk ON reading (id);
    CREATE INDEX reading_user_idx ON reading (userID);
    CREATE INDEX reading_work_idx ON reading (workID);
    CREATE TABLE works (id INTEGER NOT NULL);
    CREATE UNIQUE INDEX works_pk ON works (id);
    CREATE TABLE covers (id INTEGER NOT NULL, workID INTEGER NOT NULL);
    CREATE UNIQUE INDEX covers_pk ON covers (id);
    CREATE INDEX covers_work_idx ON covers (workID);
  `);
  const insertUser = db.prepare('INSERT INTO user (userID) VALUES (?)');
  const insertReading = db.prepare(
    'INSERT INTO reading (id, userID, workID) VALUES (?, ?, 1)',
  );
  db.transaction(() => {
    for (let i = 0; i < readers; i++) {
      insertUser.run(i);
      insertReading.run(i, i);
    }
    db.exec('INSERT INTO works (id) VALUES (1)');
    db.exec('INSERT INTO covers (id, workID) VALUES (1, 1)');
  });
  // Replicas have statistics (zero-cache runs `PRAGMA optimize`). Without
  // them SQLite can serve `workID = ? AND userID = ?` from the workID index,
  // which reads every reading of the work.
  db.exec('ANALYZE');

  const source = (name: string, columns: string[], pk: string) =>
    new TableSource(
      lc,
      testLogConfig,
      db,
      name,
      Object.fromEntries(columns.map(c => [c, {type: 'number'}] as const)),
      [pk],
    );
  const covers = source('covers', ['id', 'workID'], 'id');
  const delegate = new TestBuilderDelegate(
    {
      user: source('user', ['userID'], 'userID'),
      reading: source('reading', ['id', 'userID', 'workID'], 'id'),
      works: source('works', ['id'], 'id'),
      covers,
    },
    false,
    false,
    !pushdown,
  );
  const input = buildPipeline(ast, delegate, 'bench');
  let pushes = 0;
  input.setOutput({
    push() {
      pushes++;
      return [];
    },
  });
  consume(input.fetch({}));
  return {db, input, covers, pushes: () => pushes};
}

describe('correlated predicate pushdown: covers push', () => {
  for (const readers of READER_COUNTS) {
    for (const pushdown of [false, true]) {
      bench(
        `${readers} readers, pushdown ${pushdown ? 'on' : 'off'}`,
        function* () {
          const {db, input, covers, pushes} = setup(readers, pushdown);
          const cover = {id: 2, workID: 1};

          yield () => {
            const before = pushes();
            consume(covers.push(makeSourceChangeAdd(cover)));
            consume(covers.push(makeSourceChangeRemove(cover)));
            if (pushes() - before !== 2) {
              throw new Error(`Expected 2 pushes, got ${pushes() - before}`);
            }
          };

          input.destroy();
          db.close();
        },
        {
          min_cpu_time: 1,
          min_samples: readers >= 1_000_000 ? 3 : 20,
          max_samples: readers >= 1_000_000 ? 3 : 20,
        },
      );
    }
  }
});
