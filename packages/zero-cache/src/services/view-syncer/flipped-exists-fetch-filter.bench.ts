import {afterAll, beforeAll} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {bench, describe, use} from '../../../../shared/src/bench.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {NoSubqueryCondition} from '../../../../zql/src/builder/filter.ts';
import type {Input} from '../../../../zql/src/ivm/operator.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {TableSource} from '../../../../zqlite/src/table-source.ts';

const ROW_COUNTS = [1_000, 10_000, 100_000] as const;
const columns = {
  id: {type: 'number'},
  payload: {type: 'string'},
} as const;

type Fetch = {
  readonly db: Database;
  readonly input: Input;
  readonly pkFilter: NoSubqueryCondition;
};

function fetchRowCount(input: Input, filter: NoSubqueryCondition | undefined) {
  let count = 0;
  for (const node of input.fetch({filter})) {
    if (node !== 'yield') {
      count++;
    }
  }
  return count;
}

describe('flipped EXISTS fetch-filter pushdown', () => {
  const lc = createSilentLogContext();
  const fetches = new Map<number, Fetch>();

  beforeAll(() => {
    for (const rowCount of ROW_COUNTS) {
      const db = new Database(lc, ':memory:');
      const probeID = rowCount / 2;
      db.exec(/*sql*/ `
        CREATE TABLE track (
          id INTEGER PRIMARY KEY,
          payload TEXT NOT NULL
        );
        CREATE UNIQUE INDEX track_id_unique ON track(id);
      `);
      const insert = db.prepare(
        'INSERT INTO track (id, payload) VALUES (?, ?)',
      );
      db.transaction(() => {
        for (let id = 1; id <= rowCount; id++) {
          insert.run(id, 'x'.repeat(128));
        }
      });

      const source = new TableSource(lc, testLogConfig, db, 'track', columns, [
        'id',
      ]);
      fetches.set(rowCount, {
        db,
        input: source.connect(undefined),
        pkFilter: {
          type: 'simple',
          left: {type: 'column', name: 'id'},
          op: '=',
          right: {type: 'literal', value: probeID},
        },
      });
    }
  });

  afterAll(() => {
    for (const {db, input} of fetches.values()) {
      input.destroy();
      db.close();
    }
  });

  for (const rowCount of ROW_COUNTS) {
    bench(`before: unfiltered PK branch (${rowCount} rows)`, () => {
      const {input} = fetches.get(rowCount)!;
      use(fetchRowCount(input, undefined));
    });

    bench(`after: PK filter pushed to source (${rowCount} rows)`, () => {
      const {input, pkFilter} = fetches.get(rowCount)!;
      use(fetchRowCount(input, pkFilter));
    });
  }
});
