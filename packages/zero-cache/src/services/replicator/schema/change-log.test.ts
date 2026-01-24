import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../../db/statements.ts';
import {expectTableExact} from '../../../test/lite.ts';
import {
  CREATE_CHANGELOG_SCHEMA,
  logDeleteOp,
  logResetOp,
  logSetOp,
  logTruncateOp,
} from './change-log.ts';

describe('replicator/schema/change-log', () => {
  let db: StatementRunner;

  beforeEach(() => {
    const conn = new Database(createSilentLogContext(), ':memory:');
    conn.exec(CREATE_CHANGELOG_SCHEMA);
    db = new StatementRunner(conn);
  });

  function expectChangeLog(...entries: unknown[]) {
    expectTableExact(
      db.db,
      '_zero.changeLog2',
      entries,
      'number',
      'stateVersion',
      'pos',
      'table',
      'rowKey',
    );
  }

  test('replicator/schema/change-log', () => {
    expect(logSetOp(db, '01', 0, 'foo', {a: 1, b: 2})).toMatchInlineSnapshot(
      `"{"a":1,"b":2}"`,
    );
    expect(logSetOp(db, '01', 1, 'foo', {b: 3, a: 2})).toMatchInlineSnapshot(
      `"{"a":2,"b":3}"`,
    );
    expect(logSetOp(db, '01', 2, 'bar', {b: 2, a: 1})).toMatchInlineSnapshot(
      `"{"a":1,"b":2}"`,
    );
    expect(logSetOp(db, '01', 3, 'bar', {a: 2, b: 3})).toMatchInlineSnapshot(
      `"{"a":2,"b":3}"`,
    );

    expectChangeLog(
      {
        stateVersion: '01',
        pos: 0,
        table: 'foo',
        rowKey: '{"a":1,"b":2}',
        op: 's',
      },
      {
        stateVersion: '01',
        pos: 1,
        table: 'foo',
        rowKey: '{"a":2,"b":3}',
        op: 's',
      },
      {
        stateVersion: '01',
        pos: 2,
        table: 'bar',
        rowKey: '{"a":1,"b":2}',
        op: 's',
      },
      {
        stateVersion: '01',
        pos: 3,
        table: 'bar',
        rowKey: '{"a":2,"b":3}',
        op: 's',
      },
    );

    expect(logDeleteOp(db, '02', 0, 'bar', {b: 3, a: 2})).toMatchInlineSnapshot(
      `"{"a":2,"b":3}"`,
    );

    expectChangeLog(
      {
        stateVersion: '01',
        pos: 0,
        table: 'foo',
        rowKey: '{"a":1,"b":2}',
        op: 's',
      },
      {
        stateVersion: '01',
        pos: 1,
        table: 'foo',
        rowKey: '{"a":2,"b":3}',
        op: 's',
      },
      {
        stateVersion: '01',
        pos: 2,
        table: 'bar',
        rowKey: '{"a":1,"b":2}',
        op: 's',
      },
      {
        stateVersion: '02',
        pos: 0,
        table: 'bar',
        rowKey: '{"a":2,"b":3}',
        op: 'd',
      },
    );

    expect(logDeleteOp(db, '03', 0, 'foo', {a: 2, b: 3})).toMatchInlineSnapshot(
      `"{"a":2,"b":3}"`,
    );
    expect(logSetOp(db, '03', 1, 'foo', {b: 4, a: 5})).toMatchInlineSnapshot(
      `"{"a":5,"b":4}"`,
    );
    logTruncateOp(db, '03', 'foo'); // Clears all "foo" log entries, including the previous two.
    expect(logSetOp(db, '03', 2, 'foo', {b: 9, a: 8})).toMatchInlineSnapshot(
      `"{"a":8,"b":9}"`,
    );

    expectChangeLog(
      {
        stateVersion: '01',
        pos: 0,
        table: 'foo',
        rowKey: '{"a":1,"b":2}',
        op: 's',
      },
      {
        stateVersion: '01',
        pos: 2,
        table: 'bar',
        rowKey: '{"a":1,"b":2}',
        op: 's',
      },
      {
        stateVersion: '02',
        pos: 0,
        table: 'bar',
        rowKey: '{"a":2,"b":3}',
        op: 'd',
      },
      {stateVersion: '03', pos: -1, table: 'foo', rowKey: '03', op: 't'},
      {
        stateVersion: '03',
        pos: 2,
        table: 'foo',
        rowKey: '{"a":8,"b":9}',
        op: 's',
      },
    );

    expect(logDeleteOp(db, '04', 0, 'bar', {a: 1, b: 2})).toMatchInlineSnapshot(
      `"{"a":1,"b":2}"`,
    );
    expect(logSetOp(db, '04', 1, 'bar', {b: 3, a: 2})).toMatchInlineSnapshot(
      `"{"a":2,"b":3}"`,
    );
    logResetOp(db, '04', 'bar');
    expect(logSetOp(db, '04', 2, 'bar', {b: 9, a: 7})).toMatchInlineSnapshot(
      `"{"a":7,"b":9}"`,
    );

    expectChangeLog(
      {
        stateVersion: '01',
        pos: 0,
        table: 'foo',
        rowKey: '{"a":1,"b":2}',
        op: 's',
      },
      {stateVersion: '03', pos: -1, table: 'foo', rowKey: '03', op: 't'},
      {
        stateVersion: '03',
        pos: 2,
        table: 'foo',
        rowKey: '{"a":8,"b":9}',
        op: 's',
      },
      {stateVersion: '04', pos: -1, table: 'bar', rowKey: '04', op: 'r'},
      {
        stateVersion: '04',
        pos: 2,
        table: 'bar',
        rowKey: '{"a":7,"b":9}',
        op: 's',
      },
    );

    // The last table-wide op is the only one that persists.
    logTruncateOp(db, '05', 'baz');
    logResetOp(db, '05', 'baz');
    logResetOp(db, '05', 'baz');
    logResetOp(db, '05', 'baz');

    expectChangeLog(
      {
        stateVersion: '01',
        pos: 0,
        table: 'foo',
        rowKey: '{"a":1,"b":2}',
        op: 's',
      },
      {stateVersion: '03', pos: -1, table: 'foo', rowKey: '03', op: 't'},
      {
        stateVersion: '03',
        pos: 2,
        table: 'foo',
        rowKey: '{"a":8,"b":9}',
        op: 's',
      },
      {stateVersion: '04', pos: -1, table: 'bar', rowKey: '04', op: 'r'},
      {
        stateVersion: '04',
        pos: 2,
        table: 'bar',
        rowKey: '{"a":7,"b":9}',
        op: 's',
      },
      {stateVersion: '05', pos: -1, table: 'baz', rowKey: '05', op: 'r'},
    );

    logResetOp(db, '06', 'baz');
    logResetOp(db, '06', 'baz');
    logTruncateOp(db, '06', 'baz');
    logTruncateOp(db, '06', 'baz');

    expectChangeLog(
      {
        stateVersion: '01',
        pos: 0,
        table: 'foo',
        rowKey: '{"a":1,"b":2}',
        op: 's',
      },
      {stateVersion: '03', pos: -1, table: 'foo', rowKey: '03', op: 't'},
      {
        stateVersion: '03',
        pos: 2,
        table: 'foo',
        rowKey: '{"a":8,"b":9}',
        op: 's',
      },
      {stateVersion: '04', pos: -1, table: 'bar', rowKey: '04', op: 'r'},
      {
        stateVersion: '04',
        pos: 2,
        table: 'bar',
        rowKey: '{"a":7,"b":9}',
        op: 's',
      },
      {stateVersion: '05', pos: -1, table: 'baz', rowKey: '05', op: 'r'},
      {stateVersion: '06', pos: -1, table: 'baz', rowKey: '06', op: 't'},
    );
  });
});
