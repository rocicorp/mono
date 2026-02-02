import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../../db/statements.ts';
import {expectTableExact} from '../../../test/lite.ts';
import {ChangeLog, CREATE_CHANGELOG_SCHEMA} from './change-log.ts';

describe('replicator/schema/change-log', () => {
  let db: StatementRunner;
  let changeLog: ChangeLog;

  beforeEach(() => {
    const conn = new Database(createSilentLogContext(), ':memory:');
    conn.exec(CREATE_CHANGELOG_SCHEMA);
    db = new StatementRunner(conn);
    changeLog = new ChangeLog(conn);
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

  test('change-log', () => {
    expect(
      changeLog.logSetOp('01', 0, 'foo', {a: 1, b: 2}, undefined),
    ).toMatchInlineSnapshot(`"{"a":1,"b":2}"`);
    expect(
      changeLog.logSetOp('01', 1, 'foo', {b: 3, a: 2}, undefined),
    ).toMatchInlineSnapshot(`"{"a":2,"b":3}"`);
    expect(
      changeLog.logSetOp('01', 2, 'bar', {b: 2, a: 1}, undefined),
    ).toMatchInlineSnapshot(`"{"a":1,"b":2}"`);
    expect(
      changeLog.logSetOp('01', 3, 'bar', {a: 2, b: 3}, undefined),
    ).toMatchInlineSnapshot(`"{"a":2,"b":3}"`);

    expectChangeLog(
      {
        stateVersion: '01',
        pos: 0,
        table: 'foo',
        rowKey: '{"a":1,"b":2}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '01',
        pos: 1,
        table: 'foo',
        rowKey: '{"a":2,"b":3}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '01',
        pos: 2,
        table: 'bar',
        rowKey: '{"a":1,"b":2}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '01',
        pos: 3,
        table: 'bar',
        rowKey: '{"a":2,"b":3}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
    );

    expect(
      changeLog.logDeleteOp('02', 0, 'bar', {b: 3, a: 2}),
    ).toMatchInlineSnapshot(`"{"a":2,"b":3}"`);

    expectChangeLog(
      {
        stateVersion: '01',
        pos: 0,
        table: 'foo',
        rowKey: '{"a":1,"b":2}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '01',
        pos: 1,
        table: 'foo',
        rowKey: '{"a":2,"b":3}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '01',
        pos: 2,
        table: 'bar',
        rowKey: '{"a":1,"b":2}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '02',
        pos: 0,
        table: 'bar',
        rowKey: '{"a":2,"b":3}',
        op: 'd',
        backfillingColumnVersions: '{}',
      },
    );

    expect(
      changeLog.logDeleteOp('03', 0, 'foo', {a: 2, b: 3}),
    ).toMatchInlineSnapshot(`"{"a":2,"b":3}"`);
    expect(
      changeLog.logSetOp('03', 1, 'foo', {b: 4, a: 5}, undefined),
    ).toMatchInlineSnapshot(`"{"a":5,"b":4}"`);
    changeLog.logTruncateOp('03', 'foo');
    expect(
      changeLog.logSetOp('03', 2, 'foo', {b: 9, a: 8}, undefined),
    ).toMatchInlineSnapshot(`"{"a":8,"b":9}"`);

    expectChangeLog(
      {
        stateVersion: '01',
        pos: 0,
        table: 'foo',
        rowKey: '{"a":1,"b":2}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '01',
        pos: 2,
        table: 'bar',
        rowKey: '{"a":1,"b":2}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '02',
        pos: 0,
        table: 'bar',
        rowKey: '{"a":2,"b":3}',
        op: 'd',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '03',
        pos: -1,
        table: 'foo',
        rowKey: '03',
        op: 't',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '03',
        pos: 0,
        table: 'foo',
        rowKey: '{"a":2,"b":3}',
        op: 'd',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '03',
        pos: 1,
        table: 'foo',
        rowKey: '{"a":5,"b":4}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '03',
        pos: 2,
        table: 'foo',
        rowKey: '{"a":8,"b":9}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
    );

    expect(
      changeLog.logDeleteOp('04', 0, 'bar', {a: 1, b: 2}),
    ).toMatchInlineSnapshot(`"{"a":1,"b":2}"`);
    expect(
      changeLog.logSetOp('04', 1, 'bar', {b: 3, a: 2}, undefined),
    ).toMatchInlineSnapshot(`"{"a":2,"b":3}"`);
    changeLog.logResetOp('04', 'bar');
    expect(
      changeLog.logSetOp('04', 2, 'bar', {b: 9, a: 7}, undefined),
    ).toMatchInlineSnapshot(`"{"a":7,"b":9}"`);

    expectChangeLog(
      {
        stateVersion: '01',
        pos: 0,
        table: 'foo',
        rowKey: '{"a":1,"b":2}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '03',
        pos: -1,
        table: 'foo',
        rowKey: '03',
        op: 't',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '03',
        pos: 0,
        table: 'foo',
        rowKey: '{"a":2,"b":3}',
        op: 'd',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '03',
        pos: 1,
        table: 'foo',
        rowKey: '{"a":5,"b":4}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '03',
        pos: 2,
        table: 'foo',
        rowKey: '{"a":8,"b":9}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '04',
        pos: -1,
        table: 'bar',
        rowKey: '04',
        op: 'r',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '04',
        pos: 0,
        table: 'bar',
        rowKey: '{"a":1,"b":2}',
        op: 'd',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '04',
        pos: 1,
        table: 'bar',
        rowKey: '{"a":2,"b":3}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '04',
        pos: 2,
        table: 'bar',
        rowKey: '{"a":7,"b":9}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
    );

    // The last table-wide op is the only one that persists.
    changeLog.logTruncateOp('05', 'baz');
    changeLog.logResetOp('05', 'baz');
    changeLog.logResetOp('05', 'baz');
    changeLog.logResetOp('05', 'baz');

    expectChangeLog(
      {
        stateVersion: '01',
        pos: 0,
        table: 'foo',
        rowKey: '{"a":1,"b":2}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '03',
        pos: -1,
        table: 'foo',
        rowKey: '03',
        op: 't',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '03',
        pos: 0,
        table: 'foo',
        rowKey: '{"a":2,"b":3}',
        op: 'd',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '03',
        pos: 1,
        table: 'foo',
        rowKey: '{"a":5,"b":4}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '03',
        pos: 2,
        table: 'foo',
        rowKey: '{"a":8,"b":9}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '04',
        pos: -1,
        table: 'bar',
        rowKey: '04',
        op: 'r',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '04',
        pos: 0,
        table: 'bar',
        rowKey: '{"a":1,"b":2}',
        op: 'd',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '04',
        pos: 1,
        table: 'bar',
        rowKey: '{"a":2,"b":3}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '04',
        pos: 2,
        table: 'bar',
        rowKey: '{"a":7,"b":9}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '05',
        pos: -1,
        table: 'baz',
        rowKey: '05',
        op: 'r',
        backfillingColumnVersions: '{}',
      },
    );

    changeLog.logResetOp('06', 'baz');
    changeLog.logResetOp('06', 'baz');
    changeLog.logTruncateOp('06', 'baz');
    changeLog.logTruncateOp('06', 'baz');

    expectChangeLog(
      {
        stateVersion: '01',
        pos: 0,
        table: 'foo',
        rowKey: '{"a":1,"b":2}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '03',
        pos: -1,
        table: 'foo',
        rowKey: '03',
        op: 't',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '03',
        pos: 0,
        table: 'foo',
        rowKey: '{"a":2,"b":3}',
        op: 'd',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '03',
        pos: 1,
        table: 'foo',
        rowKey: '{"a":5,"b":4}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '03',
        pos: 2,
        table: 'foo',
        rowKey: '{"a":8,"b":9}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '04',
        pos: -1,
        table: 'bar',
        rowKey: '04',
        op: 'r',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '04',
        pos: 0,
        table: 'bar',
        rowKey: '{"a":1,"b":2}',
        op: 'd',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '04',
        pos: 1,
        table: 'bar',
        rowKey: '{"a":2,"b":3}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '04',
        pos: 2,
        table: 'bar',
        rowKey: '{"a":7,"b":9}',
        op: 's',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '05',
        pos: -1,
        table: 'baz',
        rowKey: '05',
        op: 'r',
        backfillingColumnVersions: '{}',
      },
      {
        stateVersion: '06',
        pos: -1,
        table: 'baz',
        rowKey: '06',
        op: 't',
        backfillingColumnVersions: '{}',
      },
    );
  });

  test('change-log with backfill', () => {
    changeLog.logSetOp('123', 0, 'foo', {a: 1, b: 2}, ['c', 'b']);

    expectChangeLog({
      stateVersion: '123',
      pos: 0,
      table: 'foo',
      rowKey: '{"a":1,"b":2}',
      op: 's',
      backfillingColumnVersions: '{"c":"123","b":"123"}',
    });

    changeLog.logSetOp('2440', 0, 'foo', {a: 1, b: 2}, ['d', 'c']);

    expectChangeLog({
      stateVersion: '2440',
      pos: 0,
      table: 'foo',
      rowKey: '{"a":1,"b":2}',
      op: 's',
      backfillingColumnVersions: '{"c":"2440","b":"123","d":"2440"}',
    });

    // A set that does not write any backfilling column values preserves
    // the existing versions.
    changeLog.logSetOp('2560', 0, 'foo', {a: 1, b: 2}, []);

    expectChangeLog({
      stateVersion: '2560',
      pos: 0,
      table: 'foo',
      rowKey: '{"a":1,"b":2}',
      op: 's',
      backfillingColumnVersions: '{"c":"2440","b":"123","d":"2440"}',
    });

    // A delete op clears the backfillingColumnVersions.
    changeLog.logDeleteOp('2568', 0, 'foo', {a: 1, b: 2});

    expectChangeLog({
      stateVersion: '2568',
      pos: 0,
      table: 'foo',
      rowKey: '{"a":1,"b":2}',
      op: 'd',
      backfillingColumnVersions: '{}',
    });

    changeLog.logSetOp('2888', 0, 'foo', {a: 1, b: 2}, ['e', 'f']);

    expectChangeLog({
      stateVersion: '2888',
      pos: 0,
      table: 'foo',
      rowKey: '{"a":1,"b":2}',
      op: 's',
      backfillingColumnVersions: '{"e":"2888","f":"2888"}',
    });

    // A set that specifies `undefined` as backfilled is the signal
    // that the backfill is complete and column version data can be
    // cleared.
    changeLog.logSetOp('2990', 0, 'foo', {a: 1, b: 2}, undefined);

    expectChangeLog({
      stateVersion: '2990',
      pos: 0,
      table: 'foo',
      rowKey: '{"a":1,"b":2}',
      op: 's',
      backfillingColumnVersions: '{}',
    });
  });
});
