/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../../db/statements.ts';
import {expectTableExact} from '../../../test/lite.ts';
import {
  initChangeLog,
  logDeleteOp,
  logResetOp,
  logSetOp,
  logTruncateOp,
} from './change-log.ts';

describe('replicator/schema/change-log', () => {
  let db: StatementRunner;

  beforeEach(() => {
    const conn = new Database(createSilentLogContext(), ':memory:');
    initChangeLog(conn);
    db = new StatementRunner(conn);
  });

  function expectChangeLog(...entries: unknown[]) {
    expectTableExact(
      db.db,
      '_zero.changeLog',
      entries,
      'number',
      'stateVersion',
      'table',
      'rowKey',
    );
  }

  test('replicator/schema/change-log', () => {
    expect(logSetOp(db, '01', 'foo', {a: 1, b: 2})).toMatchInlineSnapshot(
      `"{"a":1,"b":2}"`,
    );
    expect(logSetOp(db, '01', 'foo', {b: 3, a: 2})).toMatchInlineSnapshot(
      `"{"a":2,"b":3}"`,
    );
    expect(logSetOp(db, '01', 'bar', {b: 2, a: 1})).toMatchInlineSnapshot(
      `"{"a":1,"b":2}"`,
    );
    expect(logSetOp(db, '01', 'bar', {a: 2, b: 3})).toMatchInlineSnapshot(
      `"{"a":2,"b":3}"`,
    );

    expectChangeLog(
      {stateVersion: '01', table: 'bar', rowKey: '{"a":1,"b":2}', op: 's'},
      {stateVersion: '01', table: 'bar', rowKey: '{"a":2,"b":3}', op: 's'},
      {stateVersion: '01', table: 'foo', rowKey: '{"a":1,"b":2}', op: 's'},
      {stateVersion: '01', table: 'foo', rowKey: '{"a":2,"b":3}', op: 's'},
    );

    expect(logDeleteOp(db, '02', 'bar', {b: 3, a: 2})).toMatchInlineSnapshot(
      `"{"a":2,"b":3}"`,
    );

    expectChangeLog(
      {stateVersion: '01', table: 'bar', rowKey: '{"a":1,"b":2}', op: 's'},
      {stateVersion: '01', table: 'foo', rowKey: '{"a":1,"b":2}', op: 's'},
      {stateVersion: '01', table: 'foo', rowKey: '{"a":2,"b":3}', op: 's'},
      {stateVersion: '02', table: 'bar', rowKey: '{"a":2,"b":3}', op: 'd'},
    );

    expect(logDeleteOp(db, '03', 'foo', {a: 2, b: 3})).toMatchInlineSnapshot(
      `"{"a":2,"b":3}"`,
    );
    expect(logSetOp(db, '03', 'foo', {b: 4, a: 5})).toMatchInlineSnapshot(
      `"{"a":5,"b":4}"`,
    );
    logTruncateOp(db, '03', 'foo'); // Clears all "foo" log entries, including the previous two.
    expect(logSetOp(db, '03', 'foo', {b: 9, a: 8})).toMatchInlineSnapshot(
      `"{"a":8,"b":9}"`,
    );

    expectChangeLog(
      {stateVersion: '01', table: 'bar', rowKey: '{"a":1,"b":2}', op: 's'},
      {stateVersion: '01', table: 'foo', rowKey: '{"a":1,"b":2}', op: 's'},
      {stateVersion: '02', table: 'bar', rowKey: '{"a":2,"b":3}', op: 'd'},
      {stateVersion: '03', table: 'foo', rowKey: '', op: 't'},
      {stateVersion: '03', table: 'foo', rowKey: '{"a":8,"b":9}', op: 's'},
    );

    expect(logDeleteOp(db, '04', 'bar', {a: 1, b: 2})).toMatchInlineSnapshot(
      `"{"a":1,"b":2}"`,
    );
    expect(logSetOp(db, '04', 'bar', {b: 3, a: 2})).toMatchInlineSnapshot(
      `"{"a":2,"b":3}"`,
    );
    logResetOp(db, '04', 'bar'); // Clears all "bar" log entries, including the previous two.
    expect(logSetOp(db, '04', 'bar', {b: 9, a: 7})).toMatchInlineSnapshot(
      `"{"a":7,"b":9}"`,
    );

    expectChangeLog(
      {stateVersion: '01', table: 'foo', rowKey: '{"a":1,"b":2}', op: 's'},
      {stateVersion: '03', table: 'foo', rowKey: '', op: 't'},
      {stateVersion: '03', table: 'foo', rowKey: '{"a":8,"b":9}', op: 's'},
      {stateVersion: '04', table: 'bar', rowKey: '', op: 'r'},
      {stateVersion: '04', table: 'bar', rowKey: '{"a":7,"b":9}', op: 's'},
    );

    // The last table-wide op is the only one that persists.
    logTruncateOp(db, '05', 'baz');
    logResetOp(db, '05', 'baz');
    logResetOp(db, '05', 'baz');
    logResetOp(db, '05', 'baz');

    expectChangeLog(
      {stateVersion: '01', table: 'foo', rowKey: '{"a":1,"b":2}', op: 's'},
      {stateVersion: '03', table: 'foo', rowKey: '', op: 't'},
      {stateVersion: '03', table: 'foo', rowKey: '{"a":8,"b":9}', op: 's'},
      {stateVersion: '04', table: 'bar', rowKey: '', op: 'r'},
      {stateVersion: '04', table: 'bar', rowKey: '{"a":7,"b":9}', op: 's'},
      {stateVersion: '05', table: 'baz', rowKey: '', op: 'r'},
    );

    logResetOp(db, '06', 'baz');
    logResetOp(db, '06', 'baz');
    logTruncateOp(db, '06', 'baz');
    logTruncateOp(db, '06', 'baz');

    expectChangeLog(
      {stateVersion: '01', table: 'foo', rowKey: '{"a":1,"b":2}', op: 's'},
      {stateVersion: '03', table: 'foo', rowKey: '', op: 't'},
      {stateVersion: '03', table: 'foo', rowKey: '{"a":8,"b":9}', op: 's'},
      {stateVersion: '04', table: 'bar', rowKey: '', op: 'r'},
      {stateVersion: '04', table: 'bar', rowKey: '{"a":7,"b":9}', op: 's'},
      {stateVersion: '06', table: 'baz', rowKey: '', op: 't'},
    );
  });
});
