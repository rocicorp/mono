import {beforeEach, describe, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.js';
import {Database} from '../../../../../zqlite/src/db.js';
import {StatementRunner} from '../../../db/statements.js';
import {expectTables} from '../../../test/lite.js';
import {
  initChangeLog,
  logDeleteOp,
  logResetOp,
  logSetOp,
  logTruncateOp,
} from './change-log.js';

describe('replicator/schema/change-log', () => {
  let db: StatementRunner;

  beforeEach(() => {
    const conn = new Database(createSilentLogContext(), ':memory:');
    initChangeLog(conn);
    db = new StatementRunner(conn);
  });

  test('replicator/schema/change-log', () => {
    logSetOp(db, '01', 'foo', {a: 1, b: 2});
    logSetOp(db, '01', 'foo', {b: 3, a: 2}); // Note: rowKey JSON should have sorted keys
    logSetOp(db, '01', 'bar', {b: 2, a: 1}); // Note: rowKey JSON should have sorted keys
    logSetOp(db, '01', 'bar', {a: 2, b: 3});

    expectTables(db.db, {
      ['_zero.ChangeLog']: [
        {stateVersion: '01', table: 'bar', rowKey: '{"a":1,"b":2}', op: 's'},
        {stateVersion: '01', table: 'bar', rowKey: '{"a":2,"b":3}', op: 's'},
        {stateVersion: '01', table: 'foo', rowKey: '{"a":1,"b":2}', op: 's'},
        {stateVersion: '01', table: 'foo', rowKey: '{"a":2,"b":3}', op: 's'},
      ],
    });

    logDeleteOp(db, '02', 'bar', {a: 2, b: 3});

    expectTables(db.db, {
      ['_zero.ChangeLog']: [
        {stateVersion: '01', table: 'bar', rowKey: '{"a":1,"b":2}', op: 's'},
        {stateVersion: '01', table: 'foo', rowKey: '{"a":1,"b":2}', op: 's'},
        {stateVersion: '01', table: 'foo', rowKey: '{"a":2,"b":3}', op: 's'},
        {stateVersion: '02', table: 'bar', rowKey: '{"a":2,"b":3}', op: 'd'},
      ],
    });

    logDeleteOp(db, '03', 'foo', {a: 2, b: 3});
    logSetOp(db, '03', 'foo', {b: 4, a: 5});
    logTruncateOp(db, '03', 'foo'); // Clears all "foo" log entries, including the previous two.
    logSetOp(db, '03', 'foo', {b: 9, a: 8});

    expectTables(db.db, {
      ['_zero.ChangeLog']: [
        {stateVersion: '01', table: 'bar', rowKey: '{"a":1,"b":2}', op: 's'},
        {stateVersion: '02', table: 'bar', rowKey: '{"a":2,"b":3}', op: 'd'},
        {stateVersion: '03', table: 'foo', rowKey: '', op: 't'},
        {stateVersion: '03', table: 'foo', rowKey: '{"a":8,"b":9}', op: 's'},
      ],
    });

    logDeleteOp(db, '04', 'bar', {a: 1, b: 2});
    logSetOp(db, '04', 'bar', {b: 3, a: 2});
    logResetOp(db, '04', 'bar'); // Clears all "bar" log entries, including the previous two.
    logSetOp(db, '04', 'bar', {b: 9, a: 7});

    expectTables(db.db, {
      ['_zero.ChangeLog']: [
        {stateVersion: '03', table: 'foo', rowKey: '', op: 't'},
        {stateVersion: '03', table: 'foo', rowKey: '{"a":8,"b":9}', op: 's'},
        {stateVersion: '04', table: 'bar', rowKey: null, op: 'r'},
        {stateVersion: '04', table: 'bar', rowKey: '{"a":7,"b":9}', op: 's'},
      ],
    });

    // Test that table-wide ops preserve each other and reset always sort before truncates.
    logTruncateOp(db, '05', 'baz');
    logResetOp(db, '05', 'baz');
    logResetOp(db, '05', 'baz');
    logResetOp(db, '05', 'baz');

    expectTables(db.db, {
      ['_zero.ChangeLog']: [
        {stateVersion: '03', table: 'foo', rowKey: '', op: 't'},
        {stateVersion: '03', table: 'foo', rowKey: '{"a":8,"b":9}', op: 's'},
        {stateVersion: '04', table: 'bar', rowKey: null, op: 'r'},
        {stateVersion: '04', table: 'bar', rowKey: '{"a":7,"b":9}', op: 's'},
        {stateVersion: '05', table: 'baz', rowKey: null, op: 'r'},
        {stateVersion: '05', table: 'baz', rowKey: '', op: 't'},
      ],
    });

    logResetOp(db, '06', 'baz');
    logResetOp(db, '06', 'baz');
    logTruncateOp(db, '06', 'baz');
    logTruncateOp(db, '06', 'baz');

    expectTables(db.db, {
      ['_zero.ChangeLog']: [
        {stateVersion: '03', table: 'foo', rowKey: '', op: 't'},
        {stateVersion: '03', table: 'foo', rowKey: '{"a":8,"b":9}', op: 's'},
        {stateVersion: '04', table: 'bar', rowKey: null, op: 'r'},
        {stateVersion: '04', table: 'bar', rowKey: '{"a":7,"b":9}', op: 's'},
        {stateVersion: '06', table: 'baz', rowKey: null, op: 'r'},
        {stateVersion: '06', table: 'baz', rowKey: '', op: 't'},
      ],
    });
  });
});
