import {Database} from '../../../../../zqlite/src/db.js';
import {beforeEach, describe, test} from 'vitest';
import {StatementRunner} from '../../../db/statements.js';
import {expectTables} from '../../../test/lite.js';
import {
  initChangeLog,
  logDeleteOp,
  logSetOp,
  logTruncateOp,
} from './change-log.js';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.js';

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
        {stateVersion: '03', table: 'foo', rowKey: null, op: 't'},
        {stateVersion: '03', table: 'foo', rowKey: '{"a":8,"b":9}', op: 's'},
      ],
    });
  });
});
