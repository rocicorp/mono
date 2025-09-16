/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {expectTables} from '../test/lite.ts';
import {StatementRunner} from './statements.ts';

describe('db/statements', () => {
  let db: StatementRunner;

  beforeEach(() => {
    const conn = new Database(createSilentLogContext(), ':memory:');
    conn.exec('CREATE TABLE foo(id INT PRIMARY KEY)');
    db = new StatementRunner(conn);
  });

  test('statement caching', () => {
    expect(db.statementCache.size).toBe(0);
    db.run('INSERT INTO foo(id) VALUES(?)', 123);
    expectTables(db.db, {foo: [{id: 123}]});
    expect(db.statementCache.size).toBe(1);

    db.run('INSERT INTO foo(id) VALUES(?)', 456);
    expectTables(db.db, {foo: [{id: 123}, {id: 456}]});
    expect(db.statementCache.size).toBe(1);

    expect(db.get('SELECT * FROM FOO')).toEqual({id: 123});
    expect(db.statementCache.size).toBe(2);

    expect(db.all('SELECT * FROM FOO')).toEqual([{id: 123}, {id: 456}]);
    expect(db.statementCache.size).toBe(2);
  });

  test('convenience methods', () => {
    db.beginConcurrent();

    db.run('INSERT INTO foo(id) VALUES(?)', 321);
    db.run('INSERT INTO foo(id) VALUES(?)', 456);
    expectTables(db.db, {foo: [{id: 321}, {id: 456}]});

    db.rollback();
    expectTables(db.db, {foo: []});

    db.begin();
    db.run('INSERT INTO foo(id) VALUES(?)', 987);
    db.commit();

    expectTables(db.db, {foo: [{id: 987}]});
  });
});
