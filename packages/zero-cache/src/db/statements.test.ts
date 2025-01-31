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
