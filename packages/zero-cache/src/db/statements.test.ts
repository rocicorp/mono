import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {DbFile, expectTables} from '../test/lite.js';
import {StatementRunner} from './statements.js';

describe('db/statements', () => {
  let dbFile: DbFile;
  let db: StatementRunner;

  beforeEach(() => {
    dbFile = new DbFile('statements-test');
    const conn = dbFile.connect();
    conn.exec('CREATE TABLE foo(id INT PRIMARY KEY)');
    db = new StatementRunner(conn);
  });

  afterEach(async () => {
    await dbFile.unlink();
  });

  test('statement caching', () => {
    expect(db.size).toBe(0);
    db.run('INSERT INTO foo(id) VALUES(?)', 123);
    expectTables(db.db, {foo: [{id: 123}]});
    expect(db.size).toBe(1);

    db.run('INSERT INTO foo(id) VALUES(?)', 456);
    expectTables(db.db, {foo: [{id: 123}, {id: 456}]});
    expect(db.size).toBe(1);

    expect(db.get('SELECT * FROM FOO')).toEqual({id: 123});
    expect(db.size).toBe(2);

    expect(db.all('SELECT * FROM FOO')).toEqual([{id: 123}, {id: 456}]);
    expect(db.size).toBe(2);
  });

  test('convenience methods', () => {
    db.beginConcurrent();

    db.run('INSERT INTO foo(id) VALUES(?)', 321);
    db.run('INSERT INTO foo(id) VALUES(?)', 456);
    expectTables(db.db, {foo: [{id: 321}, {id: 456}]});

    db.rollback();
    expectTables(db.db, {foo: []});

    db.beginConcurrent();
    db.run('INSERT INTO foo(id) VALUES(?)', 987);
    db.commit();

    expectTables(db.db, {foo: [{id: 987}]});
  });
});
