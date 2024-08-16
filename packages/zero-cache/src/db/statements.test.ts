import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {DbFile, expectTables} from '../test/lite.js';
import {StatementCachingDatabase} from './statements.js';

describe('db/statements', () => {
  let dbFile: DbFile;
  let db: StatementCachingDatabase;

  beforeEach(() => {
    dbFile = new DbFile('statements-test');
    db = new StatementCachingDatabase(dbFile.connect());
    db.run('CREATE TABLE foo(id INT PRIMARY KEY)');
  });

  afterEach(async () => {
    await dbFile.unlink();
  });

  test('statement caching', () => {
    const stmt1 = db.prepare('INSERT INTO foo(id) VALUES(?)');
    const stmt2 = db.prepare('INSERT INTO foo(id) VALUES(?)');

    expect(stmt1).toBe(stmt2);
    stmt1.run(123);

    expectTables(db.db, {foo: [{id: 123}]});
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
