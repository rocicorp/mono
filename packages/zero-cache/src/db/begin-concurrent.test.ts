import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {DbFile} from '../test/lite.js';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';

describe('db/begin-concurrent', () => {
  let dbFile: DbFile;
  const lc = createSilentLogContext();
  beforeEach(() => {
    dbFile = new DbFile('begin-concurrent');
    const conn = dbFile.connect(lc);
    conn.pragma('journal_mode = WAL');
    conn.pragma('synchronous = NORMAL');
    conn.exec('CREATE TABLE foo(id INTEGER PRIMARY KEY);');
    conn.close();
  });

  afterEach(async () => {
    await dbFile.unlink();
  });

  test('independent, concurrent actions before commit', () => {
    const conn1 = dbFile.connect(lc);
    conn1.pragma('journal_mode = WAL');
    conn1.prepare('BEGIN CONCURRENT').run();

    const conn2 = dbFile.connect(lc);
    conn2.pragma('journal_mode = WAL');
    conn2.prepare('BEGIN CONCURRENT').run();

    conn1.prepare('INSERT INTO foo(id) VALUES(1)').run();
    expect(conn1.prepare('SELECT * FROM foo').all()).toEqual([{id: 1}]);

    conn2.prepare('INSERT INTO foo(id) VALUES(2)').run();
    expect(conn2.prepare('SELECT * FROM foo').all()).toEqual([{id: 2}]);

    conn1.prepare('COMMIT').run();
    conn2.prepare('ROLLBACK').run();

    conn1.close();
    conn2.close();
  });

  test('begin concurrent is deferred', () => {
    const conn1 = dbFile.connect(lc);
    conn1.pragma('journal_mode = WAL');
    conn1.prepare('BEGIN CONCURRENT').run();

    const conn2 = dbFile.connect(lc);
    conn2.pragma('journal_mode = WAL');

    // Note: Like BEGIN DEFERRED, the BEGIN CONCURRENT transaction does not actually start until
    // the database is first accessed
    conn2.prepare('BEGIN CONCURRENT').run();

    conn1.prepare('INSERT INTO foo(id) VALUES(1)').run();
    conn1.prepare('COMMIT').run();

    expect(conn1.prepare('SELECT * FROM foo').all()).toEqual([{id: 1}]);

    // So the conn2 transaction actually starts here, after conn1 committed.
    conn2.prepare('INSERT INTO foo(id) VALUES(2)').run();
    expect(conn2.prepare('SELECT * FROM foo').all()).toEqual([
      {id: 1},
      {id: 2},
    ]);

    conn2.prepare('ROLLBACK').run();

    conn1.close();
    conn2.close();
  });

  test('simulate immediate', () => {
    const conn1 = dbFile.connect(lc);
    conn1.pragma('journal_mode = WAL');
    conn1.prepare('BEGIN CONCURRENT').run();
    // Force the transaction to start immediately by accessing the database.
    expect(conn1.prepare('SELECT * FROM foo').all()).toEqual([]);

    const conn2 = dbFile.connect(lc);
    conn2.pragma('journal_mode = WAL');
    conn2.prepare('BEGIN CONCURRENT').run();
    // Force the transaction to start immediately by accessing the database.
    expect(conn2.prepare('SELECT * FROM foo').all()).toEqual([]);

    conn1.prepare('INSERT INTO foo(id) VALUES(1)').run();
    conn1.prepare('COMMIT').run();

    expect(conn1.prepare('SELECT * FROM foo').all()).toEqual([{id: 1}]);

    // Should not see commit from conn1.
    conn2.prepare('INSERT INTO foo(id) VALUES(2)').run();
    expect(conn2.prepare('SELECT * FROM foo').all()).toEqual([{id: 2}]);

    conn2.prepare('ROLLBACK').run();

    conn1.close();
    conn2.close();
  });

  test('begin concurrent with savepoints', () => {
    const conn1 = dbFile.connect(lc);
    conn1.pragma('journal_mode = WAL');
    conn1.prepare('BEGIN CONCURRENT').run();
    // Force the transaction to start immediately by accessing the database.
    expect(conn1.prepare('SELECT * FROM foo').all()).toEqual([]);

    const conn2 = dbFile.connect(lc);
    conn2.pragma('journal_mode = WAL');
    conn2.prepare('BEGIN CONCURRENT').run();
    // Force the transaction to start immediately by accessing the database.
    expect(conn2.prepare('SELECT * FROM foo').all()).toEqual([]);

    conn1.prepare('INSERT INTO foo(id) VALUES(1)').run();
    conn1.prepare('COMMIT').run();

    expect(conn1.prepare('SELECT * FROM foo').all()).toEqual([{id: 1}]);

    // Should not see commit from conn1.
    conn2.prepare('SAVEPOINT foobar').run();
    conn2.prepare('INSERT INTO foo(id) VALUES(2)').run();
    expect(conn2.prepare('SELECT * FROM foo').all()).toEqual([{id: 2}]);

    // Should rollback to the savepoint, which should still exclude conn1's commit.
    conn2.prepare('ROLLBACK TO foobar').run();
    expect(conn2.prepare('SELECT * FROM foo').all()).toEqual([]);

    conn2.prepare('ROLLBACK').run();

    conn1.close();
    conn2.close();
  });
});
