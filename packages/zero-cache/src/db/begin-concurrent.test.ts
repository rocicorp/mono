import {existsSync, statSync} from 'node:fs';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {DbFile} from '../test/lite.ts';

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

  afterEach(() => {
    dbFile.delete();
  });

  const startSpillingWriter = () => {
    const writer = dbFile.connect(lc);
    writer.pragma('journal_mode = DELETE');
    writer.pragma('journal_mode = WAL2');
    writer.pragma('cache_size = 10');
    writer.pragma('cache_spill = ON');
    writer.exec('CREATE TABLE large(id INTEGER PRIMARY KEY, value BLOB)');

    const journalBytes = () =>
      [`${dbFile.path}-wal`, `${dbFile.path}-wal2`].reduce(
        (total, path) => total + (existsSync(path) ? statSync(path).size : 0),
        0,
      );
    const before = journalBytes();

    writer.prepare('BEGIN IMMEDIATE').run();
    writer.exec(`
      WITH RECURSIVE rows(id) AS (
        VALUES(1)
        UNION ALL
        SELECT id + 1 FROM rows WHERE id < 2000
      )
      INSERT INTO large SELECT id, randomblob(4000) FROM rows;
    `);

    return {writer, spilledBytes: journalBytes() - before};
  };

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

  test('immediate writer commits alongside a concurrent snapshot', () => {
    const snapshot = dbFile.connect(lc);
    snapshot.pragma('journal_mode = DELETE');
    snapshot.pragma('journal_mode = WAL2');
    snapshot.prepare('BEGIN CONCURRENT').run();
    expect(snapshot.prepare('SELECT * FROM foo').all()).toEqual([]);
    snapshot.prepare('INSERT INTO foo(id) VALUES(2)').run();

    const writer = dbFile.connect(lc);
    writer.pragma('journal_mode = WAL2');
    writer.prepare('BEGIN IMMEDIATE').run();
    writer.prepare('INSERT INTO foo(id) VALUES(1)').run();
    writer.prepare('COMMIT').run();

    // The snapshot retains its historic view and private change.
    expect(snapshot.prepare('SELECT * FROM foo').all()).toEqual([{id: 2}]);

    const current = dbFile.connect(lc);
    current.pragma('journal_mode = WAL2');
    expect(current.prepare('SELECT * FROM foo').all()).toEqual([{id: 1}]);

    snapshot.prepare('ROLLBACK').run();
    expect(current.prepare('SELECT * FROM foo').all()).toEqual([{id: 1}]);

    snapshot.close();
    writer.close();
    current.close();
  });

  test('concurrent snapshot proceeds alongside an immediate writer', () => {
    const writer = dbFile.connect(lc);
    writer.pragma('journal_mode = DELETE');
    writer.pragma('journal_mode = WAL2');
    writer.prepare('BEGIN IMMEDIATE').run();
    writer.prepare('INSERT INTO foo(id) VALUES(1)').run();

    const snapshot = dbFile.connect(lc);
    snapshot.pragma('journal_mode = WAL2');
    snapshot.prepare('BEGIN CONCURRENT').run();

    // The snapshot can progress while the writer has an uncommitted change.
    expect(snapshot.prepare('SELECT * FROM foo').all()).toEqual([]);
    snapshot.prepare('INSERT INTO foo(id) VALUES(2)').run();
    expect(snapshot.prepare('SELECT * FROM foo').all()).toEqual([{id: 2}]);

    snapshot.prepare('ROLLBACK').run();
    writer.prepare('COMMIT').run();
    expect(writer.prepare('SELECT * FROM foo').all()).toEqual([{id: 1}]);

    snapshot.close();
    writer.close();
  });

  test('immediate writer spills dirty pages before commit', () => {
    const {writer, spilledBytes} = startSpillingWriter();

    // Spilled pages are written as uncommitted WAL frames. A concurrent
    // transaction would retain them in memory until commit instead.
    expect(spilledBytes).toBeGreaterThan(1024 * 1024);

    writer.prepare('ROLLBACK').run();
    expect(writer.prepare('SELECT count(*) AS count FROM large').get()).toEqual(
      {count: 0},
    );
    writer.close();
  });

  test('concurrent snapshot proceeds alongside spilled immediate writes', () => {
    const {writer, spilledBytes} = startSpillingWriter();
    expect(spilledBytes).toBeGreaterThan(1024 * 1024);

    const snapshot = dbFile.connect(lc);
    snapshot.pragma('journal_mode = WAL2');
    snapshot.prepare('BEGIN CONCURRENT').run();

    // The snapshot ignores the writer's uncommitted WAL frames and can make a
    // private change of its own.
    expect(
      snapshot.prepare('SELECT count(*) AS count FROM large').get(),
    ).toEqual({count: 0});
    snapshot.prepare('INSERT INTO large VALUES(2001, randomblob(4000))').run();
    expect(
      snapshot.prepare('SELECT count(*) AS count FROM large').get(),
    ).toEqual({count: 1});

    writer.prepare('COMMIT').run();
    expect(
      snapshot.prepare('SELECT count(*) AS count FROM large').get(),
    ).toEqual({count: 1});
    expect(writer.prepare('SELECT count(*) AS count FROM large').get()).toEqual(
      {count: 2000},
    );

    snapshot.prepare('ROLLBACK').run();
    snapshot.close();
    writer.close();
  });
});
