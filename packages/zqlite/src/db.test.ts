import {LogContext} from '@rocicorp/logger';
import {expect, test, vi} from 'vitest';
import {TestLogSink} from '../../shared/src/logging-test-utils.js';
import {Database} from './db.js';

test('slow queries are logged', () => {
  vi.useFakeTimers();
  const sink = new TestLogSink();
  const lc = new LogContext('debug', undefined, sink);

  // threshold is 0 so all queries will be logged
  const db = new Database(lc, ':memory:', undefined, 0);

  db.exec('CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)');
  db.exec(/*sql*/ `INSERT INTO foo (name) VALUES ('Alice'), ('Bob')`);

  const stmt = db.prepare('SELECT * FROM foo WHERE name = ?');

  stmt.run('Alice');
  stmt.get('Alice');
  stmt.all('Alice');

  const stmt2 = db.prepare('SELECT * FROM foo');

  for (const _ of stmt2.iterate()) {
    vi.advanceTimersByTime(100);
  }

  expect(sink.messages).toEqual([
    [
      'warn',
      {class: 'Database', path: ':memory:', method: 'exec'},
      ['Slow query', 0],
    ],
    [
      'warn',
      {class: 'Database', path: ':memory:', method: 'exec'},
      ['Slow query', 0],
    ],
    [
      'warn',
      {
        class: 'Database',
        path: ':memory:',
        method: 'prepare',
      },
      ['Slow query', 0],
    ],
    [
      'warn',
      {
        class: 'Statement',
        path: ':memory:',
        sql: 'SELECT * FROM foo WHERE name = ?',
        method: 'run',
      },
      ['Slow query', 0],
    ],
    [
      'warn',
      {
        class: 'Statement',
        path: ':memory:',
        sql: 'SELECT * FROM foo WHERE name = ?',
        method: 'get',
      },
      ['Slow query', 0],
    ],
    [
      'warn',
      {
        class: 'Statement',
        path: ':memory:',
        sql: 'SELECT * FROM foo WHERE name = ?',
        method: 'all',
      },
      ['Slow query', 0],
    ],
    [
      'warn',
      {
        class: 'Database',
        path: ':memory:',
        method: 'prepare',
      },
      ['Slow query', 0],
    ],
    [
      'warn',
      {
        class: 'Statement',
        path: ':memory:',
        sql: 'SELECT * FROM foo',
        method: 'iterate',
        type: 'total',
      },
      ['Slow query', 200],
    ],
    [
      'warn',
      {
        class: 'Statement',
        path: ':memory:',
        sql: 'SELECT * FROM foo',
        method: 'iterate',
        type: 'sqlite',
      },
      ['Slow query', 0],
    ],
  ]);
});

test('sql errors are annotated with sql', () => {
  const sink = new TestLogSink();
  const lc = new LogContext('debug', undefined, sink);

  // threshold is 0 so all queries will be logged
  const db = new Database(lc, ':memory:');

  let result;
  try {
    db.exec('CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT))');
  } catch (e) {
    result = String(e);
  }
  expect(result).toBe(
    'SqliteError: near ")": syntax error: CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT))',
  );

  try {
    db.prepare('SELECT * FROM foo WHERE name = ??');
  } catch (e) {
    result = String(e);
  }
  expect(result).toBe(
    'SqliteError: near "?": syntax error: SELECT * FROM foo WHERE name = ??',
  );

  try {
    db.pragma('&Df6(&');
  } catch (e) {
    result = String(e);
  }
  expect(result).toBe('SqliteError: near "&": syntax error: &Df6(&');
});
