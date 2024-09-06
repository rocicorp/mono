import {expect, test, vi} from 'vitest';
import {Database} from './db.js';
import {TestLogSink} from 'shared/src/logging-test-utils.js';
import {LogContext} from '@rocicorp/logger';

test('slow queries are logged', () => {
  vi.useFakeTimers();
  const sink = new TestLogSink();
  const lc = new LogContext('debug', undefined, sink);

  // threshold is 0 so all queries will be logged
  const db = new Database(lc, ':memory:', 0);

  db.exec('CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)');
  db.exec('INSERT INTO foo (name) VALUES ("Alice"), ("Bob")');

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
      'error',
      {component: 'Database', path: ':memory:', method: 'exec'},
      ['Slow query', 0],
    ],
    [
      'error',
      {component: 'Database', path: ':memory:', method: 'exec'},
      ['Slow query', 0],
    ],
    [
      'error',
      {
        component: 'Statement',
        path: ':memory:',
        sql: 'SELECT * FROM foo WHERE name = ?',
        method: 'run',
      },
      ['Slow query', 0],
    ],
    [
      'error',
      {
        component: 'Statement',
        path: ':memory:',
        sql: 'SELECT * FROM foo WHERE name = ?',
        method: 'get',
      },
      ['Slow query', 0],
    ],
    [
      'error',
      {
        component: 'Statement',
        path: ':memory:',
        sql: 'SELECT * FROM foo WHERE name = ?',
        method: 'all',
      },
      ['Slow query', 0],
    ],
    [
      'error',
      {
        component: 'Statement',
        path: ':memory:',
        sql: 'SELECT * FROM foo',
        method: 'iterate',
        type: 'total',
      },
      ['Slow query', 200],
    ],
    [
      'error',
      {
        component: 'Statement',
        path: ':memory:',
        sql: 'SELECT * FROM foo',
        method: 'iterate',
        type: 'sqlite',
      },
      ['Slow query', 0],
    ],
  ]);
});
