import {LogContext} from '@rocicorp/logger';
import {expect, test, vi} from 'vitest';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../shared/src/logging-test-utils.ts';
import {Database} from './db.ts';

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
    // case_sensitive_like pragma, set in the Database constructor.
    [
      'warn',
      {class: 'Database', path: ':memory:', method: 'pragma'},
      ['Slow SQLite query', 0],
    ],
    // page_size pragma, also read in the constructor.
    [
      'warn',
      {class: 'Database', path: ':memory:', method: 'pragma'},
      ['Slow SQLite query', 0],
    ],
    [
      'warn',
      {class: 'Database', path: ':memory:', method: 'exec'},
      ['Slow SQLite query', 0],
    ],
    [
      'warn',
      {class: 'Database', path: ':memory:', method: 'exec'},
      ['Slow SQLite query', 0],
    ],
    [
      'warn',
      {
        class: 'Database',
        path: ':memory:',
        method: 'prepare',
      },
      ['Slow SQLite query', 0],
    ],
    [
      'warn',
      {
        class: 'Statement',
        path: ':memory:',
        sql: 'SELECT * FROM foo WHERE name = ?',
        method: 'run',
      },
      ['Slow SQLite query', 0],
    ],
    [
      'warn',
      {
        class: 'Statement',
        path: ':memory:',
        sql: 'SELECT * FROM foo WHERE name = ?',
        method: 'get',
      },
      ['Slow SQLite query', 0],
    ],
    [
      'warn',
      {
        class: 'Statement',
        path: ':memory:',
        sql: 'SELECT * FROM foo WHERE name = ?',
        method: 'all',
      },
      ['Slow SQLite query', 0],
    ],
    [
      'warn',
      {
        class: 'Database',
        path: ':memory:',
        method: 'prepare',
      },
      ['Slow SQLite query', 0],
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
      ['Slow SQLite query', 200],
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
      ['Slow SQLite query', 0],
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

test('compaction', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  db.pragma('auto_vacuum = INCREMENTAL');
  db.exec(`CREATE TABLE foo(val text);`);

  function pageCount() {
    const [{page_count: n}] = db.pragma<{page_count: number}>('page_count');
    return n;
  }
  const startingPageCount = pageCount();

  const pageOfText = 'a'.repeat(4000); // Takes about one page_size (4096 bytes)
  const stmt = db.prepare('INSERT INTO foo (val) VALUES (?)');
  for (let i = 0; i < 10; i++) {
    stmt.run(pageOfText);
  }

  expect(pageCount()).toBe(10 + startingPageCount);
  db.compact(0); // Threshold is low, but nothing to compact.
  expect(pageCount()).toBe(10 + startingPageCount);

  db.prepare('DELETE FROM foo').run();

  db.compact(11 * 4096); // Threshold too high.
  expect(pageCount()).toBe(10 + startingPageCount);

  db.compact(10 * 4096); // Threshold met.
  expect(pageCount()).toBe(startingPageCount);
});

test('CAST Buffer parameters preserve SQLite text semantics', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  db.exec(`
    CREATE TABLE direct_text (
      id INTEGER PRIMARY KEY,
      binary_value TEXT COLLATE BINARY,
      nocase_value TEXT COLLATE NOCASE,
      payload JSON
    );
    CREATE INDEX direct_text_binary_idx ON direct_text(binary_value);
    CREATE INDEX direct_text_nocase_idx ON direct_text(nocase_value);
  `);

  const rowSql = '(?, CAST(? AS TEXT), CAST(? AS TEXT), CAST(? AS TEXT))';
  db.prepare(
    `INSERT INTO direct_text VALUES ${[rowSql, rowSql, rowSql, rowSql, rowSql].join(',')}`,
  ).run([
    1,
    Buffer.from('éclair'),
    Buffer.from('Zulu'),
    Buffer.from('{"message":"雪"}'),
    2,
    Buffer.from(''),
    Buffer.from(''),
    Buffer.from('{"empty":""}'),
    3,
    null,
    null,
    null,
    4,
    Buffer.from('apple'),
    Buffer.from('apple'),
    Buffer.from('[1,2]'),
    5,
    Buffer.from('Banana'),
    Buffer.from('Banana'),
    Buffer.from('42'),
  ]);

  expect(
    db
      .prepare(
        `SELECT id, typeof(binary_value) AS binary_type,
                typeof(nocase_value) AS nocase_type,
                typeof(payload) AS payload_type
           FROM direct_text ORDER BY id`,
      )
      .all(),
  ).toEqual([
    {id: 1, binary_type: 'text', nocase_type: 'text', payload_type: 'text'},
    {id: 2, binary_type: 'text', nocase_type: 'text', payload_type: 'text'},
    {id: 3, binary_type: 'null', nocase_type: 'null', payload_type: 'null'},
    {id: 4, binary_type: 'text', nocase_type: 'text', payload_type: 'text'},
    {
      id: 5,
      binary_type: 'text',
      nocase_type: 'text',
      payload_type: 'integer',
    },
  ]);
  expect(
    db
      .prepare(
        `SELECT id, json_valid(payload) AS valid FROM direct_text
          WHERE payload IS NOT NULL ORDER BY id`,
      )
      .all(),
  ).toEqual([
    {id: 1, valid: 1},
    {id: 2, valid: 1},
    {id: 4, valid: 1},
    {id: 5, valid: 1},
  ]);
  expect(
    db
      .prepare(
        `SELECT json_extract(payload, '$.message') AS message FROM direct_text WHERE id = 1`,
      )
      .get(),
  ).toEqual({message: '雪'});

  expect(
    db
      .prepare(
        `SELECT id FROM direct_text INDEXED BY direct_text_binary_idx
          WHERE binary_value = ?`,
      )
      .all('apple'),
  ).toEqual([{id: 4}]);
  expect(
    db
      .prepare(
        `SELECT id FROM direct_text INDEXED BY direct_text_nocase_idx
          WHERE nocase_value = ?`,
      )
      .all('APPLE'),
  ).toEqual([{id: 4}]);
  expect(
    db
      .prepare(`SELECT id FROM direct_text WHERE binary_value = ?`)
      .all('APPLE'),
  ).toEqual([]);

  const binaryPlan = db
    .prepare(
      `EXPLAIN QUERY PLAN SELECT id FROM direct_text
        INDEXED BY direct_text_binary_idx WHERE binary_value = ?`,
    )
    .all<{detail: string}>('apple')
    .map(row => row.detail)
    .join('\n');
  expect(binaryPlan).toMatch(
    /SEARCH direct_text USING COVERING INDEX direct_text_binary_idx/,
  );

  expect(
    db
      .prepare(
        `SELECT binary_value FROM direct_text
          WHERE binary_value IS NOT NULL ORDER BY binary_value`,
      )
      .all(),
  ).toEqual([
    {binary_value: ''},
    {binary_value: 'Banana'},
    {binary_value: 'apple'},
    {binary_value: 'éclair'},
  ]);
  expect(
    db
      .prepare(
        `SELECT nocase_value FROM direct_text
          WHERE nocase_value IS NOT NULL ORDER BY nocase_value`,
      )
      .all(),
  ).toEqual([
    {nocase_value: ''},
    {nocase_value: 'apple'},
    {nocase_value: 'Banana'},
    {nocase_value: 'Zulu'},
  ]);
});

test('synchronous CAST binding does not retain the source Buffer', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  db.exec('CREATE TABLE direct_text (value TEXT)');
  const source = Buffer.from('stable text');

  db.prepare('INSERT INTO direct_text VALUES (CAST(? AS TEXT))').run(source);
  source.fill('x');

  expect(db.prepare('SELECT value FROM direct_text').get()).toEqual({
    value: 'stable text',
  });
});
