import {describe, expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {assert} from '../../shared/src/asserts.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import type {Ordering} from '../../zero-protocol/src/ast.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../zero-protocol/src/primary-key.ts';
import {Catch} from '../../zql/src/ivm/catch.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
  type SourceChange,
} from '../../zql/src/ivm/source.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import {Database} from './db.ts';
import {PendingDelta} from './pending-delta.ts';
import {TableSource} from './table-source.ts';

const lc = createSilentLogContext();

const columns = {
  id: {type: 'string'},
  a: {type: 'number'},
  b: {type: 'string'},
} as const;

const byID: Ordering = [['id', 'asc']];

test('touched keys include removed rows, for single and composite keys', () => {
  const single = new PendingDelta(['id']);
  single.set({id: 'a', v: 1});
  single.delete({id: 'b', v: 2});
  expect([...single.touchedKeys()]).toEqual([{id: 'a', v: 1}, {id: 'b'}]);
  expect([...single.liveRows()]).toEqual([{id: 'a', v: 1}]);

  const composite = new PendingDelta(['x', 'y']);
  composite.delete({x: 'a', y: 1, v: 1});
  composite.set({x: 'a', y: 2, v: 2});
  composite.set({x: 'b', y: 1, v: 3});
  composite.delete({x: 'b', y: 1, v: 3});
  expect([...composite.touchedKeys()]).toEqual([
    {x: 'a', y: 1},
    {x: 'a', y: 2, v: 2},
    {x: 'b', y: 1},
  ]);
  expect([...composite.liveRows()]).toEqual([{x: 'a', y: 2, v: 2}]);
});

test('overrides answers by the whole composite key', () => {
  const composite = new PendingDelta(['x', 'y']);
  composite.set({x: 'a', y: 1, v: 1});
  composite.delete({x: 'b', y: 2, v: 2});
  expect(composite.overrides({x: 'a', y: 1, v: 0})).toBe(true);
  expect(composite.overrides({x: 'b', y: 2, v: 0})).toBe(true);
  // A shared first column is not enough.
  expect(composite.overrides({x: 'a', y: 2, v: 0})).toBe(false);
  expect(composite.overrides({x: 'c', y: 1, v: 0})).toBe(false);
  composite.clear();
  expect(composite.overrides({x: 'a', y: 1, v: 0})).toBe(false);
});

test('rowsFor begins at the start row', () => {
  const delta = new PendingDelta(['id']);
  for (const [id, g] of [
    ['a', 1],
    ['b', 2],
    ['c', 1],
    ['d', 2],
    ['e', 1],
  ] as const) {
    delta.set({id, g});
  }
  const ids = (
    constraint: {g: number} | undefined,
    reverse: boolean,
    start: Row | undefined,
  ) =>
    Array.from(
      delta.rowsFor(byID, constraint, reverse, undefined, undefined, start),
      r => r.id,
    );

  expect(ids(undefined, false, {id: 'c', g: 1})).toEqual(['c', 'd', 'e']);
  expect(ids(undefined, true, {id: 'c', g: 1})).toEqual(['c', 'b', 'a']);
  expect(ids({g: 1}, false, {id: 'b', g: 1})).toEqual(['c', 'e']);
  expect(ids({g: 1}, true, {id: 'd', g: 1})).toEqual(['c', 'a']);
  // A start outside the constraint span leaves the caller to apply it.
  expect(ids({g: 1}, false, {id: 'b', g: 2})).toEqual(['a', 'c', 'e']);
});

test('pending byte estimate tracks replacements, tombstones, indexes, and clear', () => {
  const delta = new PendingDelta(['id']);
  const row = {id: 'a', value: {items: ['small', 1, true, null]}};
  expect(delta.estimatedBytes).toBe(0);
  delta.set(row);
  const originalBytes = delta.estimatedBytes;
  expect(originalBytes).toBeGreaterThan(0);
  delta.set(row);
  expect(delta.estimatedBytes).toBe(originalBytes);
  delta.set({...row, extra: 'x'.repeat(1024)});
  expect(delta.estimatedBytes).toBeGreaterThan(originalBytes + 2048);
  delta.set(row);
  expect(delta.estimatedBytes).toBe(originalBytes);

  [...delta.rowsFor(byID, undefined, undefined, undefined, undefined)];
  const indexedBytes = delta.estimatedBytes;
  expect(indexedBytes).toBeGreaterThan(originalBytes);
  [...delta.rowsFor(byID, undefined, undefined, undefined, undefined)];
  expect(delta.estimatedBytes).toBe(indexedBytes);
  delta.delete(row);
  expect(delta.estimatedBytes).toBeGreaterThan(0);
  expect(delta.estimatedBytes).toBeLessThan(indexedBytes);
  const tombstoneBytes = delta.estimatedBytes;
  delta.delete(row);
  expect(delta.estimatedBytes).toBe(tombstoneBytes);
  delta.set(row);
  expect(delta.estimatedBytes).toBe(indexedBytes);
  delta.clear();
  expect(delta.estimatedBytes).toBe(0);
  delta.delete(row);
  expect(delta.estimatedBytes).toBeGreaterThan(0);
  expect(delta.estimatedBytes).toBeLessThan(originalBytes);
});

function newDB(rows: readonly Row[], ddl?: string, table = 'foo') {
  const db = new Database(lc, ':memory:');
  db.exec(ddl ?? `CREATE TABLE foo (id TEXT PRIMARY KEY, a, b);`);
  const cols = rows.length > 0 ? Object.keys(rows[0]) : [];
  if (cols.length > 0) {
    const stmt = db.prepare(
      `INSERT INTO ${table} (${cols.join(',')}) VALUES (${cols
        .map(() => '?')
        .join(',')})`,
    );
    for (const row of rows) {
      stmt.run(...cols.map(c => row[c] as unknown));
    }
  }
  return db;
}

function newSource(
  db: Database,
  deferWrites: boolean,
  table = 'foo',
  cols: Record<string, {type: string}> = columns,
  primaryKey: PrimaryKey = ['id'],
) {
  return new TableSource(
    lc,
    testLogConfig,
    db,
    table,
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    cols as any,
    primaryKey,
    () => false,
    {deferWrites},
  );
}

/** Every row the source vends, through a real connection. */
function fetchAll(source: TableSource, sort: Ordering = byID): Row[] {
  const input = source.connect(sort);
  const out = new Catch(input);
  input.setOutput(out);
  try {
    return out.fetch({}).map(r => {
      assert(r !== 'yield', 'Expected a row, not a yield');
      return r.row;
    });
  } finally {
    input.destroy();
  }
}

/** What is actually durable in the backing database. */
function rowsInDB(db: Database, table = 'foo'): Row[] {
  return db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all<Row>();
}

function apply(
  source: TableSource,
  changes: readonly SourceChange[],
  sort: Ordering = byID,
) {
  // A connected output is what makes the source fan each change out and take
  // the mid-push fetch paths, which is where the batch overlay is read.
  const input = source.connect(sort);
  const out = new Catch(input);
  input.setOutput(out);
  try {
    for (const change of changes) {
      consume(source.push(change));
    }
  } finally {
    input.destroy();
  }
}

const initial: Row[] = [
  {id: 'a', a: 1, b: 'one'},
  {id: 'b', a: 2, b: 'two'},
  {id: 'c', a: 3, b: 'three'},
];

describe('deferred writes leave the backing database alone', () => {
  test('adds, edits and removes are visible to fetch but not to the db', () => {
    const db = newDB(initial);
    const source = newSource(db, true);

    apply(source, [
      makeSourceChangeAdd({id: 'd', a: 4, b: 'four'}),
      makeSourceChangeEdit({id: 'b', a: 20, b: 'twenty'}, initial[1]),
      makeSourceChangeRemove(initial[2]),
    ]);

    expect(fetchAll(source)).toEqual([
      {id: 'a', a: 1, b: 'one'},
      {id: 'b', a: 20, b: 'twenty'},
      {id: 'd', a: 4, b: 'four'},
    ]);
    // The snapshot the source is reading is untouched -- that is the whole
    // point of the mode.
    expect(rowsInDB(db)).toEqual(initial);
  });

  test('write-through mode does write, and both modes agree', () => {
    const deferredDB = newDB(initial);
    const writeDB = newDB(initial);
    const deferred = newSource(deferredDB, true);
    const writeThrough = newSource(writeDB, false);

    const changes = [
      makeSourceChangeAdd({id: 'd', a: 4, b: 'four'}),
      makeSourceChangeEdit({id: 'b', a: 20, b: 'twenty'}, initial[1]),
      makeSourceChangeRemove(initial[2]),
    ];
    apply(deferred, changes);
    apply(writeThrough, changes);

    expect(fetchAll(deferred)).toEqual(fetchAll(writeThrough));
    expect(rowsInDB(deferredDB)).toEqual(initial);
    expect(rowsInDB(writeDB)).not.toEqual(initial);
  });
});

describe('the batch coalesces by primary key', () => {
  test.for([
    {
      name: 'add then edit then remove nets to nothing',
      changes: [
        makeSourceChangeAdd({id: 'd', a: 4, b: 'four'}),
        makeSourceChangeEdit(
          {id: 'd', a: 40, b: 'forty'},
          {
            id: 'd',
            a: 4,
            b: 'four',
          },
        ),
        makeSourceChangeRemove({id: 'd', a: 40, b: 'forty'}),
      ],
      expected: initial,
    },
    {
      name: 'remove then add nets to the added value',
      changes: [
        makeSourceChangeRemove(initial[0]),
        makeSourceChangeAdd({id: 'a', a: 100, b: 'hundred'}),
      ],
      expected: [{id: 'a', a: 100, b: 'hundred'}, initial[1], initial[2]],
    },
    {
      name: 'repeated edits keep only the last value',
      changes: [
        makeSourceChangeEdit({id: 'a', a: 10, b: 'ten'}, initial[0]),
        makeSourceChangeEdit(
          {id: 'a', a: 11, b: 'eleven'},
          {
            id: 'a',
            a: 10,
            b: 'ten',
          },
        ),
      ],
      expected: [{id: 'a', a: 11, b: 'eleven'}, initial[1], initial[2]],
    },
  ])('$name', ({changes, expected}) => {
    const db = newDB(initial);
    const source = newSource(db, true);
    apply(source, changes);
    expect(fetchAll(source)).toEqual(expected);
    expect(rowsInDB(db)).toEqual(initial);
  });

  test('an edit that moves the primary key is a remove plus an add', () => {
    const db = newDB(initial);
    const source = newSource(db, true);
    apply(source, [
      makeSourceChangeEdit({id: 'z', a: 1, b: 'one'}, initial[0]),
    ]);
    expect(fetchAll(source)).toEqual([
      initial[1],
      initial[2],
      {id: 'z', a: 1, b: 'one'},
    ]);
  });

  test('an edit keeps columns the change did not mention', () => {
    const db = newDB(initial);
    const source = newSource(db, true);
    // `UPDATE` only sets the non-primary columns present in the change, so a
    // partial edit must not blank out `b`.
    apply(source, [
      makeSourceChangeEdit({id: 'a', a: 99, b: 'one'}, initial[0]),
    ]);
    expect(fetchAll(source)[0]).toEqual({id: 'a', a: 99, b: 'one'});
  });
});

describe('ordering and constraints see the batch', () => {
  test('batch rows splice into a non-primary sort', () => {
    const db = newDB(initial);
    const source = newSource(db, true);
    apply(source, [
      makeSourceChangeAdd({id: 'd', a: 0, b: 'zero'}),
      makeSourceChangeEdit({id: 'c', a: -1, b: 'three'}, initial[2]),
    ]);
    const sort: Ordering = [
      ['a', 'asc'],
      ['id', 'asc'],
    ];
    expect(fetchAll(source, sort).map(r => r.id)).toEqual(['c', 'd', 'a', 'b']);
  });

  test('a constrained fetch sees only the batch rows that match', () => {
    const db = newDB(initial);
    const source = newSource(db, true);
    apply(source, [
      makeSourceChangeAdd({id: 'd', a: 1, b: 'four'}),
      makeSourceChangeAdd({id: 'e', a: 2, b: 'five'}),
    ]);
    const input = source.connect(byID);
    const out = new Catch(input);
    input.setOutput(out);
    const rows = out.fetch({constraint: {a: 1}}).map(r => {
      assert(r !== 'yield', 'Expected a row, not a yield');
      return r.row.id;
    });
    input.destroy();
    expect(rows).toEqual(['a', 'd']);
  });

  test('a reversed fetch merges the batch in reverse', () => {
    const db = newDB(initial);
    const source = newSource(db, true);
    apply(source, [makeSourceChangeAdd({id: 'bb', a: 9, b: 'nine'})]);
    const input = source.connect(byID);
    const out = new Catch(input);
    input.setOutput(out);
    const rows = out.fetch({reverse: true}).map(r => {
      assert(r !== 'yield', 'Expected a row, not a yield');
      return r.row.id;
    });
    input.destroy();
    expect(rows).toEqual(['c', 'bb', 'b', 'a']);
  });
});

describe('a fetch with a start', () => {
  test('sees the batch rows from the start on', () => {
    const db = newDB(initial);
    const source = newSource(db, true);
    apply(source, [
      makeSourceChangeAdd({id: 'aa', a: 1, b: 'x'}),
      makeSourceChangeAdd({id: 'bb', a: 1, b: 'y'}),
      makeSourceChangeAdd({id: 'd', a: 1, b: 'z'}),
      makeSourceChangeRemove(initial[2]),
    ]);
    const input = source.connect(byID);
    const out = new Catch(input);
    input.setOutput(out);
    const fetch = (basis: 'at' | 'after', reverse = false) =>
      out
        .fetch({
          constraint: {a: 1},
          start: {row: {id: 'aa', a: 1, b: 'x'}, basis},
          reverse,
        })
        .map(r => {
          assert(r !== 'yield', 'Expected a row, not a yield');
          return r.row.id;
        });
    expect(fetch('at')).toEqual(['aa', 'bb', 'd']);
    expect(fetch('after')).toEqual(['bb', 'd']);
    expect(fetch('at', true)).toEqual(['aa', 'a']);
    expect(fetch('after', true)).toEqual(['a']);
    input.destroy();
  });
});

describe('compound primary keys', () => {
  const compoundColumns = {
    a: {type: 'number'},
    b: {type: 'number'},
    c: {type: 'string'},
  } as const;
  const compoundRows: Row[] = [
    {a: 1, b: 1, c: 'x'},
    {a: 1, b: 2, c: 'y'},
    {a: 2, b: 1, c: 'z'},
  ];
  const compoundSort: Ordering = [
    ['a', 'asc'],
    ['b', 'asc'],
  ];

  test('the batch keys rows on every primary key column', () => {
    const db = newDB(
      compoundRows,
      `CREATE TABLE foo (a, b, c, PRIMARY KEY (a, b));`,
    );
    const source = newSource(db, true, 'foo', compoundColumns, ['a', 'b']);

    apply(
      source,
      [
        // Same `a`, different `b` -- a distinct row, not an overwrite.
        makeSourceChangeAdd({a: 1, b: 3, c: 'new'}),
        makeSourceChangeRemove(compoundRows[0]),
      ],
      compoundSort,
    );

    expect(fetchAll(source, compoundSort)).toEqual([
      {a: 1, b: 2, c: 'y'},
      {a: 1, b: 3, c: 'new'},
      {a: 2, b: 1, c: 'z'},
    ]);
    expect(rowsInDB(db)).toEqual(compoundRows);
  });
});

describe('getRow', () => {
  test.each(['edit', 'remove'] as const)(
    'revalidates a non-primary unique key after %s and finds its new owner',
    change => {
      const original = {id: 'a', a: 1, b: 'old'};
      const edited = {...original, b: 'new'};
      const db = newDB(
        [original],
        'CREATE TABLE foo (id TEXT PRIMARY KEY, a, b TEXT UNIQUE)',
      );
      const source = newSource(db, true);
      apply(source, [
        change === 'edit'
          ? makeSourceChangeEdit(edited, original)
          : makeSourceChangeRemove(original),
      ]);

      expect(source.getRow({b: 'old'})).toBeUndefined();
      expect(source.getRow({b: 'new'})).toEqual(
        change === 'edit' ? edited : undefined,
      );
      expect(source.getRow({a: 1, b: 'old'})).toBeUndefined();
      expect(source.getRow({a: 1})).toEqual(
        change === 'edit' ? edited : undefined,
      );

      const replacement = {id: 'b', a: 2, b: 'old'};
      apply(source, [makeSourceChangeAdd(replacement)]);
      expect(source.getRow({b: 'old'})).toEqual(replacement);
      expect(rowsInDB(db)).toEqual([original]);
    },
  );

  test('reflects a row the batch edited, removed, or added', () => {
    const db = newDB(initial);
    const source = newSource(db, true);
    apply(source, [
      makeSourceChangeEdit({id: 'a', a: 11, b: 'one'}, initial[0]),
      makeSourceChangeRemove(initial[1]),
      makeSourceChangeAdd({id: 'd', a: 4, b: 'four'}),
    ]);

    expect(source.getRow({id: 'a'})).toEqual({id: 'a', a: 11, b: 'one'});
    expect(source.getRow({id: 'b'})).toBeUndefined();
    expect(source.getRow({id: 'c'})).toEqual(initial[2]);
    expect(source.getRow({id: 'd'})).toEqual({id: 'd', a: 4, b: 'four'});
  });

  test('finds a batch-added row by a non-primary unique key', () => {
    const db = newDB(initial);
    const source = newSource(db, true);
    apply(source, [makeSourceChangeAdd({id: 'd', a: 4, b: 'four'})]);
    expect(source.getRow({a: 4})).toEqual({id: 'd', a: 4, b: 'four'});
  });
});

describe('setDB', () => {
  test('moving to a snapshot that already has the changes drops the batch', () => {
    const db = newDB(initial);
    const source = newSource(db, true);
    apply(source, [makeSourceChangeAdd({id: 'd', a: 4, b: 'four'})]);
    expect(fetchAll(source).map(r => r.id)).toEqual(['a', 'b', 'c', 'd']);

    // The replicator's own commit is what makes the change durable; the source
    // leapfrogs onto a snapshot that already contains it.
    const next = newDB([...initial, {id: 'd', a: 4, b: 'four'}]);
    expect(source.pendingBytes).toBeGreaterThan(0);
    source.setDB(next);
    expect(source.pendingBytes).toBe(0);

    // Exactly one 'd' -- the batch is gone rather than double-counted.
    expect(fetchAll(source).map(r => r.id)).toEqual(['a', 'b', 'c', 'd']);
  });
});
