import {expect, test} from 'vitest';
import {compareRowsTest} from './data.test.js';
import type {Ordering} from '../ast2/ast.js';
import {MemorySource, SourceChange} from './memory-source.js';
import {CaptureOutput} from './capture-output.js';
import type {Row, Node} from './data.js';

test('schema', () => {
  compareRowsTest((order: Ordering) => {
    const ms = new MemorySource(order);
    return ms.schema().compareRows;
  });
});

function asNodes(rows: Row[]): Node[] {
  return rows.map(row => ({
    row,
    relationships: new Map(),
  }));
}

test('simple-pull', () => {
  // works the same for hydrate and fetch.
  for (const m of ['hydrate', 'fetch'] as const) {
    const ms = new MemorySource([['a', 'asc']]);
    const out = new CaptureOutput();
    expect([...ms[m]({}, out)]).toEqual([]);

    ms.push({type: 'add', row: {a: 3}});
    expect([...ms[m]({}, out)]).toEqual(asNodes([{a: 3}]));

    ms.push({type: 'add', row: {a: 1}});
    ms.push({type: 'add', row: {a: 2}});
    expect([...ms[m]({}, out)]).toEqual(asNodes([{a: 1}, {a: 2}, {a: 3}]));

    ms.push({type: 'remove', row: {a: 1}});
    expect([...ms[m]({}, out)]).toEqual(asNodes([{a: 2}, {a: 3}]));

    ms.push({type: 'remove', row: {a: 2}});
    ms.push({type: 'remove', row: {a: 3}});
    expect([...ms[m]({}, out)]).toEqual([]);
  }
});

test('pull-with-constraint', () => {
  // works the same for hydrate and fetch.
  for (const m of ['hydrate', 'fetch'] as const) {
    const ms = new MemorySource([['a', 'asc']]);
    const out = new CaptureOutput();
    ms.addOutput(out);
    ms.push({type: 'add', row: {a: 3, b: true, c: 1}});
    ms.push({type: 'add', row: {a: 1, b: true, c: 2}});
    ms.push({type: 'add', row: {a: 2, b: false, c: null}});

    expect([...ms[m]({constraint: {key: 'b', value: true}}, out)]).toEqual(
      asNodes([
        {a: 1, b: true, c: 2},
        {a: 3, b: true, c: 1},
      ]),
    );

    expect([...ms[m]({constraint: {key: 'b', value: false}}, out)]).toEqual(
      asNodes([{a: 2, b: false, c: null}]),
    );

    expect([...ms[m]({constraint: {key: 'c', value: 1}}, out)]).toEqual(
      asNodes([{a: 3, b: true, c: 1}]),
    );

    expect([...ms[m]({constraint: {key: 'c', value: 0}}, out)]).toEqual(
      asNodes([]),
    );

    // Constraints are used to implement joins and so should use join
    // semantics for equality. null !== null.
    expect([...ms[m]({constraint: {key: 'c', value: null}}, out)]).toEqual(
      asNodes([]),
    );
    expect([...ms[m]({constraint: {key: 'c', value: undefined}}, out)]).toEqual(
      asNodes([]),
    );

    // Not really a feature, but because of loose typing of joins and how we
    // accept undefined we can't really tell when constraining on a field that
    // doesn't exist.
    expect([...ms[m]({constraint: {key: 'd', value: null}}, out)]).toEqual(
      asNodes([]),
    );
    expect([...ms[m]({constraint: {key: 'd', value: undefined}}, out)]).toEqual(
      asNodes([]),
    );
  }
});

test('fetch-start', () => {
  const ms = new MemorySource([['a', 'asc']]);
  const out = new CaptureOutput();
  ms.addOutput(out);

  expect([...ms.fetch({start: {row: {a: 2}, basis: 'before'}}, out)]).toEqual(
    asNodes([]),
  );
  expect([...ms.fetch({start: {row: {a: 2}, basis: 'at'}}, out)]).toEqual(
    asNodes([]),
  );
  expect([...ms.fetch({start: {row: {a: 2}, basis: 'after'}}, out)]).toEqual(
    asNodes([]),
  );

  ms.push({type: 'add', row: {a: 2}});
  ms.push({type: 'add', row: {a: 3}});
  expect([...ms.fetch({start: {row: {a: 2}, basis: 'before'}}, out)]).toEqual(
    asNodes([{a: 2}, {a: 3}]),
  );
  expect([...ms.fetch({start: {row: {a: 2}, basis: 'at'}}, out)]).toEqual(
    asNodes([{a: 2}, {a: 3}]),
  );
  expect([...ms.fetch({start: {row: {a: 2}, basis: 'after'}}, out)]).toEqual(
    asNodes([{a: 3}]),
  );

  expect([...ms.fetch({start: {row: {a: 3}, basis: 'before'}}, out)]).toEqual(
    asNodes([{a: 2}, {a: 3}]),
  );
  expect([...ms.fetch({start: {row: {a: 3}, basis: 'at'}}, out)]).toEqual(
    asNodes([{a: 3}]),
  );
  expect([...ms.fetch({start: {row: {a: 3}, basis: 'after'}}, out)]).toEqual(
    asNodes([]),
  );

  expect([...ms.fetch({start: {row: {a: 4}, basis: 'before'}}, out)]).toEqual(
    asNodes([{a: 3}]),
  );
  expect([...ms.fetch({start: {row: {a: 4}, basis: 'at'}}, out)]).toEqual(
    asNodes([]),
  );
  expect([...ms.fetch({start: {row: {a: 4}, basis: 'after'}}, out)]).toEqual(
    asNodes([]),
  );

  ms.push({type: 'remove', row: {a: 2}});
  ms.push({type: 'remove', row: {a: 3}});
  expect([...ms.fetch({start: {row: {a: 2}, basis: 'before'}}, out)]).toEqual(
    asNodes([]),
  );
  expect([...ms.fetch({start: {row: {a: 2}, basis: 'at'}}, out)]).toEqual(
    asNodes([]),
  );
  expect([...ms.fetch({start: {row: {a: 2}, basis: 'after'}}, out)]).toEqual(
    asNodes([]),
  );
});

function asChanges(sc: SourceChange[]) {
  return sc.map(c => ({
    type: c.type,
    node: {
      row: c.row,
      relationships: new Map(),
    },
  }));
}

test('push', () => {
  const ms = new MemorySource([['a', 'asc']]);
  const out = new CaptureOutput();
  ms.addOutput(out);

  expect(out.changes).toEqual([]);

  ms.push({type: 'add', row: {a: 2}});
  expect(out.changes).toEqual(asChanges([{type: 'add', row: {a: 2}}]));

  ms.push({type: 'add', row: {a: 1}});
  expect(out.changes).toEqual(
    asChanges([
      {type: 'add', row: {a: 2}},
      {type: 'add', row: {a: 1}},
    ]),
  );

  ms.push({type: 'remove', row: {a: 1}});
  ms.push({type: 'remove', row: {a: 2}});
  expect(out.changes).toEqual(
    asChanges([
      {type: 'add', row: {a: 2}},
      {type: 'add', row: {a: 1}},
      {type: 'remove', row: {a: 1}},
      {type: 'remove', row: {a: 2}},
    ]),
  );

  // Remove row that isn't there
  out.reset();
  expect(() => ms.push({type: 'remove', row: {a: 1}})).toThrow('Row not found');
  expect(out.changes).toEqual(asChanges([]));

  // Add row twice
  ms.push({type: 'add', row: {a: 1}});
  expect(out.changes).toEqual(asChanges([{type: 'add', row: {a: 1}}]));
  expect(() => ms.push({type: 'add', row: {a: 1}})).toThrow(
    'Row already exists',
  );
  expect(out.changes).toEqual(asChanges([{type: 'add', row: {a: 1}}]));
});
