/* eslint-disable @typescript-eslint/naming-convention */
import {expect, test} from 'vitest';
import type {Ordering} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {makeComparator, type Node} from './data.ts';
import {SortToRootOrder} from './sort-to-root-order.ts';
import {MemoryStorage} from './memory-storage.ts';
import type {SourceSchema} from './schema.ts';
import type {Change} from './change.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Output,
} from './operator.ts';
import type {Stream} from './stream.ts';

// Mock input that provides rows in a specific order
class MockOrderedInput implements Input {
  readonly #rows: Row[];
  readonly #schema: SourceSchema;
  #output: Output = throwOutput;

  constructor(rows: Row[], currentOrdering: Ordering) {
    this.#rows = rows;
    this.#schema = {
      tableName: 'test',
      columns: {
        id: {type: 'number'},
        name: {type: 'string'},
        value: {type: 'number'},
      },
      primaryKey: ['id'],
      relationships: {},
      isHidden: false,
      system: 'client',
      compareRows: makeComparator(currentOrdering),
      sort: currentOrdering,
    };
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  *fetch(req: FetchRequest): Stream<Node> {
    let startIdx = 0;
    let endIdx = this.#rows.length;
    let step = 1;

    if (req.start) {
      // Find the start position
      const idx = this.#rows.findIndex(
        r => this.#schema.compareRows(r, req.start!.row) === 0,
      );
      if (idx !== -1) {
        startIdx = req.start.basis === 'after' ? idx + 1 : idx;
      }
    }

    if (req.reverse) {
      // Reverse iteration
      startIdx = req.start ? startIdx : this.#rows.length - 1;
      endIdx = -1;
      step = -1;
    }

    for (let i = startIdx; i !== endIdx; i += step) {
      yield {
        row: this.#rows[i],
        relationships: {},
      };
    }
  }

  *cleanup(_req: FetchRequest): Stream<Node> {
    // Return empty for cleanup
  }

  destroy(): void {
    // No-op
  }

  simulatePush(change: Change): void {
    this.#output.push(change);
  }
}

test('sorts rows to target order', () => {
  const storage = new MemoryStorage();

  // Input rows sorted by name
  const rows: Row[] = [
    {id: 1, name: 'Alice', value: 100},
    {id: 3, name: 'Bob', value: 300},
    {id: 2, name: 'Charlie', value: 200},
  ];

  const input = new MockOrderedInput(rows, [['name', 'asc']]);

  // We want to sort by id ascending
  const sorter = new SortToRootOrder({
    input,
    storage,
    targetSort: [['id', 'asc']],
  });

  const result = [...sorter.fetch({})];

  expect(result.length).toBe(3);
  expect(result[0].row).toEqual({id: 1, name: 'Alice', value: 100});
  expect(result[1].row).toEqual({id: 2, name: 'Charlie', value: 200});
  expect(result[2].row).toEqual({id: 3, name: 'Bob', value: 300});
});

test('sorts with composite ordering', () => {
  const storage = new MemoryStorage();

  // Input rows in random order
  const rows: Row[] = [
    {id: 2, name: 'Bob', value: 100},
    {id: 1, name: 'Alice', value: 200},
    {id: 3, name: 'Bob', value: 50},
    {id: 4, name: 'Alice', value: 100},
  ];

  const input = new MockOrderedInput(rows, [['id', 'asc']]);

  // Sort by name asc, then value desc
  const sorter = new SortToRootOrder({
    input,
    storage,
    targetSort: [
      ['name', 'asc'],
      ['value', 'desc'],
    ],
  });

  const result = [...sorter.fetch({})];

  expect(result.length).toBe(4);
  expect(result[0].row).toEqual({id: 1, name: 'Alice', value: 200});
  expect(result[1].row).toEqual({id: 4, name: 'Alice', value: 100});
  expect(result[2].row).toEqual({id: 2, name: 'Bob', value: 100});
  expect(result[3].row).toEqual({id: 3, name: 'Bob', value: 50});
});

test('handles descending sort', () => {
  const storage = new MemoryStorage();

  const rows: Row[] = [
    {id: 1, name: 'Alice', value: 100},
    {id: 2, name: 'Bob', value: 200},
    {id: 3, name: 'Charlie', value: 300},
  ];

  const input = new MockOrderedInput(rows, [['id', 'asc']]);

  // Sort by value descending
  const sorter = new SortToRootOrder({
    input,
    storage,
    targetSort: [['value', 'desc']],
  });

  const result = [...sorter.fetch({})];

  expect(result.length).toBe(3);
  expect(result[0].row).toEqual({id: 3, name: 'Charlie', value: 300});
  expect(result[1].row).toEqual({id: 2, name: 'Bob', value: 200});
  expect(result[2].row).toEqual({id: 1, name: 'Alice', value: 100});
});

test('handles fetch with start position', () => {
  const storage = new MemoryStorage();

  const rows: Row[] = [
    {id: 3, name: 'Charlie', value: 300},
    {id: 1, name: 'Alice', value: 100},
    {id: 2, name: 'Bob', value: 200},
  ];

  const input = new MockOrderedInput(rows, [['name', 'asc']]);

  const sorter = new SortToRootOrder({
    input,
    storage,
    targetSort: [['id', 'asc']],
  });

  // Fetch starting at id=2
  const result = [
    ...sorter.fetch({
      start: {
        row: {id: 2, name: 'Bob', value: 200},
        basis: 'at',
      },
    }),
  ];

  expect(result.length).toBe(2);
  expect(result[0].row).toEqual({id: 2, name: 'Bob', value: 200});
  expect(result[1].row).toEqual({id: 3, name: 'Charlie', value: 300});

  // Fetch starting after id=2
  const resultAfter = [
    ...sorter.fetch({
      start: {
        row: {id: 2, name: 'Bob', value: 200},
        basis: 'after',
      },
    }),
  ];

  expect(resultAfter.length).toBe(1);
  expect(resultAfter[0].row).toEqual({id: 3, name: 'Charlie', value: 300});
});

test('handles fetch with reverse', () => {
  const storage = new MemoryStorage();

  const rows: Row[] = [
    {id: 3, name: 'Charlie', value: 300},
    {id: 1, name: 'Alice', value: 100},
    {id: 2, name: 'Bob', value: 200},
  ];

  const input = new MockOrderedInput(rows, [['name', 'asc']]);

  const sorter = new SortToRootOrder({
    input,
    storage,
    targetSort: [['id', 'asc']],
  });

  // Fetch in reverse order
  const result = [...sorter.fetch({reverse: true})];

  expect(result.length).toBe(3);
  expect(result[0].row).toEqual({id: 3, name: 'Charlie', value: 300});
  expect(result[1].row).toEqual({id: 2, name: 'Bob', value: 200});
  expect(result[2].row).toEqual({id: 1, name: 'Alice', value: 100});
});

test('handles fetch with start and reverse', () => {
  const storage = new MemoryStorage();

  const rows: Row[] = [
    {id: 1, name: 'Alice', value: 100},
    {id: 2, name: 'Bob', value: 200},
    {id: 3, name: 'Charlie', value: 300},
    {id: 4, name: 'David', value: 400},
  ];

  const input = new MockOrderedInput(rows, [['id', 'asc']]);

  const sorter = new SortToRootOrder({
    input,
    storage,
    targetSort: [['id', 'asc']],
  });

  // Fetch in reverse starting at id=3
  const result = [
    ...sorter.fetch({
      start: {
        row: {id: 3, name: 'Charlie', value: 300},
        basis: 'at',
      },
      reverse: true,
    }),
  ];

  expect(result.length).toBe(3);
  expect(result[0].row).toEqual({id: 3, name: 'Charlie', value: 300});
  expect(result[1].row).toEqual({id: 2, name: 'Bob', value: 200});
  expect(result[2].row).toEqual({id: 1, name: 'Alice', value: 100});
});

test('push passes through unsorted', () => {
  const storage = new MemoryStorage();

  const input = new MockOrderedInput([], [['id', 'asc']]);

  const sorter = new SortToRootOrder({
    input,
    storage,
    targetSort: [['name', 'asc']],
  });

  const pushedChanges: Change[] = [];
  sorter.setOutput({
    push(change) {
      pushedChanges.push(change);
    },
  });

  // Simulate various push changes
  const addChange: Change = {
    type: 'add',
    node: {
      row: {id: 1, name: 'Alice', value: 100},
      relationships: {},
    },
  };

  input.simulatePush(addChange);
  expect(pushedChanges.length).toBe(1);
  expect(pushedChanges[0]).toBe(addChange);

  const removeChange: Change = {
    type: 'remove',
    node: {
      row: {id: 2, name: 'Bob', value: 200},
      relationships: {},
    },
  };

  input.simulatePush(removeChange);
  expect(pushedChanges.length).toBe(2);
  expect(pushedChanges[1]).toBe(removeChange);

  const editChange: Change = {
    type: 'edit',
    oldNode: {
      row: {id: 1, name: 'Alice', value: 100},
      relationships: {},
    },
    node: {
      row: {id: 1, name: 'Alice Updated', value: 150},
      relationships: {},
    },
  };

  input.simulatePush(editChange);
  expect(pushedChanges.length).toBe(3);
  expect(pushedChanges[2]).toBe(editChange);
});

test('cleanup sorts results', () => {
  const storage = new MemoryStorage();

  const rows: Row[] = [
    {id: 3, name: 'Charlie', value: 300},
    {id: 1, name: 'Alice', value: 100},
    {id: 2, name: 'Bob', value: 200},
  ];

  // Create a custom input that returns rows in cleanup
  class MockCleanupInput extends MockOrderedInput {
    *cleanup(_req: FetchRequest): Stream<Node> {
      for (const row of this.rows) {
        yield {row, relationships: {}};
      }
    }

    get rows() {
      return rows;
    }
  }

  const input = new MockCleanupInput(rows, [['name', 'asc']]);

  const sorter = new SortToRootOrder({
    input,
    storage,
    targetSort: [['id', 'asc']],
  });

  const result = [...sorter.cleanup({})];

  expect(result.length).toBe(3);
  expect(result[0].row).toEqual({id: 1, name: 'Alice', value: 100});
  expect(result[1].row).toEqual({id: 2, name: 'Bob', value: 200});
  expect(result[2].row).toEqual({id: 3, name: 'Charlie', value: 300});
});

test('handles empty input', () => {
  const storage = new MemoryStorage();

  const input = new MockOrderedInput([], [['id', 'asc']]);

  const sorter = new SortToRootOrder({
    input,
    storage,
    targetSort: [['name', 'asc']],
  });

  const result = [...sorter.fetch({})];
  expect(result.length).toBe(0);
});

test('handles single row', () => {
  const storage = new MemoryStorage();

  const rows: Row[] = [{id: 1, name: 'Alice', value: 100}];

  const input = new MockOrderedInput(rows, [['id', 'asc']]);

  const sorter = new SortToRootOrder({
    input,
    storage,
    targetSort: [['name', 'asc']],
  });

  const result = [...sorter.fetch({})];

  expect(result.length).toBe(1);
  expect(result[0].row).toEqual({id: 1, name: 'Alice', value: 100});
});

test('getSchema returns schema with updated sort', () => {
  const storage = new MemoryStorage();

  const input = new MockOrderedInput([], [['id', 'asc']]);

  const targetSort: Ordering = [
    ['name', 'asc'],
    ['value', 'desc'],
  ];

  const sorter = new SortToRootOrder({
    input,
    storage,
    targetSort,
  });

  const schema = sorter.getSchema();

  expect(schema.sort).toEqual(targetSort);
  // Check that compareRows uses the new sort
  const row1 = {id: 1, name: 'Alice', value: 100};
  const row2 = {id: 2, name: 'Alice', value: 200};
  const row3 = {id: 3, name: 'Bob', value: 100};

  expect(schema.compareRows(row1, row2)).toBeGreaterThan(0); // Same name, but value 100 > 200 in desc order
  expect(schema.compareRows(row1, row3)).toBeLessThan(0); // Alice < Bob
});
