import {expect, test} from 'vitest';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {Ordering} from '../../../zero-protocol/src/ast.ts';
import {makeComparator, type Node} from './data.ts';
import {Distinct} from './distinct.ts';
import {MemorySource} from './memory-source.ts';
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

function createTestNode(row: Row): Node {
  return {
    row,
    relationships: {},
  };
}

// Mock input that allows duplicate rows for testing
class MockInput implements Input {
  readonly #rows: Row[];
  readonly #schema: SourceSchema;
  // @ts-expect-error - output is set but not used in this mock
  #output: Output = throwOutput;

  constructor(
    rows: Row[],
    ordering: Ordering,
    primaryKey: PrimaryKey = ['id'],
  ) {
    this.#rows = rows;
    this.#schema = {
      tableName: 'test',
      columns: {
        id: {type: 'string'},
        name: {type: 'string'},
        value: {type: 'number'},
        type: {type: 'string'},
      },
      primaryKey,
      relationships: {},
      isHidden: false,
      system: 'client',
      compareRows: makeComparator(ordering),
      sort: ordering,
    };
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  *fetch(_req: FetchRequest): Stream<Node> {
    for (const row of this.#rows) {
      yield createTestNode(row);
    }
  }

  *cleanup(_req: FetchRequest): Stream<Node> {
    // Return empty for cleanup
  }

  destroy(): void {
    // No-op
  }
}

test('distinct filters duplicate rows on fetch', () => {
  const storage = new MemoryStorage();

  const rows: Row[] = [
    {id: '1', name: 'Alice', value: 10},
    {id: '1', name: 'Alice', value: 10}, // duplicate
    {id: '2', name: 'Bob', value: 20},
    {id: '1', name: 'Alice', value: 10}, // duplicate
    {id: '3', name: 'Charlie', value: 30},
  ];

  const input = new MockInput(rows, [['id', 'asc']]);
  const distinct = new Distinct(input, storage, ['id'] as PrimaryKey);

  const result = [...distinct.fetch({})];

  expect(result.length).toBe(3);
  expect(result[0].row).toEqual({id: '1', name: 'Alice', value: 10});
  expect(result[1].row).toEqual({id: '2', name: 'Bob', value: 20});
  expect(result[2].row).toEqual({id: '3', name: 'Charlie', value: 30});
});

test('distinct handles add changes', () => {
  const storage = new MemoryStorage();

  const source = new MemorySource(
    'test',
    {
      id: {type: 'string'},
      name: {type: 'string'},
      value: {type: 'number'},
    },
    ['id'],
  );

  const connected = source.connect([['id', 'asc']]);
  const distinct = new Distinct(connected, storage, ['id'] as PrimaryKey);

  const changes: Array<{type: string; row?: Row}> = [];
  distinct.setOutput({
    push(change) {
      if (change.type === 'add' || change.type === 'remove') {
        changes.push({type: change.type, row: change.node.row});
      }
    },
  });

  // Initial fetch to initialize state
  [...distinct.fetch({})];

  // Add a new row
  distinct.push({
    type: 'add',
    node: createTestNode({id: '1', name: 'Alice', value: 10}),
  });

  expect(changes.length).toBe(1);
  expect(changes[0]).toEqual({
    type: 'add',
    row: {id: '1', name: 'Alice', value: 10},
  });

  // Try to add duplicate - should be ignored
  changes.length = 0;
  distinct.push({
    type: 'add',
    node: createTestNode({id: '1', name: 'Alice', value: 10}),
  });

  expect(changes.length).toBe(0);

  // Add a different row
  distinct.push({
    type: 'add',
    node: createTestNode({id: '2', name: 'Bob', value: 20}),
  });

  expect(changes.length).toBe(1);
  expect(changes[0]).toEqual({
    type: 'add',
    row: {id: '2', name: 'Bob', value: 20},
  });
});

test('distinct handles remove changes', () => {
  const storage = new MemoryStorage();

  const source = new MemorySource(
    'test',
    {
      id: {type: 'string'},
      name: {type: 'string'},
      value: {type: 'number'},
    },
    ['id'],
  );

  const rows: Row[] = [
    {id: '1', name: 'Alice', value: 10},
    {id: '2', name: 'Bob', value: 20},
  ];

  for (const row of rows) {
    source.push({
      type: 'add',
      row,
    });
  }

  const connected = source.connect([['id', 'asc']]);
  const distinct = new Distinct(connected, storage, ['id'] as PrimaryKey);

  const changes: Array<{type: string; row?: Row}> = [];
  distinct.setOutput({
    push(change) {
      if (change.type === 'add' || change.type === 'remove') {
        changes.push({type: change.type, row: change.node.row});
      }
    },
  });

  // Initial fetch to populate state
  [...distinct.fetch({})];

  // Remove existing row
  distinct.push({
    type: 'remove',
    node: createTestNode({id: '1', name: 'Alice', value: 10}),
  });

  expect(changes.length).toBe(1);
  expect(changes[0]).toEqual({
    type: 'remove',
    row: {id: '1', name: 'Alice', value: 10},
  });

  // Try to remove non-existent row - should be ignored
  changes.length = 0;
  distinct.push({
    type: 'remove',
    node: createTestNode({id: '3', name: 'Charlie', value: 30}),
  });

  expect(changes.length).toBe(0);
});

test('distinct handles edit changes', () => {
  const storage = new MemoryStorage();

  const source = new MemorySource(
    'test',
    {
      id: {type: 'string'},
      name: {type: 'string'},
      value: {type: 'number'},
    },
    ['id'],
  );

  const rows: Row[] = [{id: '1', name: 'Alice', value: 10}];

  for (const row of rows) {
    source.push({
      type: 'add',
      row,
    });
  }

  const connected = source.connect([['id', 'asc']]);
  const distinct = new Distinct(connected, storage, ['id'] as PrimaryKey);

  const changes: Array<Change> = [];
  distinct.setOutput({
    push(change) {
      changes.push(change);
    },
  });

  // Initial fetch to populate state
  [...distinct.fetch({})];

  // Edit without changing key
  distinct.push({
    type: 'edit',
    node: createTestNode({id: '1', name: 'Alice Updated', value: 15}),
    oldNode: createTestNode({id: '1', name: 'Alice', value: 10}),
  });

  expect(changes.length).toBe(1);
  expect(changes[0].type).toBe('edit');
  expect(changes[0].node.row).toEqual({
    id: '1',
    name: 'Alice Updated',
    value: 15,
  });

  // Edit that changes key - should be converted to remove + add
  changes.length = 0;
  distinct.push({
    type: 'edit',
    node: createTestNode({id: '2', name: 'Bob', value: 20}),
    oldNode: createTestNode({id: '1', name: 'Alice Updated', value: 15}),
  });

  expect(changes.length).toBe(2);
  expect(changes[0].type).toBe('remove');
  expect(changes[0].node.row).toEqual({
    id: '1',
    name: 'Alice Updated',
    value: 15,
  });
  expect(changes[1].type).toBe('add');
  expect(changes[1].node.row).toEqual({id: '2', name: 'Bob', value: 20});
});

test('distinct with composite keys', () => {
  const storage = new MemoryStorage();

  const rows: Row[] = [
    {id: '1', type: 'A', name: 'Alice'},
    {id: '1', type: 'B', name: 'Alice B'},
    {id: '1', type: 'A', name: 'Alice Duplicate'}, // duplicate on (id, type)
    {id: '2', type: 'A', name: 'Bob'},
  ];

  const input = new MockInput(
    rows,
    [
      ['id', 'asc'],
      ['type', 'asc'],
    ],
    ['id', 'type'],
  );
  const distinct = new Distinct(input, storage, ['id', 'type'] as PrimaryKey);

  const result = [...distinct.fetch({})];

  expect(result.length).toBe(3);
  expect(result[0].row).toEqual({id: '1', type: 'A', name: 'Alice'});
  expect(result[1].row).toEqual({id: '1', type: 'B', name: 'Alice B'});
  expect(result[2].row).toEqual({id: '2', type: 'A', name: 'Bob'});
});

test('distinct with nested flat join format', () => {
  const storage = new MemoryStorage();
  
  // Rows with flat join data nested under aliases
  const rows: Row[] = [
    {
      id: '1', 
      name: 'Alice', 
      value: 10,
      existsJoin1: {id: 'j1', data: 'joined1'},
    },
    {
      id: '1',  // Same root id
      name: 'Alice', 
      value: 10,
      existsJoin1: {id: 'j2', data: 'joined2'}, // Different join data
    },
    {
      id: '2', 
      name: 'Bob', 
      value: 20,
      existsJoin1: {id: 'j3', data: 'joined3'},
    },
    {
      id: '1',  // Another duplicate root
      name: 'Alice', 
      value: 10,
      existsJoin1: {id: 'j4', data: 'joined4'},
      existsJoin2: {id: 'j5', data: 'joined5'}, // Multiple joins
    },
  ];
  
  const input = new MockInput(rows, [['id', 'asc']]);
  const distinct = new Distinct(input, storage, ['id'] as PrimaryKey);
  
  const result = [...distinct.fetch({})];
  
  // Should only get 2 unique root rows (id=1 and id=2)
  expect(result.length).toBe(2);
  expect(result[0].row.id).toBe('1');
  expect(result[0].row.name).toBe('Alice');
  // Should preserve the first occurrence's join data
  expect((result[0].row as any).existsJoin1).toEqual({id: 'j1', data: 'joined1'});
  expect(result[1].row.id).toBe('2');
  expect(result[1].row.name).toBe('Bob');
});

test('cleanup removes state', () => {
  const storage = new MemoryStorage();

  const source = new MemorySource(
    'test',
    {
      id: {type: 'string'},
      name: {type: 'string'},
      value: {type: 'number'},
    },
    ['id'],
  );

  const rows: Row[] = [
    {id: '1', name: 'Alice', value: 10},
    {id: '2', name: 'Bob', value: 20},
  ];

  for (const row of rows) {
    source.push({
      type: 'add',
      row,
    });
  }

  const connected = source.connect([['id', 'asc']]);
  const distinct = new Distinct(connected, storage, ['id'] as PrimaryKey);

  // Initial fetch to populate state
  const fetchResult = [...distinct.fetch({})];
  expect(fetchResult.length).toBe(2);

  // Cleanup
  const cleanupResult = [...distinct.cleanup({})];
  expect(cleanupResult.length).toBe(2);

  // After cleanup, a new fetch should work normally
  const newFetchResult = [...distinct.fetch({})];
  expect(newFetchResult.length).toBe(2);
});
