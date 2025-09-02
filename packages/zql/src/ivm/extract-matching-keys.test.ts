/* eslint-disable @typescript-eslint/naming-convention */
/* eslint-disable @typescript-eslint/no-explicit-any */
import {expect, test} from 'vitest';
import type {Ordering} from '../../../zero-protocol/src/ast.ts';
import {makeComparator, type Node} from './data.ts';
import {ExtractMatchingKeys} from './extract-matching-keys.ts';
import type {SourceSchema} from './schema.ts';
import type {Change} from './change.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Output,
} from './operator.ts';
import type {Stream} from './stream.ts';

// Helper to create a test schema
function createTestSchema(
  tableName: string,
  primaryKey: string[],
): SourceSchema {
  const ordering: Ordering = primaryKey.map(k => [k, 'asc']);
  return {
    tableName,
    columns: {
      id: {type: 'string'},
      name: {type: 'string'},
      value: {type: 'number'},
      foo_id: {type: 'string'},
      bar_id: {type: 'string'},
      baz_id: {type: 'string'},
    },
    primaryKey: primaryKey as [string, ...string[]],
    relationships: {},
    isHidden: false,
    system: 'client',
    compareRows: makeComparator(ordering),
    sort: ordering,
  };
}

// Mock input that provides a nested tree structure
class MockNestedInput implements Input {
  readonly #nodes: Node[];
  readonly #schema: SourceSchema;
  #output: Output = throwOutput;

  constructor(nodes: Node[], schema: SourceSchema) {
    this.#nodes = nodes;
    this.#schema = schema;
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  *fetch(_req: FetchRequest): Stream<Node> {
    for (const node of this.#nodes) {
      yield node;
    }
  }

  *cleanup(_req: FetchRequest): Stream<Node> {
    // Empty for tests
  }

  destroy(): void {
    // No-op
  }

  simulatePush(change: Change): void {
    this.#output.push(change);
  }
}

test('extracts target table from nested structure', () => {
  const bazSchema = createTestSchema('baz', ['baz_id']);
  const fooSchema = createTestSchema('foo', ['foo_id']);

  // Create nested structure: baz -> bar -> foo
  const nestedNodes: Node[] = [
    {
      row: {baz_id: '1', name: 'Baz1'},
      relationships: {
        bar: () =>
          (function* () {
            yield {
              row: {bar_id: '10', name: 'Bar10'},
              relationships: {
                foo: () =>
                  (function* () {
                    yield {
                      row: {foo_id: '100', name: 'Foo100', value: 1},
                      relationships: {},
                    };
                    yield {
                      row: {foo_id: '101', name: 'Foo101', value: 2},
                      relationships: {},
                    };
                  })(),
              },
            };
          })(),
      },
    },
    {
      row: {baz_id: '2', name: 'Baz2'},
      relationships: {
        bar: () =>
          (function* () {
            yield {
              row: {bar_id: '20', name: 'Bar20'},
              relationships: {
                foo: () =>
                  (function* () {
                    yield {
                      row: {foo_id: '200', name: 'Foo200', value: 3},
                      relationships: {},
                    };
                  })(),
              },
            };
          })(),
      },
    },
  ];

  const input = new MockNestedInput(nestedNodes, bazSchema);
  const extractor = new ExtractMatchingKeys({
    input,
    targetTable: 'foo',
    targetPath: ['bar', 'foo'],
    targetSchema: fooSchema,
  });

  const result = [...extractor.fetch({})];

  expect(result.length).toBe(3);
  expect(result[0].row).toEqual({foo_id: '100', name: 'Foo100', value: 1});
  expect(result[1].row).toEqual({foo_id: '101', name: 'Foo101', value: 2});
  expect(result[2].row).toEqual({foo_id: '200', name: 'Foo200', value: 3});

  // All should have empty relationships
  result.forEach(node => {
    expect(Object.keys(node.relationships)).toHaveLength(0);
  });
});

test('deduplicates rows with same primary key', () => {
  const bazSchema = createTestSchema('baz', ['baz_id']);
  const fooSchema = createTestSchema('foo', ['foo_id']);

  // Create structure where same foo appears multiple times
  const nestedNodes: Node[] = [
    {
      row: {baz_id: '1', name: 'Baz1'},
      relationships: {
        bar: () =>
          (function* () {
            yield {
              row: {bar_id: '10', name: 'Bar10'},
              relationships: {
                foo: () =>
                  (function* () {
                    yield {
                      row: {foo_id: '100', name: 'Foo100', value: 1},
                      relationships: {},
                    };
                  })(),
              },
            };
            yield {
              row: {bar_id: '11', name: 'Bar11'},
              relationships: {
                foo: () =>
                  (function* () {
                    // Same foo_id as above
                    yield {
                      row: {foo_id: '100', name: 'Foo100', value: 1},
                      relationships: {},
                    };
                  })(),
              },
            };
          })(),
      },
    },
  ];

  const input = new MockNestedInput(nestedNodes, bazSchema);
  const extractor = new ExtractMatchingKeys({
    input,
    targetTable: 'foo',
    targetPath: ['bar', 'foo'],
    targetSchema: fooSchema,
  });

  const result = [...extractor.fetch({})];

  // Should only get one foo_id: '100' despite it appearing twice
  expect(result.length).toBe(1);
  expect(result[0].row).toEqual({foo_id: '100', name: 'Foo100', value: 1});
});

test('handles empty relationships', () => {
  const bazSchema = createTestSchema('baz', ['baz_id']);
  const fooSchema = createTestSchema('foo', ['foo_id']);

  // Create structure where bar relationship exists but is empty
  const nestedNodes: Node[] = [
    {
      row: {baz_id: '1', name: 'Baz1'},
      relationships: {
        bar: () =>
          (function* () {
            // Empty - no bars
          })(),
      },
    },
  ];

  const input = new MockNestedInput(nestedNodes, bazSchema);
  const extractor = new ExtractMatchingKeys({
    input,
    targetTable: 'foo',
    targetPath: ['bar', 'foo'],
    targetSchema: fooSchema,
  });

  const result = [...extractor.fetch({})];

  // Should get no results since no path to foo exists
  expect(result.length).toBe(0);
});

test('handles missing relationships', () => {
  const bazSchema = createTestSchema('baz', ['baz_id']);
  const fooSchema = createTestSchema('foo', ['foo_id']);

  // Create structure where bar relationship doesn't exist
  const nestedNodes: Node[] = [
    {
      row: {baz_id: '1', name: 'Baz1'},
      relationships: {
        // No 'bar' relationship at all
      },
    },
  ];

  const input = new MockNestedInput(nestedNodes, bazSchema);
  const extractor = new ExtractMatchingKeys({
    input,
    targetTable: 'foo',
    targetPath: ['bar', 'foo'],
    targetSchema: fooSchema,
  });

  const result = [...extractor.fetch({})];

  // Should get no results since path doesn't exist
  expect(result.length).toBe(0);
});

test('push add and remove changes', () => {
  const bazSchema = createTestSchema('baz', ['baz_id']);
  const fooSchema = createTestSchema('foo', ['foo_id']);

  const input = new MockNestedInput([], bazSchema);
  const extractor = new ExtractMatchingKeys({
    input,
    targetTable: 'foo',
    targetPath: ['bar', 'foo'],
    targetSchema: fooSchema,
  });

  const pushedChanges: Change[] = [];
  extractor.setOutput({
    push(change) {
      pushedChanges.push(change);
    },
  });

  // Simulate adding a nested structure
  const addChange: Change = {
    type: 'add',
    node: {
      row: {baz_id: '1', name: 'Baz1'},
      relationships: {
        bar: () =>
          (function* () {
            yield {
              row: {bar_id: '10', name: 'Bar10'},
              relationships: {
                foo: () =>
                  (function* () {
                    yield {
                      row: {foo_id: '100', name: 'Foo100', value: 1},
                      relationships: {},
                    };
                  })(),
              },
            };
          })(),
      },
    },
  };

  input.simulatePush(addChange);

  expect(pushedChanges.length).toBe(1);
  expect(pushedChanges[0].type).toBe('add');
  expect((pushedChanges[0] as any).node.row).toEqual({
    foo_id: '100',
    name: 'Foo100',
    value: 1,
  });

  // Test remove
  pushedChanges.length = 0;
  const removeChange: Change = {
    type: 'remove',
    node: addChange.node,
  };

  input.simulatePush(removeChange);

  expect(pushedChanges.length).toBe(1);
  expect(pushedChanges[0].type).toBe('remove');
  expect((pushedChanges[0] as any).node.row).toEqual({
    foo_id: '100',
    name: 'Foo100',
    value: 1,
  });
});

test('push edit changes', () => {
  const bazSchema = createTestSchema('baz', ['baz_id']);
  const fooSchema = createTestSchema('foo', ['foo_id']);

  const input = new MockNestedInput([], bazSchema);
  const extractor = new ExtractMatchingKeys({
    input,
    targetTable: 'foo',
    targetPath: ['bar', 'foo'],
    targetSchema: fooSchema,
  });

  const pushedChanges: Change[] = [];
  extractor.setOutput({
    push(change) {
      pushedChanges.push(change);
    },
  });

  // Simulate an edit that changes foo data
  const editChange: Change = {
    type: 'edit',
    oldNode: {
      row: {baz_id: '1', name: 'Baz1'},
      relationships: {
        bar: () =>
          (function* () {
            yield {
              row: {bar_id: '10', name: 'Bar10'},
              relationships: {
                foo: () =>
                  (function* () {
                    yield {
                      row: {foo_id: '100', name: 'OldFoo', value: 1},
                      relationships: {},
                    };
                  })(),
              },
            };
          })(),
      },
    },
    node: {
      row: {baz_id: '1', name: 'Baz1'},
      relationships: {
        bar: () =>
          (function* () {
            yield {
              row: {bar_id: '10', name: 'Bar10'},
              relationships: {
                foo: () =>
                  (function* () {
                    yield {
                      row: {foo_id: '100', name: 'NewFoo', value: 2},
                      relationships: {},
                    };
                  })(),
              },
            };
          })(),
      },
    },
  };

  input.simulatePush(editChange);

  expect(pushedChanges.length).toBe(1);
  expect(pushedChanges[0].type).toBe('edit');
  const edit = pushedChanges[0] as any;
  expect(edit.oldNode.row).toEqual({
    foo_id: '100',
    name: 'OldFoo',
    value: 1,
  });
  expect(edit.node.row).toEqual({
    foo_id: '100',
    name: 'NewFoo',
    value: 2,
  });
});

test('handles single-level extraction', () => {
  const barSchema = createTestSchema('bar', ['bar_id']);
  const fooSchema = createTestSchema('foo', ['foo_id']);

  // Create structure: bar -> foo (single level)
  const nestedNodes: Node[] = [
    {
      row: {bar_id: '10', name: 'Bar10'},
      relationships: {
        foo: () =>
          (function* () {
            yield {
              row: {foo_id: '100', name: 'Foo100', value: 1},
              relationships: {},
            };
            yield {
              row: {foo_id: '101', name: 'Foo101', value: 2},
              relationships: {},
            };
          })(),
      },
    },
  ];

  const input = new MockNestedInput(nestedNodes, barSchema);
  const extractor = new ExtractMatchingKeys({
    input,
    targetTable: 'foo',
    targetPath: ['foo'], // Single level path
    targetSchema: fooSchema,
  });

  const result = [...extractor.fetch({})];

  expect(result.length).toBe(2);
  expect(result[0].row).toEqual({foo_id: '100', name: 'Foo100', value: 1});
  expect(result[1].row).toEqual({foo_id: '101', name: 'Foo101', value: 2});
});

test('handles zero-level extraction (target is at root)', () => {
  const fooSchema = createTestSchema('foo', ['foo_id']);

  // Target table is already at root
  const nodes: Node[] = [
    {
      row: {foo_id: '100', name: 'Foo100', value: 1},
      relationships: {
        bar: () =>
          (function* () {
            yield {
              row: {bar_id: '10', name: 'Bar10'},
              relationships: {},
            };
          })(),
      },
    },
    {
      row: {foo_id: '101', name: 'Foo101', value: 2},
      relationships: {},
    },
  ];

  const input = new MockNestedInput(nodes, fooSchema);
  const extractor = new ExtractMatchingKeys({
    input,
    targetTable: 'foo',
    targetPath: [], // Empty path - target is at root
    targetSchema: fooSchema,
  });

  const result = [...extractor.fetch({})];

  expect(result.length).toBe(2);
  expect(result[0].row).toEqual({foo_id: '100', name: 'Foo100', value: 1});
  expect(result[1].row).toEqual({foo_id: '101', name: 'Foo101', value: 2});
});
