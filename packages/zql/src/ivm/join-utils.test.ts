import {describe, expect, test} from 'vitest';
import {
  generateWithOverlayUnordered,
  generateWithOverlayNoYieldUnordered,
  rowEqualsForCompoundKey,
  isJoinMatch,
  buildJoinConstraint,
} from './join-utils.ts';
import type {Node} from './data.ts';
import type {SourceSchema} from './schema.ts';
import type {Change} from './change.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Stream} from './stream.ts';

function makeNode(row: Row): Node {
  return {row, relationships: {}};
}

function makeSchema(primaryKey: readonly [string, ...string[]]): SourceSchema {
  return {
    tableName: 'test',
    columns: {},
    primaryKey,
    relationships: {},
    isHidden: false,
    system: 'client',
    compareRows: () => 0,
  };
}

function collectNodes(stream: Stream<Node | 'yield'>): (Node | 'yield')[] {
  return [...stream];
}

function collectRows(
  stream: Stream<Node | 'yield'>,
): Record<string, unknown>[] {
  return [...stream].filter((n): n is Node => n !== 'yield').map(n => n.row);
}

describe('generateWithOverlayUnordered', () => {
  const schema = makeSchema(['id']);

  describe('remove', () => {
    test('yields overlay node first then all stream nodes', () => {
      const stream: Stream<Node | 'yield'> = [
        makeNode({id: 1}),
        makeNode({id: 2}),
      ];
      const overlay: Change = {type: 'remove', node: makeNode({id: 3})};

      const result = collectRows(
        generateWithOverlayUnordered(stream, overlay, schema),
      );
      expect(result).toEqual([{id: 3}, {id: 1}, {id: 2}]);
    });

    test('does not assert when overlay node is not in stream', () => {
      const stream: Stream<Node | 'yield'> = [];
      const overlay: Change = {type: 'remove', node: makeNode({id: 99})};

      const result = collectRows(
        generateWithOverlayUnordered(stream, overlay, schema),
      );
      expect(result).toEqual([{id: 99}]);
    });
  });

  describe('add', () => {
    test('suppresses matching node from stream', () => {
      const stream: Stream<Node | 'yield'> = [
        makeNode({id: 1}),
        makeNode({id: 2}),
        makeNode({id: 3}),
      ];
      const overlay: Change = {type: 'add', node: makeNode({id: 2})};

      const result = collectRows(
        generateWithOverlayUnordered(stream, overlay, schema),
      );
      expect(result).toEqual([{id: 1}, {id: 3}]);
    });

    test('asserts if no matching node found in stream', () => {
      const stream: Stream<Node | 'yield'> = [makeNode({id: 1})];
      const overlay: Change = {type: 'add', node: makeNode({id: 99})};

      expect(() =>
        collectNodes(generateWithOverlayUnordered(stream, overlay, schema)),
      ).toThrow(
        'overlayGenerator: overlay was never applied to any fetched node',
      );
    });
  });

  describe('edit', () => {
    test('yields old node first and suppresses matching node from stream', () => {
      const stream: Stream<Node | 'yield'> = [
        makeNode({id: 1}),
        makeNode({id: 2, val: 'new'}),
      ];
      const overlay: Change = {
        type: 'edit',
        node: makeNode({id: 2, val: 'new'}),
        oldNode: makeNode({id: 2, val: 'old'}),
      };

      const result = collectRows(
        generateWithOverlayUnordered(stream, overlay, schema),
      );
      expect(result).toEqual([{id: 2, val: 'old'}, {id: 1}]);
    });

    test('asserts if no matching node found in stream', () => {
      const stream: Stream<Node | 'yield'> = [makeNode({id: 1})];
      const overlay: Change = {
        type: 'edit',
        node: makeNode({id: 99}),
        oldNode: makeNode({id: 99}),
      };

      expect(() =>
        collectNodes(generateWithOverlayUnordered(stream, overlay, schema)),
      ).toThrow(
        'overlayGenerator: overlay was never applied to any fetched node',
      );
    });
  });

  describe('child', () => {
    test('overlays child relationship on matching node', () => {
      const childSchema = makeSchema(['cid']);
      const schemaWithRel: SourceSchema = {
        ...schema,
        relationships: {items: childSchema},
      };

      const stream: Stream<Node | 'yield'> = [
        makeNode({id: 1}),
        {
          row: {id: 2},
          relationships: {
            items: function* () {
              yield makeNode({cid: 'a'});
              yield makeNode({cid: 'b'});
            },
          },
        },
      ];

      const childChange: Change = {type: 'add', node: makeNode({cid: 'c'})};
      const overlay: Change = {
        type: 'child',
        node: makeNode({id: 2}),
        child: {
          relationshipName: 'items',
          change: childChange,
        },
      };

      const result = collectNodes(
        generateWithOverlayUnordered(stream, overlay, schemaWithRel),
      );
      expect(result).toHaveLength(2);
      // First node passes through unchanged
      expect((result[0] as Node).row).toEqual({id: 1});
      // Second node has overlaid relationship
      const overlaid = result[1] as Node;
      expect(overlaid.row).toEqual({id: 2});
      // The relationship should be a function (lazy stream)
      expect(typeof overlaid.relationships.items).toBe('function');
    });

    test('asserts if no matching node found in stream', () => {
      const schemaWithRel: SourceSchema = {
        ...schema,
        relationships: {items: makeSchema(['cid'])},
      };

      const stream: Stream<Node | 'yield'> = [makeNode({id: 1})];
      const overlay: Change = {
        type: 'child',
        node: makeNode({id: 99}),
        child: {
          relationshipName: 'items',
          change: {type: 'add', node: makeNode({cid: 'c'})},
        },
      };

      expect(() =>
        collectNodes(
          generateWithOverlayUnordered(stream, overlay, schemaWithRel),
        ),
      ).toThrow(
        'overlayGenerator: overlay was never applied to any fetched node',
      );
    });
  });

  describe('compound primary key', () => {
    const compoundSchema = makeSchema(['a', 'b']);

    test('matches on all PK columns', () => {
      const stream: Stream<Node | 'yield'> = [
        makeNode({a: 1, b: 1, val: 'x'}),
        makeNode({a: 1, b: 2, val: 'y'}),
        makeNode({a: 2, b: 1, val: 'z'}),
      ];
      const overlay: Change = {
        type: 'add',
        node: makeNode({a: 1, b: 2}),
      };

      const result = collectRows(
        generateWithOverlayUnordered(stream, overlay, compoundSchema),
      );
      expect(result).toEqual([
        {a: 1, b: 1, val: 'x'},
        {a: 2, b: 1, val: 'z'},
      ]);
    });

    test('does not match on partial PK', () => {
      const stream: Stream<Node | 'yield'> = [
        makeNode({a: 1, b: 1}),
        makeNode({a: 1, b: 2}),
      ];
      // Matches a=1 but b differs
      const overlay: Change = {
        type: 'add',
        node: makeNode({a: 1, b: 3}),
      };

      expect(() =>
        collectNodes(
          generateWithOverlayUnordered(stream, overlay, compoundSchema),
        ),
      ).toThrow(
        'overlayGenerator: overlay was never applied to any fetched node',
      );
    });
  });

  describe('yield markers', () => {
    test('passes yield markers through unchanged', () => {
      const stream: Stream<Node | 'yield'> = [
        makeNode({id: 1}),
        'yield' as const,
        makeNode({id: 2}),
        'yield' as const,
        makeNode({id: 3}),
      ];
      const overlay: Change = {type: 'add', node: makeNode({id: 2})};

      const result = collectNodes(
        generateWithOverlayUnordered(stream, overlay, schema),
      );
      expect(result).toEqual([
        expect.objectContaining({row: {id: 1}}),
        'yield',
        'yield',
        expect.objectContaining({row: {id: 3}}),
      ]);
    });
  });
});

describe('generateWithOverlayNoYieldUnordered', () => {
  const schema = makeSchema(['id']);

  test('strips yield markers from output', () => {
    function* stream(): Stream<Node> {
      yield makeNode({id: 1});
      yield makeNode({id: 2});
      yield makeNode({id: 3});
    }
    const overlay: Change = {type: 'add', node: makeNode({id: 2})};

    const result = [
      ...generateWithOverlayNoYieldUnordered(stream(), overlay, schema),
    ];
    expect(result).toHaveLength(2);
    expect(result.map(n => n.row)).toEqual([{id: 1}, {id: 3}]);
  });
});

describe('rowEqualsForCompoundKey', () => {
  test('single key match', () => {
    expect(rowEqualsForCompoundKey({id: 1}, {id: 1}, ['id'])).toBe(true);
  });

  test('single key mismatch', () => {
    expect(rowEqualsForCompoundKey({id: 1}, {id: 2}, ['id'])).toBe(false);
  });

  test('compound key all match', () => {
    expect(
      rowEqualsForCompoundKey({a: 1, b: 'x'}, {a: 1, b: 'x'}, ['a', 'b']),
    ).toBe(true);
  });

  test('compound key partial mismatch', () => {
    expect(
      rowEqualsForCompoundKey({a: 1, b: 'x'}, {a: 1, b: 'y'}, ['a', 'b']),
    ).toBe(false);
  });

  test('null equals null (compareValues treats null as a real value)', () => {
    expect(rowEqualsForCompoundKey({id: null}, {id: null}, ['id'])).toBe(true);
  });

  test('extra columns ignored', () => {
    expect(
      rowEqualsForCompoundKey({id: 1, val: 'a'}, {id: 1, val: 'b'}, ['id']),
    ).toBe(true);
  });
});

describe('isJoinMatch', () => {
  test('single key match', () => {
    expect(isJoinMatch({id: 1}, ['id'], {id: 1}, ['id'])).toBe(true);
  });

  test('single key mismatch', () => {
    expect(isJoinMatch({id: 1}, ['id'], {id: 2}, ['id'])).toBe(false);
  });

  test('compound key match with different column names', () => {
    expect(
      isJoinMatch({a: 1, b: 'x'}, ['a', 'b'], {x: 1, y: 'x'}, ['x', 'y']),
    ).toBe(true);
  });

  test('null parent value returns false (SQL NULL semantics)', () => {
    expect(isJoinMatch({id: null}, ['id'], {id: 1}, ['id'])).toBe(false);
  });

  test('null child value returns false', () => {
    expect(isJoinMatch({id: 1}, ['id'], {id: null}, ['id'])).toBe(false);
  });

  test('both null returns false (unlike rowEqualsForCompoundKey)', () => {
    expect(isJoinMatch({id: null}, ['id'], {id: null}, ['id'])).toBe(false);
  });
});

describe('buildJoinConstraint', () => {
  test('single key maps value correctly', () => {
    expect(buildJoinConstraint({id: 1}, ['id'], ['id'])).toEqual({id: 1});
  });

  test('compound key maps all values', () => {
    expect(buildJoinConstraint({a: 1, b: 'x'}, ['a', 'b'], ['a', 'b'])).toEqual(
      {a: 1, b: 'x'},
    );
  });

  test('null value returns undefined', () => {
    expect(buildJoinConstraint({id: null}, ['id'], ['id'])).toBeUndefined();
  });

  test('null in second position returns undefined', () => {
    expect(
      buildJoinConstraint({a: 1, b: null}, ['a', 'b'], ['x', 'y']),
    ).toBeUndefined();
  });

  test('different source/target key names', () => {
    expect(
      buildJoinConstraint(
        {userId: 5, orgId: 10},
        ['userId', 'orgId'],
        ['id', 'org'],
      ),
    ).toEqual({id: 5, org: 10});
  });
});
