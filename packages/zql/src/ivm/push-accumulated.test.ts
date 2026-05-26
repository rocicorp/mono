/* oxlint-disable @typescript-eslint/no-explicit-any */
import {beforeEach, describe, expect, test} from 'vitest';
import {emptyArray, identity} from '../../../shared/src/sentinels.ts';
import {ChangeIndex} from './change-index.js';
import {ChangeType} from './change-type.js';
import {
  makeAddChange,
  makeChildChange,
  makeEditChange,
  makeRemoveChange,
  type AddChange,
  type Change,
  type EditChange,
  type RemoveChange,
} from './change.js';
import type {InputBase, Output} from './operator.js';
import {
  pushAccumulatedChanges as genPushAccumulatedChanges,
  makeAddEmptyRelationships,
  mergeEmpty,
  mergeRelationships,
} from './push-accumulated.js';
import type {SourceSchema} from './schema.js';

const mockPusher: InputBase = {
  getSchema: () => mockSchema as any,
  destroy: () => {},
};

function pushAccumulatedChanges(
  accumulatedPushes: Change[],
  output: Output,
  pusher: InputBase,
  fanOutChangeType: ChangeType,
  mergeRelationships: (existing: Change, incoming: Change) => Change,
  addEmptyRelationships: (change: Change) => Change,
) {
  [
    ...genPushAccumulatedChanges(
      accumulatedPushes,
      output,
      pusher,
      fanOutChangeType,
      mergeRelationships,
      addEmptyRelationships,
    ),
  ];
}

const mockChildChange: Change = makeChildChange(
  {row: {id: 1}, relationships: {}},
  {
    change: makeAddChange({row: {id: 2}, relationships: {}}),
    relationshipName: 'child',
  },
);

const mockSchema: SourceSchema = {
  tableName: 'test',
  columns: {},
  primaryKey: ['id'],
  relationships: {
    rel1: {} as any,
    rel2: {} as any,
  },
  compareRows: () => 0,
  isHidden: false,
  sort: [],
  system: 'client',
};

describe('pushAccumulatedChanges', () => {
  let output: Output;
  let pushedChanges: Change[];

  beforeEach(() => {
    pushedChanges = [];
    output = {
      push: (change: Change) => {
        pushedChanges.push(change);
        return emptyArray;
      },
    } as Output;
  });

  describe('invariant: add coming in will only create adds coming out', () => {
    test('single add change passes through', () => {
      const accumulatedPushes: Change[] = [
        makeAddChange({row: {id: 1}, relationships: {}}),
      ];

      pushAccumulatedChanges(
        accumulatedPushes,
        output,
        mockPusher,
        ChangeType.ADD,
        mergeRelationships,
        identity,
      );

      expect(pushedChanges).toHaveLength(1);
      expect(pushedChanges[0]?.[ChangeIndex.TYPE]).toBe(ChangeType.ADD);
    });

    test('multiple add changes collapse to single add', () => {
      const accumulatedPushes: Change[] = [
        makeAddChange({row: {id: 1}, relationships: {rel1: () => []}}),
        makeAddChange({row: {id: 1}, relationships: {rel2: () => []}}),
      ];

      pushAccumulatedChanges(
        accumulatedPushes,
        output,
        mockPusher,
        ChangeType.ADD,
        mergeRelationships,
        identity,
      );

      expect(pushedChanges).toHaveLength(1);
      expect(pushedChanges[0]?.[ChangeIndex.TYPE]).toBe(ChangeType.ADD);
      expect(
        Object.keys(pushedChanges[0]?.[ChangeIndex.NODE]?.relationships ?? {}),
      ).toEqual(expect.arrayContaining(['rel1', 'rel2']));
    });

    test('no changes when all branches filter out add', () => {
      const accumulatedPushes: Change[] = [];

      pushAccumulatedChanges(
        accumulatedPushes,
        output,
        mockPusher,
        ChangeType.ADD,
        mergeRelationships,
        identity,
      );

      expect(pushedChanges).toHaveLength(0);
    });
  });

  describe('invariant: remove coming in will only create removes coming out', () => {
    test('single remove change passes through', () => {
      const accumulatedPushes: Change[] = [
        makeRemoveChange({row: {id: 1}, relationships: {}}),
      ];

      pushAccumulatedChanges(
        accumulatedPushes,
        output,
        mockPusher,
        ChangeType.REMOVE,
        mergeRelationships,
        identity,
      );

      expect(pushedChanges).toHaveLength(1);
      expect(pushedChanges[0]?.[ChangeIndex.TYPE]).toBe(ChangeType.REMOVE);
    });

    test('multiple remove changes collapse to single remove', () => {
      const accumulatedPushes: Change[] = [
        makeRemoveChange({row: {id: 1}, relationships: {rel1: () => []}}),
        makeRemoveChange({row: {id: 1}, relationships: {rel2: () => []}}),
      ];

      pushAccumulatedChanges(
        accumulatedPushes,
        output,
        mockPusher,
        ChangeType.REMOVE,
        mergeRelationships,
        identity,
      );

      expect(pushedChanges).toHaveLength(1);
      expect(pushedChanges[0]?.[ChangeIndex.TYPE]).toBe(ChangeType.REMOVE);
      expect(
        Object.keys(pushedChanges[0]?.[ChangeIndex.NODE]?.relationships ?? {}),
      ).toEqual(expect.arrayContaining(['rel1', 'rel2']));
    });
  });

  describe('invariant: edit coming in can create adds, removes, and edits coming out', () => {
    test('edit preserved as edit', () => {
      const accumulatedPushes: Change[] = [
        makeEditChange(
          {row: {id: 1, value: 2}, relationships: {}},
          {row: {id: 1, value: 1}, relationships: {}},
        ),
      ];

      pushAccumulatedChanges(
        accumulatedPushes,
        output,
        mockPusher,
        ChangeType.EDIT,
        mergeRelationships,
        identity,
      );

      expect(pushedChanges).toHaveLength(1);
      expect(pushedChanges[0]?.[ChangeIndex.TYPE]).toBe(ChangeType.EDIT);
    });

    test('edit converted to add only', () => {
      const accumulatedPushes: Change[] = [
        makeAddChange({row: {id: 1, value: 2}, relationships: {}}),
      ];

      pushAccumulatedChanges(
        accumulatedPushes,
        output,
        mockPusher,
        ChangeType.EDIT,
        mergeRelationships,
        identity,
      );

      expect(pushedChanges).toHaveLength(1);
      expect(pushedChanges[0]?.[ChangeIndex.TYPE]).toBe(ChangeType.ADD);
    });

    test('edit converted to remove only', () => {
      const accumulatedPushes: Change[] = [
        makeRemoveChange({row: {id: 1, value: 1}, relationships: {}}),
      ];

      pushAccumulatedChanges(
        accumulatedPushes,
        output,
        mockPusher,
        ChangeType.EDIT,
        mergeRelationships,
        identity,
      );

      expect(pushedChanges).toHaveLength(1);
      expect(pushedChanges[0]?.[ChangeIndex.TYPE]).toBe(ChangeType.REMOVE);
    });

    test('edit split into add and remove recombines to edit', () => {
      const accumulatedPushes: Change[] = [
        makeAddChange({row: {id: 1, value: 2}, relationships: {}}),
        makeRemoveChange({row: {id: 1, value: 1}, relationships: {}}),
      ];

      pushAccumulatedChanges(
        accumulatedPushes,
        output,
        mockPusher,
        ChangeType.EDIT,
        mergeRelationships,
        identity,
      );

      expect(pushedChanges).toHaveLength(1);
      expect(pushedChanges[0]?.[ChangeIndex.TYPE]).toBe(ChangeType.EDIT);
      expect(pushedChanges[0]).toEqual(
        makeEditChange(
          {row: {id: 1, value: 2}, relationships: {}},
          {row: {id: 1, value: 1}, relationships: {}},
        ),
      );
    });

    test('edit supersedes add and remove when all three present', () => {
      const accumulatedPushes: Change[] = [
        makeEditChange(
          {row: {id: 1, value: 3}, relationships: {editRel: () => []}},
          {row: {id: 1, value: 0}, relationships: {}},
        ),
        makeAddChange({
          row: {id: 1, value: 2},
          relationships: {addRel: () => []},
        }),
        makeRemoveChange({
          row: {id: 1, value: 1},
          relationships: {removeRel: () => []},
        }),
      ];

      pushAccumulatedChanges(
        accumulatedPushes,
        output,
        mockPusher,
        ChangeType.EDIT,
        mergeRelationships,
        identity,
      );

      expect(pushedChanges).toHaveLength(1);
      expect(pushedChanges[0]?.[ChangeIndex.TYPE]).toBe(ChangeType.EDIT);
      const editChange = pushedChanges[0] as EditChange;
      expect(Object.keys(editChange[ChangeIndex.NODE].relationships)).toEqual(
        expect.arrayContaining(['editRel', 'addRel']),
      );
      expect(
        Object.keys(editChange[ChangeIndex.OLD_NODE].relationships),
      ).toEqual(expect.arrayContaining(['removeRel']));
    });
  });

  describe('invariant: child coming in can create adds, removes, and children coming out', () => {
    test('child preserved as child takes precedence', () => {
      const accumulatedPushes: Change[] = [mockChildChange];

      pushAccumulatedChanges(
        accumulatedPushes,
        output,
        mockPusher,
        ChangeType.CHILD,
        mergeRelationships,
        identity,
      );

      expect(pushedChanges).toHaveLength(1);
      expect(pushedChanges[0]?.[ChangeIndex.TYPE]).toBe(ChangeType.CHILD);
    });

    test('child converted to add only', () => {
      const accumulatedPushes: Change[] = [
        makeAddChange({row: {id: 1}, relationships: {}}),
      ];

      pushAccumulatedChanges(
        accumulatedPushes,
        output,
        mockPusher,
        ChangeType.CHILD,
        mergeRelationships,
        identity,
      );

      expect(pushedChanges).toHaveLength(1);
      expect(pushedChanges[0]?.[ChangeIndex.TYPE]).toBe(ChangeType.ADD);
    });

    test('child converted to remove only', () => {
      const accumulatedPushes: Change[] = [
        makeRemoveChange({row: {id: 1}, relationships: {}}),
      ];

      pushAccumulatedChanges(
        accumulatedPushes,
        output,
        mockPusher,
        ChangeType.CHILD,
        mergeRelationships,
        identity,
      );

      expect(pushedChanges).toHaveLength(1);
      expect(pushedChanges[0]?.[ChangeIndex.TYPE]).toBe(ChangeType.REMOVE);
    });

    test('child takes precedence over add/remove when present', () => {
      const accumulatedPushes: Change[] = [
        mockChildChange,
        makeAddChange({row: {id: 1}, relationships: {}}),
      ];

      pushAccumulatedChanges(
        accumulatedPushes,
        output,
        mockPusher,
        ChangeType.CHILD,
        mergeRelationships,
        identity,
      );

      expect(pushedChanges).toHaveLength(1);
      expect(pushedChanges[0]?.[ChangeIndex.TYPE]).toBe(ChangeType.CHILD);
    });

    test('child ensures at most one add or remove (not both)', () => {
      // This should assert fail if both add and remove are present without child
      const accumulatedPushes: Change[] = [
        makeAddChange({row: {id: 1}, relationships: {}}),
        makeRemoveChange({row: {id: 2}, relationships: {}}),
      ];

      expect(() => {
        pushAccumulatedChanges(
          accumulatedPushes,
          output,
          mockPusher,
          ChangeType.CHILD,
          mergeRelationships,
          identity,
        );
      }).toThrow('Fan-in:child expected either add or remove, not both');
    });
  });
});

describe('mergeRelationships', () => {
  test('merges relationships from add changes', () => {
    const left: Change = makeAddChange({
      row: {id: 1},
      relationships: {rel1: () => []},
    });
    const right: Change = makeAddChange({
      row: {id: 1},
      relationships: {rel2: () => []},
    });

    const result = mergeRelationships(left, right);

    expect(result[ChangeIndex.TYPE]).toBe(ChangeType.ADD);
    expect(Object.keys(result[ChangeIndex.NODE].relationships)).toEqual(
      expect.arrayContaining(['rel1', 'rel2']),
    );
  });

  test('merges relationships from remove changes', () => {
    const left: Change = makeRemoveChange({
      row: {id: 1},
      relationships: {rel1: () => []},
    });
    const right: Change = makeRemoveChange({
      row: {id: 1},
      relationships: {rel2: () => []},
    });

    const result = mergeRelationships(left, right);

    expect(result[ChangeIndex.TYPE]).toBe(ChangeType.REMOVE);
    expect(Object.keys(result[ChangeIndex.NODE].relationships)).toEqual(
      expect.arrayContaining(['rel1', 'rel2']),
    );
  });

  test('merges relationships from edit changes', () => {
    const left: Change = makeEditChange(
      {row: {id: 1}, relationships: {rel1: () => []}},
      {row: {id: 1}, relationships: {oldRel1: () => []}},
    );
    const right: Change = makeEditChange(
      {row: {id: 1}, relationships: {rel2: () => []}},
      {row: {id: 1}, relationships: {oldRel2: () => []}},
    );

    const result = mergeRelationships(left, right) as EditChange;

    expect(result[ChangeIndex.TYPE]).toBe(ChangeType.EDIT);
    expect(Object.keys(result[ChangeIndex.NODE].relationships)).toEqual(
      expect.arrayContaining(['rel1', 'rel2']),
    );
    expect(Object.keys(result[ChangeIndex.OLD_NODE].relationships)).toEqual(
      expect.arrayContaining(['oldRel1', 'oldRel2']),
    );
  });

  test('left takes precedence when same relationship exists', () => {
    const rel1Left = () => [];
    const rel1Right = () => [];

    const left: Change = makeAddChange({
      row: {id: 1},
      relationships: {rel1: rel1Left},
    });
    const right: Change = makeAddChange({
      row: {id: 1},
      relationships: {rel1: rel1Right},
    });

    const result = mergeRelationships(left, right) as AddChange;

    expect(result[ChangeIndex.NODE].relationships.rel1).toBe(rel1Left);
  });

  test('merges edit with add', () => {
    const left: Change = makeEditChange(
      {row: {id: 1}, relationships: {editRel: () => []}},
      {row: {id: 1}, relationships: {}},
    );
    const right: Change = makeAddChange({
      row: {id: 1},
      relationships: {addRel: () => []},
    });

    const result = mergeRelationships(left, right) as EditChange;

    expect(result[ChangeIndex.TYPE]).toBe(ChangeType.EDIT);
    expect(Object.keys(result[ChangeIndex.NODE].relationships)).toEqual(
      expect.arrayContaining(['editRel', 'addRel']),
    );
  });

  test('merges edit with remove', () => {
    const left: Change = makeEditChange(
      {row: {id: 1}, relationships: {}},
      {row: {id: 1}, relationships: {editOldRel: () => []}},
    );
    const right: Change = makeRemoveChange({
      row: {id: 1},
      relationships: {removeRel: () => []},
    });

    const result = mergeRelationships(left, right) as EditChange;

    expect(result[ChangeIndex.TYPE]).toBe(ChangeType.EDIT);
    expect(Object.keys(result[ChangeIndex.OLD_NODE].relationships)).toEqual(
      expect.arrayContaining(['editOldRel', 'removeRel']),
    );
  });

  test('merges relationships from child changes', () => {
    const childInfo = {
      change: makeAddChange({row: {id: 2}, relationships: {}}),
      relationshipName: 'childRel',
    };
    const left: Change = makeChildChange(
      {row: {id: 1}, relationships: {rel1: () => []}},
      childInfo,
    );
    const right: Change = makeChildChange(
      {row: {id: 1}, relationships: {rel2: () => []}},
      childInfo,
    );

    const result = mergeRelationships(left, right);

    expect(result[ChangeIndex.TYPE]).toBe(ChangeType.CHILD);
    expect(Object.keys(result[ChangeIndex.NODE].relationships)).toEqual(
      expect.arrayContaining(['rel1', 'rel2']),
    );
  });
});

describe('makeAddEmptyRelationships', () => {
  test('adds empty relationships for add change', () => {
    const schema: SourceSchema = mockSchema;

    const addEmptyRelationships = makeAddEmptyRelationships(schema);

    const change: Change = makeAddChange({row: {id: 1}, relationships: {}});

    const result = addEmptyRelationships(change) as AddChange;

    expect(Object.keys(result[ChangeIndex.NODE].relationships)).toEqual(
      expect.arrayContaining(['rel1', 'rel2']),
    );
    expect(result[ChangeIndex.NODE].relationships.rel1?.()).toEqual([]);
    expect(result[ChangeIndex.NODE].relationships.rel2?.()).toEqual([]);
  });

  test('adds empty relationships for remove change', () => {
    const schema: SourceSchema = mockSchema;

    const addEmptyRelationships = makeAddEmptyRelationships(schema);

    const change: Change = makeRemoveChange({row: {id: 1}, relationships: {}});

    const result = addEmptyRelationships(change) as RemoveChange;

    expect(Object.keys(result[ChangeIndex.NODE].relationships)).toEqual(
      expect.arrayContaining(['rel1', 'rel2']),
    );
  });

  test('adds empty relationships for edit change', () => {
    const schema: SourceSchema = mockSchema;

    const addEmptyRelationships = makeAddEmptyRelationships(schema);

    const change: Change = makeEditChange(
      {row: {id: 1}, relationships: {}},
      {row: {id: 1}, relationships: {}},
    );

    const result = addEmptyRelationships(change) as EditChange;

    expect(Object.keys(result[ChangeIndex.NODE].relationships)).toEqual(
      expect.arrayContaining(['rel1', 'rel2']),
    );
    expect(Object.keys(result[ChangeIndex.OLD_NODE].relationships)).toEqual(
      expect.arrayContaining(['rel1', 'rel2']),
    );
  });

  test('preserves existing relationships', () => {
    const schema: SourceSchema = mockSchema;

    const addEmptyRelationships = makeAddEmptyRelationships(schema);

    const existingRel = () => [{row: {id: 2}, relationships: {}}];
    const change: Change = makeAddChange({
      row: {id: 1},
      relationships: {rel1: existingRel},
    });

    const result = addEmptyRelationships(change) as AddChange;

    expect(result[ChangeIndex.NODE].relationships.rel1).toBe(existingRel);
    expect(result[ChangeIndex.NODE].relationships.rel2?.()).toEqual([]);
  });

  test('does not modify child changes', () => {
    const schema: SourceSchema = mockSchema;

    const addEmptyRelationships = makeAddEmptyRelationships(schema);

    const change: Change = mockChildChange;

    const result = addEmptyRelationships(change);

    expect(result).toBe(change);
  });

  test('returns unchanged when schema has no relationships', () => {
    const schema: SourceSchema = {
      tableName: 'test',
      columns: {},
      primaryKey: ['id'],
      relationships: {},
      compareRows: () => 0,
      isHidden: false,
      sort: [],
      system: 'client',
    };

    const addEmptyRelationships = makeAddEmptyRelationships(schema);

    const change: Change = makeAddChange({row: {id: 1}, relationships: {}});

    const result = addEmptyRelationships(change);

    expect(result).toBe(change);
  });
});

describe('mergeEmpty', () => {
  test('adds empty streams for missing relationships', () => {
    const relationships: Record<string, () => any[]> = {
      existing: () => [{id: 1}],
    };

    mergeEmpty(relationships, ['existing', 'new1', 'new2']);

    expect(Object.keys(relationships)).toEqual(
      expect.arrayContaining(['existing', 'new1', 'new2']),
    );
    expect(relationships.existing()).toEqual([{id: 1}]);
    expect(relationships.new1()).toEqual([]);
    expect(relationships.new2()).toEqual([]);
  });

  test('does not overwrite existing relationships', () => {
    const existingFn = () => [{id: 1}];
    const relationships: Record<string, () => any[]> = {
      rel1: existingFn,
    };

    mergeEmpty(relationships, ['rel1', 'rel2']);

    expect(relationships.rel1).toBe(existingFn);
    expect(relationships.rel2()).toEqual([]);
  });
});
