import {expect, suite, test} from 'vitest';
import {Join, createPrimaryKeySetStorageKey} from './join.js';
import {MemorySource} from './memory-source.js';
import {MemoryStorage} from './memory-storage.js';
import {SnitchMessage, Snitch} from './snitch.js';
import type {NormalizedValue, Row} from './data.js';
import {assert} from 'shared/src/asserts.js';
import type {Ordering} from '../ast2/ast.js';
import {Catch} from './catch.js';
import type {Change} from './change.js';
import type {SourceChange} from './source.js';
import type {ValueType} from './schema.js';
import {Input} from './operator.js';

suite('sibling relationships tests with issues, comments, and owners', () => {
  const base = {
    columns: [
      {id: 'string', ownerId: 'string'} as const,
      {id: 'string', issueId: 'string'} as const,
      {id: 'string'} as const,
    ],
    primaryKeys: [['id'], ['id'], ['id']],
    joins: [
      {
        parentKey: 'id',
        childKey: 'issueId',
        relationshipName: 'comments',
      },
      {
        parentKey: 'ownerId',
        childKey: 'id',
        relationshipName: 'owners',
      },
    ],
  };

  pushSiblingTest({
    ...base,
    name: 'create two issues, two comments each, one owner each, push a new issue with existing owner',
    sources: [
      [
        {id: 'i1', ownerId: 'o1'},
        {id: 'i2', ownerId: 'o2'},
      ],
      [
        {id: 'c1', issueId: 'i1'},
        {id: 'c2', issueId: 'i1'},
        {id: 'c3', issueId: 'i2'},
        {id: 'c4', issueId: 'i2'},
      ],
      [{id: 'o1'}, {id: 'o2'}],
    ],
    pushes: [[0, {type: 'add', row: {id: 'i3', ownerId: 'o2'}}]],
    expectedLog: [
      ['0', 'push', {type: 'add', row: {id: 'i3', ownerId: 'o2'}}],
      ['1', 'fetch', {constraint: {key: 'issueId', value: 'i3'}}],
      ['2', 'fetch', {constraint: {key: 'id', value: 'o2'}}],
      ['1', 'fetchCount', {constraint: {key: 'issueId', value: 'i3'}}, 0],
      ['2', 'fetchCount', {constraint: {key: 'id', value: 'o2'}}, 1],
    ],
    expectedPrimaryKeySetStorageKeys: [
      [
        ['i1', 'i1'],
        ['i2', 'i2'],
        ['i3', 'i3'],
      ],
      [
        ['o1', 'i1'],
        ['o2', 'i2'],
        ['o2', 'i3'],
      ],
    ],
    expectedOutput: [
      {
        type: 'add',
        node: {
          relationships: {
            comments: [],
            owners: [
              {
                row: {id: 'o2'},
                relationships: {},
              },
            ],
          },
          row: {
            id: 'i3',
            ownerId: 'o2',
          },
        },
      },
    ],
  });

  pushSiblingTest({
    ...base,
    name: 'push owner',
    sources: [
      [
        {id: 'i1', ownerId: 'o1'},
        {id: 'i2', ownerId: 'o2'},
      ],
      [
        {id: 'c1', issueId: 'i1'},
        {id: 'c2', issueId: 'i1'},
        {id: 'c3', issueId: 'i2'},
        {id: 'c4', issueId: 'i2'},
      ],
      [{id: 'o1'}],
    ],
    pushes: [[2, {type: 'add', row: {id: 'o2'}}]],
    expectedLog: [
      ['2', 'push', {type: 'add', row: {id: 'o2'}}],
      ['0', 'fetch', {constraint: {key: 'ownerId', value: 'o2'}}],
      ['1', 'fetch', {constraint: {key: 'issueId', value: 'i2'}}],
      ['0', 'fetchCount', {constraint: {key: 'ownerId', value: 'o2'}}, 1],
    ],
    expectedPrimaryKeySetStorageKeys: [
      [
        ['i1', 'i1'],
        ['i2', 'i2'],
      ],
      [
        ['o1', 'i1'],
        ['o2', 'i2'],
      ],
    ],
    expectedOutput: [
      {
        type: 'child',
        row: {id: 'i2', ownerId: 'o2'},
        child: {
          relationshipName: 'owners',
          change: {
            type: 'add',
            node: {
              relationships: {},
              row: {id: 'o2'},
            },
          },
        },
      },
    ],
  });

  pushSiblingTest({
    ...base,
    name: 'push comment',
    sources: [
      [
        {id: 'i1', ownerId: 'o1'},
        {id: 'i2', ownerId: 'o2'},
      ],
      [
        {id: 'c1', issueId: 'i1'},
        {id: 'c2', issueId: 'i1'},
        {id: 'c3', issueId: 'i2'},
        {id: 'c4', issueId: 'i2'},
      ],
      [{id: 'o1'}, {id: 'o2'}],
    ],
    pushes: [[1, {type: 'add', row: {id: 'c5', issueId: 'i1'}}]],
    expectedLog: [
      ['1', 'push', {type: 'add', row: {id: 'c5', issueId: 'i1'}}],
      ['0', 'fetch', {constraint: {key: 'id', value: 'i1'}}],
      ['0', 'fetchCount', {constraint: {key: 'id', value: 'i1'}}, 1],
    ],
    expectedPrimaryKeySetStorageKeys: [
      [
        ['i1', 'i1'],
        ['i2', 'i2'],
      ],
      [
        ['o1', 'i1'],
        ['o2', 'i2'],
      ],
    ],
    expectedOutput: [
      {
        type: 'child',
        row: {id: 'i1', ownerId: 'o1'},
        child: {
          relationshipName: 'comments',
          change: {
            type: 'add',
            node: {row: {id: 'c5', issueId: 'i1'}, relationships: {}},
          },
        },
      },
    ],
  });

  pushSiblingTest({
    ...base,
    name: 'remove owner',
    sources: [
      [
        {id: 'i1', ownerId: 'o1'},
        {id: 'i2', ownerId: 'o2'},
      ],
      [
        {id: 'c1', issueId: 'i1'},
        {id: 'c2', issueId: 'i1'},
        {id: 'c3', issueId: 'i2'},
        {id: 'c4', issueId: 'i2'},
      ],
      [{id: 'o1'}, {id: 'o2'}],
    ],
    pushes: [[2, {type: 'remove', row: {id: 'o2'}}]],
    expectedLog: [
      ['2', 'push', {type: 'remove', row: {id: 'o2'}}],
      ['0', 'fetch', {constraint: {key: 'ownerId', value: 'o2'}}],
      ['1', 'fetch', {constraint: {key: 'issueId', value: 'i2'}}],
      ['0', 'fetchCount', {constraint: {key: 'ownerId', value: 'o2'}}, 1],
    ],
    expectedPrimaryKeySetStorageKeys: [
      [
        ['i1', 'i1'],
        ['i2', 'i2'],
      ],
      [
        ['o1', 'i1'],
        ['o2', 'i2'],
      ],
    ],
    expectedOutput: [
      {
        type: 'child',
        row: {id: 'i2', ownerId: 'o2'},
        child: {
          relationshipName: 'owners',
          change: {
            type: 'remove',
            node: {
              relationships: {},
              row: {id: 'o2'},
            },
          },
        },
      },
    ],
  });

  pushSiblingTest({
    ...base,
    name: 'remove comment',
    sources: [
      [
        {id: 'i1', ownerId: 'o1'},
        {id: 'i2', ownerId: 'o2'},
      ],
      [
        {id: 'c1', issueId: 'i1'},
        {id: 'c2', issueId: 'i1'},
        {id: 'c3', issueId: 'i2'},
        {id: 'c4', issueId: 'i2'},
      ],
      [{id: 'o1'}, {id: 'o2'}],
    ],
    pushes: [[1, {type: 'remove', row: {id: 'c4', issueId: 'i2'}}]],
    expectedLog: [
      ['1', 'push', {type: 'remove', row: {id: 'c4', issueId: 'i2'}}],
      ['0', 'fetch', {constraint: {key: 'id', value: 'i2'}}],
      ['0', 'fetchCount', {constraint: {key: 'id', value: 'i2'}}, 1],
    ],
    expectedPrimaryKeySetStorageKeys: [
      [
        ['i1', 'i1'],
        ['i2', 'i2'],
      ],
      [
        ['o1', 'i1'],
        ['o2', 'i2'],
      ],
    ],
    expectedOutput: [
      {
        type: 'child',
        row: {id: 'i2', ownerId: 'o2'},
        child: {
          relationshipName: 'comments',
          change: {
            type: 'remove',
            node: {row: {id: 'c4', issueId: 'i2'}, relationships: {}},
          },
        },
      },
    ],
  });
});

function pushSiblingTest(t: PushTestSibling) {
  test(t.name, () => {
    assert(t.sources.length > 0);
    assert(t.joins.length === t.sources.length - 1);

    const log: SnitchMessage[] = [];

    const sources = t.sources.map((rows, i) => {
      const ordering = t.sorts?.[i] ?? [['id', 'asc']];
      const source = new MemorySource('test', t.columns[i], t.primaryKeys[i]);
      for (const row of rows) {
        source.push({type: 'add', row});
      }
      const snitch = new Snitch(source.connect(ordering), String(i), log, [
        'fetch',
        'fetchCount',
        'push',
        'cleanup',
      ]);
      return {
        source,
        snitch,
      };
    });

    const joins: {
      join: Join;
      storage: MemoryStorage;
    }[] = [];

    let parent: Input = sources[0].snitch;

    for (let i = 0; i < t.joins.length; i++) {
      const info = t.joins[i];
      const child = sources[i + 1].snitch;
      const storage = new MemoryStorage();

      const join = new Join(
        parent,
        child,
        storage,
        info.parentKey,
        info.childKey,
        info.relationshipName,
        false,
      );

      joins[i] = {
        join,
        storage,
      };

      parent = join;
    }

    const finalJoin = joins[joins.length - 1];

    const c = new Catch(finalJoin.join);
    c.fetch();

    log.length = 0;

    for (const [sourceIndex, change] of t.pushes) {
      sources[sourceIndex].source.push(change);
    }

    for (const [i, j] of joins.entries()) {
      const {storage} = j;
      const expectedStorageKeys = t.expectedPrimaryKeySetStorageKeys[i];
      const expectedStorage: Record<string, boolean> = {};
      for (const k of expectedStorageKeys) {
        expectedStorage[createPrimaryKeySetStorageKey(k)] = true;
      }
      expect(storage.cloneData()).toEqual(expectedStorage);
    }

    expect(t.expectedLog).toEqual(log);
    expect(t.expectedOutput).toEqual(c.pushes);
  });
}

type PushTestSibling = {
  name: string;
  columns: Record<string, ValueType>[];
  primaryKeys: readonly string[][];
  sources: Row[][];
  sorts?: Record<number, Ordering> | undefined;
  joins: {
    parentKey: string;
    childKey: string;
    relationshipName: string;
  }[];
  pushes: [sourceIndex: number, change: SourceChange][];
  expectedLog: SnitchMessage[];
  expectedPrimaryKeySetStorageKeys: NormalizedValue[][][];
  expectedOutput: Change[];
};
