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

suite(
  'relationships with issues, comments, and owners (i->c one to many) (i -> o one to one o -> i one to many ',
  () => {
    const base = {
      columns: [
        {id: 'string', ownerId: 'string'} as const, // Issues
        {id: 'string', issueId: 'string'} as const, // Comments
        {id: 'string'} as const, // Owners
      ],
      primaryKeys: [['id'], ['id'], ['id']],
      joins: [
        {
          parentKey: 'id', //issue id
          childKey: 'issueId', //comment issueid
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
        ], // Issues
        [
          {id: 'c1', issueId: 'i1'},
          {id: 'c2', issueId: 'i1'},
          {id: 'c3', issueId: 'i2'},
          {id: 'c4', issueId: 'i2'},
        ], // Comments
        [{id: 'o1'}, {id: 'o2'}], // Owners
      ],
      pushes: [[0, {type: 'add', row: {id: 'i3', ownerId: 'o2'}}]], // Push a new issue with a new owner
      expectedLog: [
        ['0', 'push', {type: 'add', row: {id: 'i3', ownerId: 'o2'}}],
        ['1', 'fetch', {constraint: {key: 'issueId', value: 'i3'}}],
        ['2', 'fetch', {constraint: {key: 'id', value: 'o2'}}],
        ['1', 'fetchCount', {constraint: {key: 'issueId', value: 'i3'}}, 0],
        ['2', 'fetchCount', {constraint: {key: 'id', value: 'o2'}}, 1],
      ],
      expectedPrimaryKeySetStorageKeys: [[['i3', 'i3']], [['o2', 'i3']]],
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
      name: 'dangling ownerId, push existing ownerId',
      sources: [
        [
          {id: 'i1', ownerId: 'o1'},
          {id: 'i2', ownerId: 'o2'},
        ], // Issues
        [
          {id: 'c1', issueId: 'i1'},
          {id: 'c2', issueId: 'i1'},
          {id: 'c3', issueId: 'i2'},
          {id: 'c4', issueId: 'i2'},
        ], // Comments
        [{id: 'o1'}], // Owners
      ],
      pushes: [[2, {type: 'add', row: {id: 'o2'}}]], // Push a new owner
      expectedLog: [
        ['2', 'push', {type: 'add', row: {id: 'o2'}}],
        ['0', 'fetch', {constraint: {key: 'ownerId', value: 'o2'}}],
        ['1', 'fetch', {constraint: {key: 'issueId', value: 'i2'}}],
        ['0', 'fetchCount', {constraint: {key: 'ownerId', value: 'o2'}}, 1],
        //['1', 'fetchCount', {constraint: {key: 'issueId', value: 'i2'}}, 0],
      ],
      expectedPrimaryKeySetStorageKeys: [[['i2', 'i2']], []],
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
  },
);

///test:
// create two issues with two comments
// create two owners
// push one new issue
// what are the appropriate asserts
//todo: updatee pushSiblingTest with new joins skip
function pushSiblingTest(t: PushTestSibling) {
  test(t.name, () => {
    assert(t.sources.length > 0);
    assert(t.joins.length === t.sources.length - 1);

    const log: SnitchMessage[] = [];

    const sources = t.sources.map((fetch, i) => {
      const ordering = t.sorts?.[i] ?? [['id', 'asc']];
      const source = new MemorySource(t.columns[i], t.primaryKeys[i]);
      for (const row of fetch) {
        source.push({type: 'add', row});
      }
      const snitch = new Snitch(source.connect(ordering), String(i), log, true);
      return {
        source,
        snitch,
      };
    });

    const joins: {
      join: Join;
      storage: MemoryStorage;
    }[] = [];

    // the one at index 0 is the parent
    // siblings you can  do in forward order
    // ----
    // join 0 joins source 0 and source 1 producing output 0
    // join 1 joins output 0 and source 2 producing output 1
    // join n joins output n-1 and source n+1 producing output n
    // intialize parent to source 0
    // iterate joins in order
    // join parent to source i+1
    // set parent = to output of join
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
      );

      console.log('join', join);
      joins[i] = {
        join,
        storage,
      };

      parent = join;
    }

    const finalJoin = joins[joins.length - 1];
    const c = new Catch(finalJoin.join);

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
