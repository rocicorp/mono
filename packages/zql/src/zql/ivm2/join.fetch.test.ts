import {expect, suite, test} from 'vitest';
import {
  Join,
  createPrimaryKeySetStorageKey,
  createPrimaryKeySetStorageKeyPrefix,
} from './join.js';
import {MemorySource} from './memory-source.js';
import {MemoryStorage} from './memory-storage.js';
import {PushMessage, Snitch, SnitchMessage} from './snitch.js';
import type {Row, Node, NormalizedValue} from './data.js';
import {assert} from 'shared/src/asserts.js';
import type {Ordering} from '../ast2/ast.js';
import {Catch} from './catch.js';
import type {Schema, ValueType} from './schema.js';

suite('fetch one:many', () => {
  const base = {
    columns: [
      {id: 'string' as const},
      {id: 'string' as const, issueID: 'string' as const},
    ],
    primaryKeys: [['id'], ['id']],
    joins: [
      {
        parentKey: 'id',
        childKey: 'issueID',
        relationshipName: 'comments',
      },
    ],
  };

  fetchTest({
    ...base,
    name: 'no data',
    sources: [[], []],
    expectedMessages: [['0', 'fetch', {}]],
    expectedPrimaryKeySetStorageKeys: [[]],
    expectedHydrate: [],
  });

  fetchTest({
    ...base,
    name: 'no parent',
    sources: [[], [{id: 'c1', issueID: 'i1'}]],
    expectedMessages: [['0', 'fetch', {}]],
    expectedPrimaryKeySetStorageKeys: [[]],
    expectedHydrate: [],
  });

  fetchTest({
    ...base,
    name: 'parent, no children',
    sources: [[{id: 'i1'}], []],
    expectedMessages: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']]],
    expectedHydrate: [{row: {id: 'i1'}, relationships: {comments: []}}],
  });

  fetchTest({
    ...base,
    name: 'one parent, one child',
    sources: [[{id: 'i1'}], [{id: 'c1', issueID: 'i1'}]],
    expectedMessages: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']]],
    expectedHydrate: [
      {
        row: {id: 'i1'},
        relationships: {
          comments: [{row: {id: 'c1', issueID: 'i1'}, relationships: {}}],
        },
      },
    ],
  });

  // one parent, wrong child
  fetchTest({
    ...base,
    name: 'one parent, wrong child',
    sources: [[{id: 'i1'}], [{id: 'c1', issueID: 'i2'}]],
    expectedMessages: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']]],
    expectedHydrate: [{row: {id: 'i1'}, relationships: {comments: []}}],
  });

  fetchTest({
    ...base,
    name: 'one parent, one child + one wrong child',
    sources: [
      [{id: 'i1'}],
      [
        {id: 'c2', issueID: 'i2'},
        {id: 'c1', issueID: 'i1'},
      ],
    ],
    expectedMessages: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']]],
    expectedHydrate: [
      {
        row: {id: 'i1'},
        relationships: {
          comments: [{row: {id: 'c1', issueID: 'i1'}, relationships: {}}],
        },
      },
    ],
  });

  fetchTest({
    ...base,
    name: 'two parents, each with two children',
    sources: [
      [{id: 'i2'}, {id: 'i1'}],
      [
        {id: 'c4', issueID: 'i2'},
        {id: 'c3', issueID: 'i2'},
        {id: 'c2', issueID: 'i1'},
        {id: 'c1', issueID: 'i1'},
      ],
    ],
    expectedMessages: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i2'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [
      [
        ['i1', 'i1'],
        ['i2', 'i2'],
      ],
    ],
    expectedHydrate: [
      {
        row: {id: 'i1'},
        relationships: {
          comments: [
            {row: {id: 'c1', issueID: 'i1'}, relationships: {}},
            {row: {id: 'c2', issueID: 'i1'}, relationships: {}},
          ],
        },
      },
      {
        row: {id: 'i2'},
        relationships: {
          comments: [
            {row: {id: 'c3', issueID: 'i2'}, relationships: {}},
            {row: {id: 'c4', issueID: 'i2'}, relationships: {}},
          ],
        },
      },
    ],
  });
});

suite('fetch many:one', () => {
  const base = {
    columns: [
      {id: 'string' as const, ownerID: 'string' as const},
      {id: 'string' as const},
    ],
    primaryKeys: [['id'], ['id']],
    joins: [
      {
        parentKey: 'ownerID',
        childKey: 'id',
        relationshipName: 'owner',
      },
    ],
  };

  fetchTest({
    ...base,
    name: 'no data',
    sources: [[], []],
    expectedMessages: [['0', 'fetch', {}]],
    expectedPrimaryKeySetStorageKeys: [[]],
    expectedHydrate: [],
  });

  fetchTest({
    ...base,
    name: 'one parent, no child',
    sources: [[{id: 'i1', ownerID: 'u1'}], []],
    expectedMessages: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'id', value: 'u1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['u1', 'i1']]],
    expectedHydrate: [
      {row: {id: 'i1', ownerID: 'u1'}, relationships: {owner: []}},
    ],
  });

  fetchTest({
    ...base,
    name: 'no parent, one child',
    sources: [[], [{id: 'u1'}]],
    expectedMessages: [['0', 'fetch', {}]],
    expectedPrimaryKeySetStorageKeys: [[]],
    expectedHydrate: [],
  });

  fetchTest({
    ...base,
    name: 'one parent, one child',
    sources: [[{id: 'i1', ownerID: 'u1'}], [{id: 'u1'}]],
    expectedMessages: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'id', value: 'u1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['u1', 'i1']]],
    expectedHydrate: [
      {
        row: {id: 'i1', ownerID: 'u1'},
        relationships: {
          owner: [{row: {id: 'u1'}, relationships: {}}],
        },
      },
    ],
  });

  fetchTest({
    ...base,
    name: 'two parents, one child',
    sources: [
      [
        {id: 'i2', ownerID: 'u1'},
        {id: 'i1', ownerID: 'u1'},
      ],
      [{id: 'u1'}],
    ],
    expectedMessages: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'id', value: 'u1'}}],
      ['1', 'fetch', {constraint: {key: 'id', value: 'u1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [
      [
        ['u1', 'i1'],
        ['u1', 'i2'],
      ],
    ],
    expectedHydrate: [
      {
        row: {id: 'i1', ownerID: 'u1'},
        relationships: {
          owner: [{row: {id: 'u1'}, relationships: {}}],
        },
      },
      {
        row: {id: 'i2', ownerID: 'u1'},
        relationships: {
          owner: [{row: {id: 'u1'}, relationships: {}}],
        },
      },
    ],
  });

  fetchTest({
    ...base,
    name: 'two parents, two children',
    sources: [
      [
        {id: 'i2', ownerID: 'u2'},
        {id: 'i1', ownerID: 'u1'},
      ],
      [{id: 'u2'}, {id: 'u1'}],
    ],
    expectedMessages: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'id', value: 'u1'}}],
      ['1', 'fetch', {constraint: {key: 'id', value: 'u2'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [
      [
        ['u1', 'i1'],
        ['u2', 'i2'],
      ],
    ],
    expectedHydrate: [
      {
        row: {id: 'i1', ownerID: 'u1'},
        relationships: {
          owner: [{row: {id: 'u1'}, relationships: {}}],
        },
      },
      {
        row: {id: 'i2', ownerID: 'u2'},
        relationships: {
          owner: [{row: {id: 'u2'}, relationships: {}}],
        },
      },
    ],
  });
});

suite('fetch one:many:many', () => {
  const base = {
    columns: [
      {id: 'string' as const},
      {id: 'string' as const, issueID: 'string' as const},
      {id: 'string' as const, commentID: 'string' as const},
    ],
    primaryKeys: [['id'], ['id'], ['id']],
    joins: [
      {
        parentKey: 'id',
        childKey: 'issueID',
        relationshipName: 'comments',
      },
      {
        parentKey: 'id',
        childKey: 'commentID',
        relationshipName: 'revisions',
      },
    ],
  };

  fetchTest({
    ...base,
    name: 'no data',
    sources: [[], [], []],
    expectedMessages: [['0', 'fetch', {}]],
    expectedPrimaryKeySetStorageKeys: [[], []],
    expectedHydrate: [],
  });

  fetchTest({
    ...base,
    name: 'no parent, one comment, no revision',
    sources: [[], [{id: 'c1', issueID: 'i1'}], []],
    expectedMessages: [['0', 'fetch', {}]],
    expectedPrimaryKeySetStorageKeys: [[], []],
    expectedHydrate: [],
  });

  fetchTest({
    ...base,
    name: 'no parent, one comment, one revision',
    sources: [[], [{id: 'c1', issueID: 'i1'}], [{id: 'r1', commentID: 'c1'}]],
    expectedMessages: [['0', 'fetch', {}]],
    expectedPrimaryKeySetStorageKeys: [[], []],
    expectedHydrate: [],
  });

  fetchTest({
    ...base,
    name: 'one issue, no comments or revisions',
    sources: [[{id: 'i1'}], [], []],
    expectedMessages: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']], []],
    expectedHydrate: [{row: {id: 'i1'}, relationships: {comments: []}}],
  });

  fetchTest({
    ...base,
    name: 'one issue, one comment, one revision',
    sources: [
      [{id: 'i1'}],
      [{id: 'c1', issueID: 'i1'}],
      [{id: 'r1', commentID: 'c1'}],
    ],
    expectedMessages: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
      ['2', 'fetch', {constraint: {key: 'commentID', value: 'c1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']], [['c1', 'c1']]],
    expectedHydrate: [
      {
        row: {id: 'i1'},
        relationships: {
          comments: [
            {
              row: {id: 'c1', issueID: 'i1'},
              relationships: {
                revisions: [
                  {row: {id: 'r1', commentID: 'c1'}, relationships: {}},
                ],
              },
            },
          ],
        },
      },
    ],
  });

  fetchTest({
    ...base,
    name: 'two issues, four comments, eight revisions',
    sources: [
      [{id: 'i2'}, {id: 'i1'}],
      [
        {id: 'c4', issueID: 'i2'},
        {id: 'c3', issueID: 'i2'},
        {id: 'c2', issueID: 'i1'},
        {id: 'c1', issueID: 'i1'},
      ],
      [
        {id: 'r8', commentID: 'c4'},
        {id: 'r7', commentID: 'c4'},
        {id: 'r6', commentID: 'c3'},
        {id: 'r5', commentID: 'c3'},
        {id: 'r4', commentID: 'c2'},
        {id: 'r3', commentID: 'c2'},
        {id: 'r2', commentID: 'c1'},
        {id: 'r1', commentID: 'c1'},
      ],
    ],
    expectedMessages: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
      ['2', 'fetch', {constraint: {key: 'commentID', value: 'c1'}}],
      ['2', 'fetch', {constraint: {key: 'commentID', value: 'c2'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i2'}}],
      ['2', 'fetch', {constraint: {key: 'commentID', value: 'c3'}}],
      ['2', 'fetch', {constraint: {key: 'commentID', value: 'c4'}}],
    ],

    expectedPrimaryKeySetStorageKeys: [
      [
        ['i1', 'i1'],
        ['i2', 'i2'],
      ],
      [
        ['c1', 'c1'],
        ['c2', 'c2'],
        ['c3', 'c3'],
        ['c4', 'c4'],
      ],
    ],

    expectedHydrate: [
      {
        row: {id: 'i1'},
        relationships: {
          comments: [
            {
              row: {id: 'c1', issueID: 'i1'},
              relationships: {
                revisions: [
                  {row: {id: 'r1', commentID: 'c1'}, relationships: {}},
                  {row: {id: 'r2', commentID: 'c1'}, relationships: {}},
                ],
              },
            },
            {
              row: {id: 'c2', issueID: 'i1'},
              relationships: {
                revisions: [
                  {row: {id: 'r3', commentID: 'c2'}, relationships: {}},
                  {row: {id: 'r4', commentID: 'c2'}, relationships: {}},
                ],
              },
            },
          ],
        },
      },
      {
        row: {id: 'i2'},
        relationships: {
          comments: [
            {
              row: {id: 'c3', issueID: 'i2'},
              relationships: {
                revisions: [
                  {row: {id: 'r5', commentID: 'c3'}, relationships: {}},
                  {row: {id: 'r6', commentID: 'c3'}, relationships: {}},
                ],
              },
            },
            {
              row: {id: 'c4', issueID: 'i2'},
              relationships: {
                revisions: [
                  {row: {id: 'r7', commentID: 'c4'}, relationships: {}},
                  {row: {id: 'r8', commentID: 'c4'}, relationships: {}},
                ],
              },
            },
          ],
        },
      },
    ],
  });
});

suite('fetch one:many:one', () => {
  const base = {
    columns: [
      {id: 'string' as const},
      {issueID: 'string' as const, labelID: 'string' as const},
      {id: 'string' as const},
    ],
    primaryKeys: [['id'], ['issueID', 'labelID'], ['id']],
    joins: [
      {
        parentKey: 'id',
        childKey: 'issueID',
        relationshipName: 'issuelabels',
      },
      {
        parentKey: 'labelID',
        childKey: 'id',
        relationshipName: 'labels',
      },
    ],
  };

  const sorts = [
    undefined,
    [
      ['issueID', 'asc'],
      ['labelID', 'asc'],
    ] as const,
  ];

  fetchTest({
    ...base,
    name: 'no data',
    sources: [[], [], []],
    sorts,
    expectedMessages: [['0', 'fetch', {}]],
    expectedPrimaryKeySetStorageKeys: [[], []],
    expectedHydrate: [],
  });

  fetchTest({
    ...base,
    name: 'no issues, one issuelabel, one label',
    sources: [[], [{issueID: 'i1', labelID: 'l1'}], [{id: 'l1'}]],
    sorts,
    expectedMessages: [['0', 'fetch', {}]],
    expectedPrimaryKeySetStorageKeys: [[], []],
    expectedHydrate: [],
  });

  fetchTest({
    ...base,
    name: 'one issue, no issuelabels, no labels',
    sources: [[{id: 'i1'}], [], []],
    sorts,
    expectedMessages: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']], []],
    expectedHydrate: [{row: {id: 'i1'}, relationships: {issuelabels: []}}],
  });

  fetchTest({
    ...base,
    name: 'one issue, one issuelabel, no labels',
    sources: [[{id: 'i1'}], [{issueID: 'i1', labelID: 'l1'}], []],
    sorts,
    expectedMessages: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
      ['2', 'fetch', {constraint: {key: 'id', value: 'l1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']], [['l1', 'i1', 'l1']]],
    expectedHydrate: [
      {
        row: {id: 'i1'},
        relationships: {
          issuelabels: [
            {
              row: {issueID: 'i1', labelID: 'l1'},
              relationships: {labels: []},
            },
          ],
        },
      },
    ],
  });

  fetchTest({
    ...base,
    name: 'one issue, one issuelabel, one label',
    sources: [[{id: 'i1'}], [{issueID: 'i1', labelID: 'l1'}], [{id: 'l1'}]],
    sorts,
    expectedMessages: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
      ['2', 'fetch', {constraint: {key: 'id', value: 'l1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']], [['l1', 'i1', 'l1']]],
    expectedHydrate: [
      {
        row: {id: 'i1'},
        relationships: {
          issuelabels: [
            {
              row: {issueID: 'i1', labelID: 'l1'},
              relationships: {
                labels: [{row: {id: 'l1'}, relationships: {}}],
              },
            },
          ],
        },
      },
    ],
  });

  fetchTest({
    ...base,
    name: 'one issue, two issuelabels, two labels',
    sources: [
      [{id: 'i1'}],
      [
        {issueID: 'i1', labelID: 'l1'},
        {issueID: 'i1', labelID: 'l2'},
      ],
      [{id: 'l1'}, {id: 'l2'}],
    ],
    sorts,
    expectedMessages: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
      ['2', 'fetch', {constraint: {key: 'id', value: 'l1'}}],
      ['2', 'fetch', {constraint: {key: 'id', value: 'l2'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [
      [['i1', 'i1']],
      [
        ['l1', 'i1', 'l1'],
        ['l2', 'i1', 'l2'],
      ],
    ],
    expectedHydrate: [
      {
        row: {id: 'i1'},
        relationships: {
          issuelabels: [
            {
              row: {issueID: 'i1', labelID: 'l1'},
              relationships: {
                labels: [{row: {id: 'l1'}, relationships: {}}],
              },
            },
            {
              row: {issueID: 'i1', labelID: 'l2'},
              relationships: {
                labels: [{row: {id: 'l2'}, relationships: {}}],
              },
            },
          ],
        },
      },
    ],
  });

  fetchTest({
    ...base,
    name: 'two issues, four issuelabels, two labels',
    sources: [
      [{id: 'i2'}, {id: 'i1'}],
      [
        {issueID: 'i2', labelID: 'l2'},
        {issueID: 'i2', labelID: 'l1'},
        {issueID: 'i1', labelID: 'l2'},
        {issueID: 'i1', labelID: 'l1'},
      ],
      [{id: 'l1'}, {id: 'l2'}],
    ],
    sorts,
    expectedMessages: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
      ['2', 'fetch', {constraint: {key: 'id', value: 'l1'}}],
      ['2', 'fetch', {constraint: {key: 'id', value: 'l2'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i2'}}],
      ['2', 'fetch', {constraint: {key: 'id', value: 'l1'}}],
      ['2', 'fetch', {constraint: {key: 'id', value: 'l2'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [
      [
        ['i1', 'i1'],
        ['i2', 'i2'],
      ],
      [
        ['l1', 'i1', 'l1'],
        ['l1', 'i2', 'l1'],
        ['l2', 'i1', 'l2'],
        ['l2', 'i2', 'l2'],
      ],
    ],
    expectedHydrate: [
      {
        row: {id: 'i1'},
        relationships: {
          issuelabels: [
            {
              row: {issueID: 'i1', labelID: 'l1'},
              relationships: {labels: [{row: {id: 'l1'}, relationships: {}}]},
            },
            {
              row: {issueID: 'i1', labelID: 'l2'},
              relationships: {labels: [{row: {id: 'l2'}, relationships: {}}]},
            },
          ],
        },
      },
      {
        row: {id: 'i2'},
        relationships: {
          issuelabels: [
            {
              row: {issueID: 'i2', labelID: 'l1'},
              relationships: {labels: [{row: {id: 'l1'}, relationships: {}}]},
            },
            {
              row: {issueID: 'i2', labelID: 'l2'},
              relationships: {labels: [{row: {id: 'l2'}, relationships: {}}]},
            },
          ],
        },
      },
    ],
  });
});

// Despite the name, this test runs the join through all three phases:
// initial fetch, fetch, and cleanup.
function fetchTest(t: FetchTest) {
  test(t.name, () => {
    assert(t.sources.length > 0);
    assert(t.joins.length === t.sources.length - 1);

    const log: SnitchMessage[] = [];

    const sources = t.sources.map((rows, i) => {
      const ordering = t.sorts?.[i] ?? [['id', 'asc']];
      const source = new MemorySource(`t${i}`, t.columns[i], t.primaryKeys[i]);
      for (const row of rows) {
        source.push({type: 'add', row});
      }
      const snitch = new Snitch(source.connect(ordering), String(i), log);
      return {
        source,
        snitch,
      };
    });

    const joins: {
      join: Join;
      storage: MemoryStorage;
    }[] = [];
    // Although we tend to think of the joins from left to right, we need to
    // build them from right to left.
    for (let i = t.joins.length - 1; i >= 0; i--) {
      const info = t.joins[i];
      const parent = sources[i].snitch;
      const child =
        i === t.joins.length - 1 ? sources[i + 1].snitch : joins[i + 1].join;
      const storage = new MemoryStorage();
      const join = new Join(
        parent,
        child,
        storage,
        info.parentKey,
        info.childKey,
        info.relationshipName,
      );
      joins[i] = {
        join,
        storage,
      };
    }

    for (const fetchType of ['fetch', 'fetch', 'cleanup'] as const) {
      log.length = 0;

      // By convention we put them in the test bottom up. Why? Easier to think
      // left-to-right.
      const finalJoin = joins[0];

      let expectedSchema: Schema | undefined;
      for (let i = sources.length - 1; i >= 0; i--) {
        const schema = sources[i].snitch.getSchema();
        if (expectedSchema) {
          expectedSchema = {
            ...schema,
            relationships: {[t.joins[i].relationshipName]: expectedSchema},
          };
        } else {
          expectedSchema = schema;
        }
      }

      // toEqual doesn't work here for some reason that I am too lazy to find.
      expect(finalJoin.join.getSchema()).toStrictEqual(expectedSchema);

      const c = new Catch(finalJoin.join);
      const r = c[fetchType]();

      expect(r).toEqual(t.expectedHydrate);
      expect(c.pushes).toEqual([]);

      for (const [i, j] of joins.entries()) {
        const {storage} = j;
        if (fetchType === 'fetch') {
          const expectedPrimaryKeySetStorageKeys =
            t.expectedPrimaryKeySetStorageKeys[i];
          const expectedStorage: Record<string, boolean> = {};
          for (const k of expectedPrimaryKeySetStorageKeys) {
            expectedStorage[createPrimaryKeySetStorageKey(k)] = true;
          }
          expect(storage.cloneData()).toEqual(expectedStorage);
        } else {
          fetchType satisfies 'cleanup';
          expect(storage.cloneData()).toEqual({});
        }
      }

      let expectedMessages = t.expectedMessages as Exclude<
        SnitchMessage,
        PushMessage
      >[];
      if (fetchType === 'fetch') {
        expectedMessages = expectedMessages.map(([name, _, arg]) => [
          name,
          'fetch',
          arg,
        ]);
      } else if (fetchType === 'cleanup') {
        // For cleanup, the last fetch for any constraint should be a cleanup.
        // Others should be fetch.
        const seen = new Set();
        for (let i = expectedMessages.length - 1; i >= 0; i--) {
          const [name, _, req] = expectedMessages[i];
          if (!seen.has(req.constraint?.value)) {
            expectedMessages[i] = [name, 'cleanup', req];
          } else {
            expectedMessages[i] = [name, 'fetch', req];
          }
          seen.add(req.constraint?.value);
        }
      }
      expect(log).toEqual(expectedMessages);
    }
  });
}

type FetchTest = {
  name: string;
  columns: Record<string, ValueType>[];
  primaryKeys: readonly string[][];
  sources: Row[][];
  sorts?: (Ordering | undefined)[] | undefined;
  joins: {
    parentKey: string;
    childKey: string;
    relationshipName: string;
  }[];
  expectedMessages: SnitchMessage[];
  expectedPrimaryKeySetStorageKeys: NormalizedValue[][][];
  expectedHydrate: Node[];
};

test('createPrimaryKeySetStorageKey', () => {
  const k123 = createPrimaryKeySetStorageKey([123, 'id1']);
  const k1234 = createPrimaryKeySetStorageKey([1234, 'id1']);

  expect(k123.startsWith(createPrimaryKeySetStorageKeyPrefix(123))).true;
  expect(k123.startsWith(createPrimaryKeySetStorageKeyPrefix(124))).false;

  expect(k1234.startsWith(createPrimaryKeySetStorageKeyPrefix(123))).false;
  expect(k1234.startsWith(createPrimaryKeySetStorageKeyPrefix(1234))).true;
});
