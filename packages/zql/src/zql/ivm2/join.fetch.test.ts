import {expect, test} from 'vitest';
import {Join} from './join.js';
import {MemorySource} from './memory-source.js';
import {MemoryStorage} from './memory-storage.js';
import {PushMessage, Snitch, SnitchMessage} from './snitch.js';
import type {Row, Node} from './data.js';
import {assert} from 'shared/src/asserts.js';
import type {Ordering} from '../ast2/ast.js';
import {Catch} from './catch.js';
import { ValueType } from './schema.js';
test('hydrate one:many', () => {
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

  // no data
  fetchTest({
    ...base,
    sources: [[], []],
    expectedMessages: [['0', 'hydrate', {}]],
    expectedStorageCounts: [{}],
    expectedHydrate: [],
  });

  // no parent
  fetchTest({
    ...base,
    sources: [[], [{id: 'c1', issueID: 'i1'}]],
    expectedMessages: [['0', 'hydrate', {}]],
    expectedStorageCounts: [{}],
    expectedHydrate: [],
  });

  // parent, no children
  fetchTest({
    ...base,
    sources: [[{id: 'i1'}], []],
    expectedMessages: [
      ['0', 'hydrate', {}],
      ['1', 'hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
    expectedStorageCounts: [{i1: 1}],
    expectedHydrate: [{row: {id: 'i1'}, relationships: {comments: []}}],
  });

  // one parent, one child
  fetchTest({
    ...base,
    sources: [[{id: 'i1'}], [{id: 'c1', issueID: 'i1'}]],
    expectedMessages: [
      ['0', 'hydrate', {}],
      ['1', 'hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
    expectedStorageCounts: [{i1: 1}],
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
    sources: [[{id: 'i1'}], [{id: 'c1', issueID: 'i2'}]],
    expectedMessages: [
      ['0', 'hydrate', {}],
      ['1', 'hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
    expectedStorageCounts: [{i1: 1}],
    expectedHydrate: [{row: {id: 'i1'}, relationships: {comments: []}}],
  });

  // one parent, one child + one wrong child
  fetchTest({
    ...base,
    sources: [
      [{id: 'i1'}],
      [
        {id: 'c2', issueID: 'i2'},
        {id: 'c1', issueID: 'i1'},
      ],
    ],
    expectedMessages: [
      ['0', 'hydrate', {}],
      ['1', 'hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
    expectedStorageCounts: [{i1: 1}],
    expectedHydrate: [
      {
        row: {id: 'i1'},
        relationships: {
          comments: [{row: {id: 'c1', issueID: 'i1'}, relationships: {}}],
        },
      },
    ],
  });

  // two parents, each with two children
  fetchTest({
    ...base,
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
      ['0', 'hydrate', {}],
      ['1', 'hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ['1', 'hydrate', {constraint: {key: 'issueID', value: 'i2'}}],
    ],
    expectedStorageCounts: [{i1: 1, i2: 1}],
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

test('hydrate many:one', () => {
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

  // no data
  fetchTest({
    ...base,
    sources: [[], []],
    expectedMessages: [['0', 'hydrate', {}]],
    expectedStorageCounts: [{}],
    expectedHydrate: [],
  });

  // one parent, no child
  fetchTest({
    ...base,
    sources: [[{id: 'i1', ownerID: 'u1'}], []],
    expectedMessages: [
      ['0', 'hydrate', {}],
      ['1', 'hydrate', {constraint: {key: 'id', value: 'u1'}}],
    ],
    expectedStorageCounts: [{u1: 1}],
    expectedHydrate: [
      {row: {id: 'i1', ownerID: 'u1'}, relationships: {owner: []}},
    ],
  });

  // no parent, one child
  fetchTest({
    ...base,
    sources: [[], [{id: 'u1'}]],
    expectedMessages: [['0', 'hydrate', {}]],
    expectedStorageCounts: [{}],
    expectedHydrate: [],
  });

  // one parent, one child
  fetchTest({
    ...base,
    sources: [[{id: 'i1', ownerID: 'u1'}], [{id: 'u1'}]],
    expectedMessages: [
      ['0', 'hydrate', {}],
      ['1', 'hydrate', {constraint: {key: 'id', value: 'u1'}}],
    ],
    expectedStorageCounts: [{u1: 1}],
    expectedHydrate: [
      {
        row: {id: 'i1', ownerID: 'u1'},
        relationships: {
          owner: [{row: {id: 'u1'}, relationships: {}}],
        },
      },
    ],
  });

  // two parents, one child
  fetchTest({
    ...base,
    sources: [
      [
        {id: 'i2', ownerID: 'u1'},
        {id: 'i1', ownerID: 'u1'},
      ],
      [{id: 'u1'}],
    ],
    expectedMessages: [
      ['0', 'hydrate', {}],
      ['1', 'hydrate', {constraint: {key: 'id', value: 'u1'}}],
      ['1', 'fetch', {constraint: {key: 'id', value: 'u1'}}],
    ],
    expectedStorageCounts: [{u1: 2}],
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

  // two parents, two children
  fetchTest({
    ...base,
    sources: [
      [
        {id: 'i2', ownerID: 'u2'},
        {id: 'i1', ownerID: 'u1'},
      ],
      [{id: 'u2'}, {id: 'u1'}],
    ],
    expectedMessages: [
      ['0', 'hydrate', {}],
      ['1', 'hydrate', {constraint: {key: 'id', value: 'u1'}}],
      ['1', 'hydrate', {constraint: {key: 'id', value: 'u2'}}],
    ],
    expectedStorageCounts: [{u1: 1, u2: 1}],
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

test('hydrate one:many:many', () => {
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
    ]
  }

  // no data
  fetchTest({
    ...base,
    sources: [[], [], []],
    expectedMessages: [['0', 'hydrate', {}]],
    expectedStorageCounts: [{}, {}],
    expectedHydrate: [],
  });

  // no parent, one comment, no revision
  fetchTest({
    ...base,
    sources: [[], [{id: 'c1', issueID: 'i1'}], []],
    expectedMessages: [['0', 'hydrate', {}]],
    expectedStorageCounts: [{}, {}],
    expectedHydrate: [],
  });

  // no parent, one comment, one revision
  fetchTest({
    ...base,
    sources: [[], [{id: 'c1', issueID: 'i1'}], [{id: 'r1', commentID: 'c1'}]],
    expectedMessages: [['0', 'hydrate', {}]],
    expectedStorageCounts: [{}, {}],
    expectedHydrate: [],
  });

  // one issue, no comments or revisions
  fetchTest({
    ...base,
    sources: [[{id: 'i1'}], [], []],
    expectedMessages: [
      ['0', 'hydrate', {}],
      ['1', 'hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
    expectedStorageCounts: [{i1: 1}, {}],
    expectedHydrate: [{row: {id: 'i1'}, relationships: {comments: []}}],
  });

  // one issue, one comment, one revision
  fetchTest({
    ...base,
    sources: [
      [{id: 'i1'}],
      [{id: 'c1', issueID: 'i1'}],
      [{id: 'r1', commentID: 'c1'}],
    ],
    expectedMessages: [
      ['0', 'hydrate', {}],
      ['1', 'hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ['2', 'hydrate', {constraint: {key: 'commentID', value: 'c1'}}],
    ],
    expectedStorageCounts: [{i1: 1}, {c1: 1}],
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

  // two issues, four comments, eight revisions
  fetchTest({
    ...base,
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
      ['0', 'hydrate', {}],
      ['1', 'hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ['2', 'hydrate', {constraint: {key: 'commentID', value: 'c1'}}],
      ['2', 'hydrate', {constraint: {key: 'commentID', value: 'c2'}}],
      ['1', 'hydrate', {constraint: {key: 'issueID', value: 'i2'}}],
      ['2', 'hydrate', {constraint: {key: 'commentID', value: 'c3'}}],
      ['2', 'hydrate', {constraint: {key: 'commentID', value: 'c4'}}],
    ],
    expectedStorageCounts: [
      {i1: 1, i2: 1},
      {c1: 1, c2: 1, c3: 1, c4: 1},
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

test('hydrate one:many:one', () => {
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
  }

  const sorts = [
    undefined,
    [
      ['issueID', 'asc'],
      ['labelID', 'asc'],
    ] as const,
  ];

  // no data
  fetchTest({
    ...base,
    sources: [[], [], []],
    sorts,
    expectedMessages: [['0', 'hydrate', {}]],
    expectedStorageCounts: [{}, {}],
    expectedHydrate: [],
  });

  // no issues, one issuelabel, one label
  fetchTest({
    ...base,
    sources: [[], [{issueID: 'i1', labelID: 'l1'}], [{id: 'l1'}]],
    sorts,
    expectedMessages: [['0', 'hydrate', {}]],
    expectedStorageCounts: [{}, {}],
    expectedHydrate: [],
  });

  // one issue, no issuelabels, no labels
  fetchTest({
    ...base,
    sources: [[{id: 'i1'}], [], []],
    sorts,
    expectedMessages: [
      ['0', 'hydrate', {}],
      ['1', 'hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
    expectedStorageCounts: [{i1: 1}, {}],
    expectedHydrate: [{row: {id: 'i1'}, relationships: {issuelabels: []}}],
  });

  // one issue, one issuelabel, no labels
  fetchTest({
    ...base,
    sources: [[{id: 'i1'}], [{issueID: 'i1', labelID: 'l1'}], []],
    sorts,
    expectedMessages: [
      ['0', 'hydrate', {}],
      ['1', 'hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ['2', 'hydrate', {constraint: {key: 'id', value: 'l1'}}],
    ],
    expectedStorageCounts: [{i1: 1}, {l1: 1}],
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

  // one issue, one issuelabel, one label
  fetchTest({
    ...base,
    sources: [[{id: 'i1'}], [{issueID: 'i1', labelID: 'l1'}], [{id: 'l1'}]],
    sorts,
    expectedMessages: [
      ['0', 'hydrate', {}],
      ['1', 'hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ['2', 'hydrate', {constraint: {key: 'id', value: 'l1'}}],
    ],
    expectedStorageCounts: [{i1: 1}, {l1: 1}],
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

  // one issue, two issuelabels, two labels
  fetchTest({
    ...base,
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
      ['0', 'hydrate', {}],
      ['1', 'hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ['2', 'hydrate', {constraint: {key: 'id', value: 'l1'}}],
      ['2', 'hydrate', {constraint: {key: 'id', value: 'l2'}}],
    ],
    expectedStorageCounts: [{i1: 1}, {l1: 1, l2: 1}],
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

  // two issues, four issuelabels, two labels
  fetchTest({
    ...base,
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
      ['0', 'hydrate', {}],
      ['1', 'hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ['2', 'hydrate', {constraint: {key: 'id', value: 'l1'}}],
      ['2', 'hydrate', {constraint: {key: 'id', value: 'l2'}}],
      ['1', 'hydrate', {constraint: {key: 'issueID', value: 'i2'}}],
      ['2', 'fetch', {constraint: {key: 'id', value: 'l1'}}],
      ['2', 'fetch', {constraint: {key: 'id', value: 'l2'}}],
    ],
    expectedStorageCounts: [
      {i1: 1, i2: 1},
      {l1: 2, l2: 2},
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

// Despite the name, this test runs the join through all three phases: hydrate,
// fetch, and dehydrate.
function fetchTest(t: FetchTest) {
  assert(t.sources.length > 0);
  assert(t.joins.length === t.sources.length - 1);

  const log: SnitchMessage[] = [];

  const sources = t.sources.map((rows, i) => {
    const ordering = t.sorts?.[i] ?? [['id', 'asc']];
    const source = new MemorySource(t.columns[i], t.primaryKeys[i]);
    for (const row of rows) {
      source.push({type: 'add', row});
    }
    const snitch = new Snitch(source, String(i), log);
    source.addOutput(snitch, ordering);
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
    parent.setOutput(join);
    child.setOutput(join);
    joins[i] = {
      join,
      storage,
    };
  }

  for (const fetchType of ['hydrate', 'fetch', 'dehydrate'] as const) {
    log.length = 0;

    // By convention we put them in the test bottom up. Why? Easier to think
    // left-to-right.
    const finalJoin = joins[0];
    const c = new Catch(finalJoin.join);
    finalJoin.join.setOutput(c);

    const r = c[fetchType]();

    expect(r).toEqual(t.expectedHydrate);
    expect(c.pushes).toEqual([]);

    for (const [i, j] of joins.entries()) {
      const {storage} = j;
      const expectedCounts = t.expectedStorageCounts[i];
      if (fetchType === 'hydrate' || fetchType === 'fetch') {
        expect(storage.cloneData()).toEqual(
          Object.fromEntries(
            Object.entries(expectedCounts).map(([k, v]) => [
              JSON.stringify(['hydrate-count', k]),
              v,
            ]),
          ),
        );
      } else {
        fetchType satisfies 'dehydrate';
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
    } else if (fetchType === 'dehydrate') {
      // For dehydrate, the last fetch for any constraint should be a dehydrate.
      // Others should be fetch.
      const seen = new Set();
      for (let i = expectedMessages.length - 1; i >= 0; i--) {
        const [name, _, req] = expectedMessages[i];
        if (!seen.has(req.constraint?.value)) {
          expectedMessages[i] = [name, 'dehydrate', req];
        } else {
          expectedMessages[i] = [name, 'fetch', req];
        }
        seen.add(req.constraint?.value);
      }
    }
    expect(log).toEqual(expectedMessages);
  }
}

type FetchTest = {
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
  expectedStorageCounts: Record<string, number>[];
  expectedHydrate: Node[];
};
