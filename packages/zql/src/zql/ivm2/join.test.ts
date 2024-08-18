import {expect, test} from 'vitest';
import {Join} from './join.js';
import {MemorySource, SourceChange} from './memory-source.js';
import {MemoryStorage} from './memory-storage.js';
import {Snarf, expandNode} from './snarf.js';
import type {Row, Node} from './data.js';
import type {FetchRequest, HydrateRequest, Output, Source} from './operator.js';
import {assert} from 'shared/src/asserts.js';
import type {Ordering} from '../ast2/ast.js';

// Despite the name, this test runs the join through all three phases: hydrate,
// fetch, and dehydrate.
function fetchTest(t: FetchTest) {
  assert(t.sources.length > 0);
  assert(t.joins.length === t.sources.length - 1);

  const sources = t.sources.map(info => {
    const ordering = info.sort ?? [['id', 'asc']];
    const source = new SpySource(new MemorySource(ordering));
    for (const row of info.rows) {
      source.push({type: 'add', row});
    }
    return source;
  });

  const joins: {
    join: Join;
    storage: MemoryStorage;
    snarf: Snarf;
  }[] = [];
  const snarfs: Snarf[] = [];

  // Although we tend to think of the joins from left to right, we need to
  // build them from right to left.
  for (let i = t.joins.length - 1; i >= 0; i--) {
    const info = t.joins[i];
    const parent = sources[i];
    const child = i === t.joins.length - 1 ? sources[i + 1] : joins[i + 1].join;
    const storage = new MemoryStorage();
    const join = new Join(
      parent,
      child,
      storage,
      info.parentKey,
      info.childKey,
      info.relationshipName,
    );
    const snarf = new Snarf();
    join.setOutput(snarf);
    joins[i] = {
      join,
      storage,
      snarf,
    };
  }

  for (const fetchType of ['hydrate', 'fetch', 'dehydrate'] as const) {
    for (const s of sources) {
      s.calls.length = 0;
    }
    for (const sn of snarfs) {
      sn.changes.length = 0;
    }

    // By convention we put them in the test bottom up. Why? Easier to think
    // left-to-right.
    const finalJoin = joins[0];
    const r = [...finalJoin.join[fetchType]({}, finalJoin.snarf)].map(
      expandNode,
    );

    expect(r).toEqual(t.expectedHydrate);
    expect(finalJoin.snarf.changes).toEqual([]);

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

    for (const [i, s] of sources.entries()) {
      let {expectedCalls} = t.sources[i];
      if (fetchType === 'fetch') {
        expectedCalls = expectedCalls.map(([_, ...args]) => ['fetch', ...args]);
      } else if (fetchType === 'dehydrate') {
        // For dehydrate, the last fetch for any constraint should be a dehydrate.
        // Others should be fetch.
        const seen = new Set();
        for (let i = expectedCalls.length - 1; i >= 0; i--) {
          const [_, req] = expectedCalls[i];
          if (!seen.has(req.constraint?.value)) {
            expectedCalls[i] = ['dehydrate', req];
          } else {
            expectedCalls[i] = ['fetch', req];
          }
          seen.add(req.constraint?.value);
        }
      }
      expect(s.calls).toEqual(expectedCalls);
    }
  }
}

type FetchTest = {
  sources: {
    rows: Row[];
    sort?: Ordering | undefined;
    expectedCalls: [string, FetchRequest][];
  }[];
  joins: {
    parentKey: string;
    childKey: string;
    relationshipName: string;
  }[];
  expectedStorageCounts: Record<string, number>[];
  expectedHydrate: Node[];
};

test('hydrate one:many', () => {
  const joins = [
    {
      parentKey: 'id',
      childKey: 'issueID',
      relationshipName: 'comments',
    },
  ];

  // no data
  fetchTest({
    joins,
    sources: [
      {
        rows: [],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [],
        expectedCalls: [],
      },
    ],
    expectedStorageCounts: [{}],
    expectedHydrate: [],
  });

  // no parent
  fetchTest({
    joins,
    sources: [
      {
        rows: [],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [{id: 'c1', issueID: 'i1'}],
        expectedCalls: [],
      },
    ],
    expectedStorageCounts: [{}],
    expectedHydrate: [],
  });

  // parent, no children
  fetchTest({
    joins,
    sources: [
      {
        rows: [{id: 'i1'}],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
        ],
      },
    ],
    expectedStorageCounts: [{i1: 1}],
    expectedHydrate: [{row: {id: 'i1'}, relationships: {comments: []}}],
  });

  // one parent, one child
  fetchTest({
    joins,
    sources: [
      {
        rows: [{id: 'i1'}],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [{id: 'c1', issueID: 'i1'}],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
        ],
      },
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
    joins,
    sources: [
      {
        rows: [{id: 'i1'}],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [{id: 'c1', issueID: 'i2'}],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
        ],
      },
    ],
    expectedStorageCounts: [{i1: 1}],
    expectedHydrate: [{row: {id: 'i1'}, relationships: {comments: []}}],
  });

  // one parent, one child + one wrong child
  fetchTest({
    joins,
    sources: [
      {
        rows: [{id: 'i1'}],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [
          {id: 'c2', issueID: 'i2'},
          {id: 'c1', issueID: 'i1'},
        ],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
        ],
      },
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
    joins,
    sources: [
      {
        rows: [{id: 'i2'}, {id: 'i1'}],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [
          {id: 'c4', issueID: 'i2'},
          {id: 'c3', issueID: 'i2'},
          {id: 'c2', issueID: 'i1'},
          {id: 'c1', issueID: 'i1'},
        ],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
          ['hydrate', {constraint: {key: 'issueID', value: 'i2'}}],
        ],
      },
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
  const joins = [
    {
      parentKey: 'ownerID',
      childKey: 'id',
      relationshipName: 'owner',
    },
  ];

  // no data
  fetchTest({
    joins,
    sources: [
      {
        rows: [],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [],
        expectedCalls: [],
      },
    ],
    expectedStorageCounts: [{}],
    expectedHydrate: [],
  });

  // one parent, no child
  fetchTest({
    joins,
    sources: [
      {
        rows: [{id: 'i1', ownerID: 'u1'}],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [],
        expectedCalls: [['hydrate', {constraint: {key: 'id', value: 'u1'}}]],
      },
    ],
    expectedStorageCounts: [{u1: 1}],
    expectedHydrate: [
      {row: {id: 'i1', ownerID: 'u1'}, relationships: {owner: []}},
    ],
  });

  // no parent, one child
  fetchTest({
    joins,
    sources: [
      {
        rows: [],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [{id: 'u1'}],
        expectedCalls: [],
      },
    ],
    expectedStorageCounts: [{}],
    expectedHydrate: [],
  });

  // one parent, one child
  fetchTest({
    joins,
    sources: [
      {
        rows: [{id: 'i1', ownerID: 'u1'}],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [{id: 'u1'}],
        expectedCalls: [['hydrate', {constraint: {key: 'id', value: 'u1'}}]],
      },
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
    joins,
    sources: [
      {
        rows: [
          {id: 'i2', ownerID: 'u1'},
          {id: 'i1', ownerID: 'u1'},
        ],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [{id: 'u1'}],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'id', value: 'u1'}}],
          ['fetch', {constraint: {key: 'id', value: 'u1'}}],
        ],
      },
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
    joins,
    sources: [
      {
        rows: [
          {id: 'i2', ownerID: 'u2'},
          {id: 'i1', ownerID: 'u1'},
        ],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [{id: 'u2'}, {id: 'u1'}],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'id', value: 'u1'}}],
          ['hydrate', {constraint: {key: 'id', value: 'u2'}}],
        ],
      },
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
  const joins = [
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
  ];

  // no data
  fetchTest({
    joins,
    sources: [
      {
        rows: [],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [],
        expectedCalls: [],
      },
      {
        rows: [],
        expectedCalls: [],
      },
    ],
    expectedStorageCounts: [{}, {}],
    expectedHydrate: [],
  });

  // no parent, one comment, no revision
  fetchTest({
    joins,
    sources: [
      {
        rows: [],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [{id: 'c1', issueID: 'i1'}],
        expectedCalls: [],
      },
      {
        rows: [],
        expectedCalls: [],
      },
    ],
    expectedStorageCounts: [{}, {}],
    expectedHydrate: [],
  });

  // no parent, one comment, one revision
  fetchTest({
    joins,
    sources: [
      {
        rows: [],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [{id: 'c1', issueID: 'i1'}],
        expectedCalls: [],
      },
      {
        rows: [{id: 'r1', commentID: 'c1'}],
        expectedCalls: [],
      },
    ],
    expectedStorageCounts: [{}, {}],
    expectedHydrate: [],
  });

  // one issue, no comments or revisions
  fetchTest({
    joins,
    sources: [
      {
        rows: [{id: 'i1'}],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
        ],
      },
      {
        rows: [],
        expectedCalls: [],
      },
    ],
    expectedStorageCounts: [{i1: 1}, {}],
    expectedHydrate: [{row: {id: 'i1'}, relationships: {comments: []}}],
  });

  // one issue, one comment, one revision
  fetchTest({
    joins,
    sources: [
      {
        rows: [{id: 'i1'}],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [{id: 'c1', issueID: 'i1'}],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
        ],
      },
      {
        rows: [{id: 'r1', commentID: 'c1'}],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'commentID', value: 'c1'}}],
        ],
      },
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
    joins,
    sources: [
      {
        rows: [{id: 'i2'}, {id: 'i1'}],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [
          {id: 'c4', issueID: 'i2'},
          {id: 'c3', issueID: 'i2'},
          {id: 'c2', issueID: 'i1'},
          {id: 'c1', issueID: 'i1'},
        ],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
          ['hydrate', {constraint: {key: 'issueID', value: 'i2'}}],
        ],
      },
      {
        rows: [
          {id: 'r8', commentID: 'c4'},
          {id: 'r7', commentID: 'c4'},
          {id: 'r6', commentID: 'c3'},
          {id: 'r5', commentID: 'c3'},
          {id: 'r4', commentID: 'c2'},
          {id: 'r3', commentID: 'c2'},
          {id: 'r2', commentID: 'c1'},
          {id: 'r1', commentID: 'c1'},
        ],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'commentID', value: 'c1'}}],
          ['hydrate', {constraint: {key: 'commentID', value: 'c2'}}],
          ['hydrate', {constraint: {key: 'commentID', value: 'c3'}}],
          ['hydrate', {constraint: {key: 'commentID', value: 'c4'}}],
        ],
      },
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
  const joins = [
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
  ];

  // no data
  fetchTest({
    joins,
    sources: [
      {
        rows: [],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [],
        expectedCalls: [],
      },
      {
        rows: [],
        expectedCalls: [],
      },
    ],
    expectedStorageCounts: [{}, {}],
    expectedHydrate: [],
  });

  // no issues, one issuelabel, one label
  fetchTest({
    joins,
    sources: [
      {
        rows: [],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [{issueID: 'i1', labelID: 'l1'}],
        expectedCalls: [],
      },
      {
        rows: [{id: 'l1'}],
        expectedCalls: [],
      },
    ],
    expectedStorageCounts: [{}, {}],
    expectedHydrate: [],
  });

  // one issue, no issuelabels, no labels
  fetchTest({
    joins,
    sources: [
      {
        rows: [{id: 'i1'}],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
        ],
      },
      {
        rows: [],
        expectedCalls: [],
      },
    ],
    expectedStorageCounts: [{i1: 1}, {}],
    expectedHydrate: [{row: {id: 'i1'}, relationships: {issuelabels: []}}],
  });

  // one issue, one issuelabel, no labels
  fetchTest({
    joins,
    sources: [
      {
        rows: [{id: 'i1'}],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [{issueID: 'i1', labelID: 'l1'}],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
        ],
      },
      {
        rows: [],
        expectedCalls: [['hydrate', {constraint: {key: 'id', value: 'l1'}}]],
      },
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
    joins,
    sources: [
      {
        rows: [{id: 'i1'}],
        expectedCalls: [['hydrate', {}]],
      },
      {
        rows: [{issueID: 'i1', labelID: 'l1'}],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
        ],
      },
      {
        rows: [{id: 'l1'}],
        expectedCalls: [['hydrate', {constraint: {key: 'id', value: 'l1'}}]],
      },
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
    joins,
    sources: [
      {
        rows: [{id: 'i1'}],
        expectedCalls: [['hydrate', {}]],
      },
      {
        sort: [
          ['issueID', 'asc'],
          ['labelID', 'asc'],
        ],
        rows: [
          {issueID: 'i1', labelID: 'l1'},
          {issueID: 'i1', labelID: 'l2'},
        ],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
        ],
      },
      {
        rows: [{id: 'l1'}, {id: 'l2'}],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'id', value: 'l1'}}],
          ['hydrate', {constraint: {key: 'id', value: 'l2'}}],
        ],
      },
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
    joins,
    sources: [
      {
        rows: [{id: 'i2'}, {id: 'i1'}],
        expectedCalls: [['hydrate', {}]],
      },
      {
        sort: [
          ['issueID', 'asc'],
          ['labelID', 'asc'],
        ],
        rows: [
          {issueID: 'i2', labelID: 'l2'},
          {issueID: 'i2', labelID: 'l1'},
          {issueID: 'i1', labelID: 'l2'},
          {issueID: 'i1', labelID: 'l1'},
        ],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
          ['hydrate', {constraint: {key: 'issueID', value: 'i2'}}],
        ],
      },
      {
        rows: [{id: 'l1'}, {id: 'l2'}],
        expectedCalls: [
          ['hydrate', {constraint: {key: 'id', value: 'l1'}}],
          ['hydrate', {constraint: {key: 'id', value: 'l2'}}],
          ['fetch', {constraint: {key: 'id', value: 'l1'}}],
          ['fetch', {constraint: {key: 'id', value: 'l2'}}],
        ],
      },
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

// Using a custom spy instead of vi.spyOn because I want to see the order of
// calls to various methods.
class SpySource implements Source {
  #spied: MemorySource;

  readonly calls: unknown[];

  constructor(spied: MemorySource) {
    this.#spied = spied;
    this.calls = [];
  }

  get schema() {
    return this.#spied.schema;
  }

  addOutput(output: Output) {
    this.#spied.addOutput(output);
  }

  hydrate(req: HydrateRequest, output: Output) {
    this.calls.push(['hydrate', req]);
    return this.#spied.hydrate(req, output);
  }

  fetch(req: HydrateRequest, output: Output) {
    this.calls.push(['fetch', req]);
    return this.#spied.fetch(req, output);
  }

  dehydrate(req: HydrateRequest, output: Output) {
    this.calls.push(['dehydrate', req]);
    return this.#spied.dehydrate(req, output);
  }

  push(change: SourceChange) {
    this.calls.push(change);
    this.#spied.push(change);
  }
}

// TODO: push
