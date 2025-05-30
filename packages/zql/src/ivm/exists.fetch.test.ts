import {expect, suite, test} from 'vitest';
import {unreachable} from '../../../shared/src/asserts.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import type {Row} from './data.ts';
import type {Ordering, PrimaryKey} from '../../../zql/src/ivm/constraint.ts';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.ts';
import {Catch, type CaughtNode} from './catch.ts';
import {Exists} from './exists.ts';
import {Join} from './join.ts';
import {MemoryStorage} from './memory-storage.ts';
import {Snitch, type SnitchMessage} from './snitch.ts';
import {createSource} from './test/source-factory.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {buildFilterPipeline} from './filter-operators.ts';

const base = {
  columns: [
    {id: {type: 'string'}},
    {id: {type: 'string'}, issueID: {type: 'string'}},
  ],
  primaryKeys: [['id'], ['id']],
  join: {
    parentKey: ['id'],
    childKey: ['issueID'],
    relationshipName: 'comments',
  },
} as const;

const lc = createSilentLogContext();

const oneParentWithChildTest: FetchTest = {
  ...base,
  existsType: 'EXISTS',
  sources: [[{id: 'i1'}], [{id: 'c1', issueID: 'i1'}]],
};

const oneParentNoChildTest: FetchTest = {
  ...base,
  sources: [[{id: 'i1'}], []],
  existsType: 'EXISTS',
};

const threeParentsTwoWithChildrenTest: FetchTest = {
  ...base,
  sources: [
    [{id: 'i1'}, {id: 'i2'}, {id: 'i3'}],
    [
      {id: 'c1', issueID: 'i1'},
      {id: 'c2', issueID: 'i3'},
    ],
  ],
  existsType: 'EXISTS',
};

const threeParentsNoChildrenTest: FetchTest = {
  ...base,
  sources: [[{id: 'i1'}, {id: 'i2'}, {id: 'i3'}], []],
  existsType: 'EXISTS',
};

suite('EXISTS', () => {
  test('one parent with child', () => {
    const {messages, storage, hydrate} = fetchTest(oneParentWithChildTest);
    expect(messages).toMatchInlineSnapshot(`
      {
        "cleanup": [
          [
            "0",
            "cleanup",
            {},
          ],
          [
            "1",
            "cleanup",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
        ],
        "fetch": [
          [
            "0",
            "fetch",
            {},
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
        ],
        "initialFetch": [
          [
            "0",
            "fetch",
            {},
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
      }
    `);
    expect(hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "comments": [
              {
                "relationships": {},
                "row": {
                  "id": "c1",
                  "issueID": "i1",
                },
              },
            ],
          },
          "row": {
            "id": "i1",
          },
        },
      ]
    `);
  });

  test('one parent with child - reversed', () => {
    const {messages, storage, hydrate} = fetchTest(
      oneParentWithChildTest,
      true,
    );
    expect(messages).toMatchInlineSnapshot(`
      {
        "cleanup": [
          [
            "1",
            "cleanup",
            {},
          ],
          [
            "0",
            "cleanup",
            {
              "constraint": {
                "id": "i1",
              },
            },
          ],
        ],
        "fetch": [
          [
            "1",
            "fetch",
            {},
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i1",
              },
            },
          ],
        ],
        "initialFetch": [
          [
            "1",
            "fetch",
            {},
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i1",
              },
            },
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i1",
              },
            },
          ],
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`
      {
        "row/["i1"]/["c1"]": 1,
      }
    `);
    expect(hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "from_comments": [
              {
                "relationships": {},
                "row": {
                  "id": "i1",
                },
              },
            ],
          },
          "row": {
            "id": "c1",
            "issueID": "i1",
          },
        },
      ]
    `);
  });

  test('one parent no child', () => {
    const {messages, storage, hydrate} = fetchTest(oneParentNoChildTest);
    expect(messages).toMatchInlineSnapshot(`
      {
        "cleanup": [
          [
            "0",
            "cleanup",
            {},
          ],
          [
            "1",
            "cleanup",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
        ],
        "fetch": [
          [
            "0",
            "fetch",
            {},
          ],
        ],
        "initialFetch": [
          [
            "0",
            "fetch",
            {},
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 0,
      }
    `);
    expect(hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('one parent no child - reversed', () => {
    const {messages, storage, hydrate} = fetchTest(oneParentNoChildTest, true);
    expect(messages).toMatchInlineSnapshot(`
      {
        "cleanup": [
          [
            "1",
            "cleanup",
            {},
          ],
        ],
        "fetch": [
          [
            "1",
            "fetch",
            {},
          ],
        ],
        "initialFetch": [
          [
            "1",
            "fetch",
            {},
          ],
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`{}`);
    expect(hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('three parents, two with children', () => {
    const {messages, storage, hydrate} = fetchTest(
      threeParentsTwoWithChildrenTest,
    );
    expect(messages).toMatchInlineSnapshot(`
      {
        "cleanup": [
          [
            "0",
            "cleanup",
            {},
          ],
          [
            "1",
            "cleanup",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
          [
            "1",
            "cleanup",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            "1",
            "cleanup",
            {
              "constraint": {
                "issueID": "i3",
              },
            },
          ],
        ],
        "fetch": [
          [
            "0",
            "fetch",
            {},
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i3",
              },
            },
          ],
        ],
        "initialFetch": [
          [
            "0",
            "fetch",
            {},
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i3",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i3",
              },
            },
          ],
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i2"]": 0,
        "row//["i3"]": 1,
      }
    `);
    expect(hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "comments": [
              {
                "relationships": {},
                "row": {
                  "id": "c1",
                  "issueID": "i1",
                },
              },
            ],
          },
          "row": {
            "id": "i1",
          },
        },
        {
          "relationships": {
            "comments": [
              {
                "relationships": {},
                "row": {
                  "id": "c2",
                  "issueID": "i3",
                },
              },
            ],
          },
          "row": {
            "id": "i3",
          },
        },
      ]
    `);
  });

  test('three parents, two children - reversed', () => {
    const {messages, storage, hydrate} = fetchTest(
      threeParentsTwoWithChildrenTest,
      true,
    );

    expect(messages).toMatchInlineSnapshot(`
      {
        "cleanup": [
          [
            "1",
            "cleanup",
            {},
          ],
          [
            "0",
            "cleanup",
            {
              "constraint": {
                "id": "i1",
              },
            },
          ],
          [
            "0",
            "cleanup",
            {
              "constraint": {
                "id": "i3",
              },
            },
          ],
        ],
        "fetch": [
          [
            "1",
            "fetch",
            {},
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i1",
              },
            },
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i3",
              },
            },
          ],
        ],
        "initialFetch": [
          [
            "1",
            "fetch",
            {},
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i1",
              },
            },
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i3",
              },
            },
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i1",
              },
            },
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i3",
              },
            },
          ],
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`
      {
        "row/["i1"]/["c1"]": 1,
        "row/["i3"]/["c2"]": 1,
      }
    `);
    expect(hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "from_comments": [
              {
                "relationships": {},
                "row": {
                  "id": "i1",
                },
              },
            ],
          },
          "row": {
            "id": "c1",
            "issueID": "i1",
          },
        },
        {
          "relationships": {
            "from_comments": [
              {
                "relationships": {},
                "row": {
                  "id": "i3",
                },
              },
            ],
          },
          "row": {
            "id": "c2",
            "issueID": "i3",
          },
        },
      ]
    `);
  });

  test('three parents no children', () => {
    const {messages, storage, hydrate} = fetchTest(threeParentsNoChildrenTest);
    expect(messages).toMatchInlineSnapshot(`
      {
        "cleanup": [
          [
            "0",
            "cleanup",
            {},
          ],
          [
            "1",
            "cleanup",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            "1",
            "cleanup",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
          [
            "1",
            "cleanup",
            {
              "constraint": {
                "issueID": "i3",
              },
            },
          ],
        ],
        "fetch": [
          [
            "0",
            "fetch",
            {},
          ],
        ],
        "initialFetch": [
          [
            "0",
            "fetch",
            {},
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i3",
              },
            },
          ],
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 0,
        "row//["i2"]": 0,
        "row//["i3"]": 0,
      }
    `);
    expect(hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('three parents no children - reversed', () => {
    const {messages, storage, hydrate} = fetchTest(
      threeParentsNoChildrenTest,
      true,
    );

    expect(messages).toMatchInlineSnapshot(`
      {
        "cleanup": [
          [
            "1",
            "cleanup",
            {},
          ],
        ],
        "fetch": [
          [
            "1",
            "fetch",
            {},
          ],
        ],
        "initialFetch": [
          [
            "1",
            "fetch",
            {},
          ],
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`{}`);
    expect(hydrate).toMatchInlineSnapshot(`[]`);
  });
});

suite('NOT EXISTS', () => {
  test('one parent with child', () => {
    const {messages, storage, hydrate} = fetchTest({
      ...oneParentWithChildTest,
      existsType: 'NOT EXISTS',
    });
    expect(messages).toMatchInlineSnapshot(`
      {
        "cleanup": [
          [
            "0",
            "cleanup",
            {},
          ],
          [
            "1",
            "cleanup",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
        ],
        "fetch": [
          [
            "0",
            "fetch",
            {},
          ],
        ],
        "initialFetch": [
          [
            "0",
            "fetch",
            {},
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
      }
    `);
    expect(hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('one parent with child - reversed', () => {
    const {messages, storage, hydrate} = fetchTest(
      {
        ...oneParentWithChildTest,
        existsType: 'NOT EXISTS',
      },
      true,
    );

    expect(messages).toMatchInlineSnapshot(`
      {
        "cleanup": [
          [
            "1",
            "cleanup",
            {},
          ],
          [
            "0",
            "cleanup",
            {
              "constraint": {
                "id": "i1",
              },
            },
          ],
        ],
        "fetch": [
          [
            "1",
            "fetch",
            {},
          ],
        ],
        "initialFetch": [
          [
            "1",
            "fetch",
            {},
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i1",
              },
            },
          ],
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`
      {
        "row/["i1"]/["c1"]": 1,
      }
    `);
    expect(hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('one parent no child', () => {
    const {messages, storage, hydrate} = fetchTest({
      ...oneParentNoChildTest,
      existsType: 'NOT EXISTS',
    });
    expect(messages).toMatchInlineSnapshot(`
      {
        "cleanup": [
          [
            "0",
            "cleanup",
            {},
          ],
          [
            "1",
            "cleanup",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
        ],
        "fetch": [
          [
            "0",
            "fetch",
            {},
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
        ],
        "initialFetch": [
          [
            "0",
            "fetch",
            {},
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 0,
      }
    `);
    expect(hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "comments": [],
          },
          "row": {
            "id": "i1",
          },
        },
      ]
    `);
  });

  test('one parent, no child - reversed', () => {
    const {messages, storage, hydrate} = fetchTest(
      {
        ...oneParentNoChildTest,
        existsType: 'NOT EXISTS',
      },
      true,
    );

    expect(messages).toMatchInlineSnapshot(`
      {
        "cleanup": [
          [
            "1",
            "cleanup",
            {},
          ],
        ],
        "fetch": [
          [
            "1",
            "fetch",
            {},
          ],
        ],
        "initialFetch": [
          [
            "1",
            "fetch",
            {},
          ],
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`{}`);
    expect(hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('three parents, two with children', () => {
    const {messages, storage, hydrate} = fetchTest({
      ...threeParentsTwoWithChildrenTest,
      existsType: 'NOT EXISTS',
    });
    expect(messages).toMatchInlineSnapshot(`
      {
        "cleanup": [
          [
            "0",
            "cleanup",
            {},
          ],
          [
            "1",
            "cleanup",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            "1",
            "cleanup",
            {
              "constraint": {
                "issueID": "i3",
              },
            },
          ],
          [
            "1",
            "cleanup",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
        ],
        "fetch": [
          [
            "0",
            "fetch",
            {},
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
        ],
        "initialFetch": [
          [
            "0",
            "fetch",
            {},
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i3",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i2"]": 0,
        "row//["i3"]": 1,
      }
    `);
    expect(hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "comments": [],
          },
          "row": {
            "id": "i2",
          },
        },
      ]
    `);
  });

  test('three parents, two with children - reversed', () => {
    const {messages, storage, hydrate} = fetchTest(
      {
        ...threeParentsTwoWithChildrenTest,
        existsType: 'NOT EXISTS',
      },
      true,
    );
    expect(messages).toMatchInlineSnapshot(`
      {
        "cleanup": [
          [
            "1",
            "cleanup",
            {},
          ],
          [
            "0",
            "cleanup",
            {
              "constraint": {
                "id": "i1",
              },
            },
          ],
          [
            "0",
            "cleanup",
            {
              "constraint": {
                "id": "i3",
              },
            },
          ],
        ],
        "fetch": [
          [
            "1",
            "fetch",
            {},
          ],
        ],
        "initialFetch": [
          [
            "1",
            "fetch",
            {},
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i1",
              },
            },
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i3",
              },
            },
          ],
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`
      {
        "row/["i1"]/["c1"]": 1,
        "row/["i3"]/["c2"]": 1,
      }
    `);
    expect(hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('three parents, no children', () => {
    const {messages, storage, hydrate} = fetchTest({
      ...threeParentsNoChildrenTest,
      existsType: 'NOT EXISTS',
    });
    expect(messages).toMatchInlineSnapshot(`
      {
        "cleanup": [
          [
            "0",
            "cleanup",
            {},
          ],
          [
            "1",
            "cleanup",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            "1",
            "cleanup",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
          [
            "1",
            "cleanup",
            {
              "constraint": {
                "issueID": "i3",
              },
            },
          ],
        ],
        "fetch": [
          [
            "0",
            "fetch",
            {},
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i3",
              },
            },
          ],
        ],
        "initialFetch": [
          [
            "0",
            "fetch",
            {},
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i3",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueID": "i3",
              },
            },
          ],
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 0,
        "row//["i2"]": 0,
        "row//["i3"]": 0,
      }
    `);
    expect(hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "comments": [],
          },
          "row": {
            "id": "i1",
          },
        },
        {
          "relationships": {
            "comments": [],
          },
          "row": {
            "id": "i2",
          },
        },
        {
          "relationships": {
            "comments": [],
          },
          "row": {
            "id": "i3",
          },
        },
      ]
    `);
  });
});

test('three parents, no children - reversed', () => {
  const {messages, storage, hydrate} = fetchTest(
    {
      ...threeParentsNoChildrenTest,
      existsType: 'NOT EXISTS',
    },
    true,
  );

  expect(messages).toMatchInlineSnapshot(`
    {
      "cleanup": [
        [
          "1",
          "cleanup",
          {},
        ],
      ],
      "fetch": [
        [
          "1",
          "fetch",
          {},
        ],
      ],
      "initialFetch": [
        [
          "1",
          "fetch",
          {},
        ],
      ],
    }
  `);
  expect(storage).toMatchInlineSnapshot(`{}`);
  expect(hydrate).toMatchInlineSnapshot(`[]`);
});

// This test runs the join through three phases:
// initial fetch, fetch, and cleanup.
function fetchTest(t: FetchTest, reverse: boolean = false): FetchTestResults {
  const log: SnitchMessage[] = [];

  const sources = t.sources.map((rows, i) => {
    const ordering = t.sorts?.[i] ?? [['id', 'asc']];
    const source = createSource(
      lc,
      testLogConfig,
      `t${i}`,
      t.columns[i],
      t.primaryKeys[i],
    );
    for (const row of rows) {
      source.push({type: 'add', row});
    }
    const snitch = new Snitch(source.connect(ordering), String(i), log);
    return {
      source,
      snitch,
    };
  });

  if (reverse) {
    sources.reverse();
  }

  const existsStorage = new MemoryStorage();

  const filter = buildFilterPipeline(
    new Join({
      parent: sources[0].snitch,
      child: sources[1].snitch,
      storage: new MemoryStorage(),
      ...(reverse
        ? {
            parentKey: t.join.childKey,
            childKey: t.join.parentKey,
            relationshipName: 'from_' + t.join.relationshipName,
          }
        : t.join),
      hidden: false,
      system: 'client',
    }),
    filterInput =>
      new Exists(
        filterInput,
        existsStorage,
        reverse ? 'from_' + t.join.relationshipName : t.join.relationshipName,
        reverse ? t.join.childKey : t.join.parentKey,
        t.existsType,
      ),
  );

  const result: FetchTestResults = {
    hydrate: [],
    storage: {},
    messages: {
      initialFetch: [],
      fetch: [],
      cleanup: [],
    },
  };
  for (const [method, fetchType] of [
    ['fetch', 'initialFetch'],
    ['fetch', 'fetch'],
    ['cleanup', 'cleanup'],
  ] as const) {
    log.length = 0;

    const c = new Catch(filter);
    const r = c[method]();
    expect(c.pushes).toEqual([]);

    switch (fetchType) {
      case 'initialFetch': {
        result.hydrate = r;
        result.storage = existsStorage.cloneData();
        break;
      }
      case 'fetch': {
        expect(r).toEqual(result.hydrate);
        expect(existsStorage.cloneData()).toEqual(result.storage);
        break;
      }
      case 'cleanup': {
        expect(r).toEqual(result.hydrate);
        expect(existsStorage.cloneData()).toEqual({});
        break;
      }
      default:
        unreachable(fetchType);
    }
    result.messages[fetchType] = [...log];
  }
  return result;
}

type FetchTest = {
  columns: readonly Record<string, SchemaValue>[];
  primaryKeys: readonly PrimaryKey[];
  sources: readonly Row[][];
  sorts?: (Ordering | undefined)[] | undefined;
  join: {
    parentKey: CompoundKey;
    childKey: CompoundKey;
    relationshipName: string;
  };
  existsType: 'EXISTS' | 'NOT EXISTS';
};

type FetchTestResults = {
  messages: {
    initialFetch: SnitchMessage[];
    fetch: SnitchMessage[];
    cleanup: SnitchMessage[];
  };
  storage: Record<string, JSONValue>;
  hydrate: CaughtNode[];
};
