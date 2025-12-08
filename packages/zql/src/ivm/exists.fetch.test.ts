import {expect, suite, test, vi} from 'vitest';
import {unreachable} from '../../../shared/src/asserts.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import type {CompoundKey, Ordering} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.ts';
import {Catch, type CaughtNode} from './catch.ts';
import {Exists} from './exists.ts';
import {Join} from './join.ts';
import {MemoryStorage} from './memory-storage.ts';
import {Snitch, type SnitchMessage} from './snitch.ts';
import {createSource} from './test/source-factory.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {buildFilterPipeline, type FilterInput} from './filter-operators.ts';
import {consume} from './stream.ts';
import type {BuilderDelegate} from '../builder/builder.ts';

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

const mockDelegate = {
  addEdge() {},
} as unknown as BuilderDelegate;

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
      {id: 'c2', issueID: 'i1'},
      {id: 'c3', issueID: 'i3'},
      {id: 'c4', issueID: 'i3'},
      {id: 'c5', issueID: 'i3'},
      {id: 'c6', issueID: 'i4'},
      {id: 'c7', issueID: 'i4'},
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
    const {messages, storage, cacheHitCounts, hydrate} = fetchTest(
      oneParentWithChildTest,
    );
    expect(messages).toMatchInlineSnapshot(`
      {
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
    expect(storage).toMatchInlineSnapshot(`{}`);
    expect(cacheHitCounts).toMatchInlineSnapshot(`Map {}`);
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
    const {messages, storage, cacheHitCounts, hydrate} = fetchTest(
      oneParentWithChildTest,
      true,
    );
    expect(messages).toMatchInlineSnapshot(`
      {
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
    expect(storage).toMatchInlineSnapshot(`{}`);
    expect(cacheHitCounts).toMatchInlineSnapshot(`Map {}`);
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
    const {messages, storage, cacheHitCounts, hydrate} =
      fetchTest(oneParentNoChildTest);
    expect(messages).toMatchInlineSnapshot(`
      {
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
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`{}`);
    expect(cacheHitCounts).toMatchInlineSnapshot(`Map {}`);
    expect(hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('one parent no child - reversed', () => {
    const {messages, storage, cacheHitCounts, hydrate} = fetchTest(
      oneParentNoChildTest,
      true,
    );
    expect(messages).toMatchInlineSnapshot(`
      {
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
    expect(cacheHitCounts).toMatchInlineSnapshot(`Map {}`);
    expect(hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('three parents, two with children', () => {
    const {messages, storage, cacheHitCounts, hydrate} = fetchTest(
      threeParentsTwoWithChildrenTest,
    );
    expect(messages).toMatchInlineSnapshot(`
      {
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
    expect(storage).toMatchInlineSnapshot(`{}`);
    expect(cacheHitCounts).toMatchInlineSnapshot(`Map {}`);
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
              {
                "relationships": {},
                "row": {
                  "id": "c2",
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
                  "id": "c3",
                  "issueID": "i3",
                },
              },
              {
                "relationships": {},
                "row": {
                  "id": "c4",
                  "issueID": "i3",
                },
              },
              {
                "relationships": {},
                "row": {
                  "id": "c5",
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
    const {messages, storage, cacheHitCounts, hydrate} = fetchTest(
      threeParentsTwoWithChildrenTest,
      true,
    );

    expect(messages).toMatchInlineSnapshot(`
      {
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
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i4",
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
                "id": "i3",
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
                "id": "i4",
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
                "id": "i3",
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
    expect(storage).toMatchInlineSnapshot(`{}`);
    expect(cacheHitCounts).toMatchInlineSnapshot(`
      Map {
        "["i1"]" => 1,
        "["i3"]" => 2,
        "["i4"]" => 1,
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
                  "id": "i1",
                },
              },
            ],
          },
          "row": {
            "id": "c2",
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
            "id": "c3",
            "issueID": "i3",
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
            "id": "c4",
            "issueID": "i3",
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
            "id": "c5",
            "issueID": "i3",
          },
        },
      ]
    `);
  });

  test('three parents no children', () => {
    const {messages, storage, cacheHitCounts, hydrate} = fetchTest(
      threeParentsNoChildrenTest,
    );
    expect(messages).toMatchInlineSnapshot(`
      {
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
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`{}`);
    expect(cacheHitCounts).toMatchInlineSnapshot(`Map {}`);
    expect(hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('three parents no children - reversed', () => {
    const {messages, storage, cacheHitCounts, hydrate} = fetchTest(
      threeParentsNoChildrenTest,
      true,
    );

    expect(messages).toMatchInlineSnapshot(`
      {
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
    expect(cacheHitCounts).toMatchInlineSnapshot(`Map {}`);
    expect(hydrate).toMatchInlineSnapshot(`[]`);
  });
});

suite('NOT EXISTS', () => {
  test('one parent with child', () => {
    const {messages, storage, cacheHitCounts, hydrate} = fetchTest({
      ...oneParentWithChildTest,
      existsType: 'NOT EXISTS',
    });
    expect(messages).toMatchInlineSnapshot(`
      {
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
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`{}`);
    expect(cacheHitCounts).toMatchInlineSnapshot(`Map {}`);
    expect(hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('one parent with child - reversed', () => {
    const {messages, storage, cacheHitCounts, hydrate} = fetchTest(
      {
        ...oneParentWithChildTest,
        existsType: 'NOT EXISTS',
      },
      true,
    );

    expect(messages).toMatchInlineSnapshot(`
      {
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
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`{}`);
    expect(cacheHitCounts).toMatchInlineSnapshot(`Map {}`);
    expect(hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('one parent no child', () => {
    const {messages, storage, cacheHitCounts, hydrate} = fetchTest({
      ...oneParentNoChildTest,
      existsType: 'NOT EXISTS',
    });
    expect(messages).toMatchInlineSnapshot(`
      {
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
    expect(storage).toMatchInlineSnapshot(`{}`);
    expect(cacheHitCounts).toMatchInlineSnapshot(`Map {}`);
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
    const {messages, storage, cacheHitCounts, hydrate} = fetchTest(
      {
        ...oneParentNoChildTest,
        existsType: 'NOT EXISTS',
      },
      true,
    );

    expect(messages).toMatchInlineSnapshot(`
      {
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
    expect(cacheHitCounts).toMatchInlineSnapshot(`Map {}`);
    expect(hydrate).toMatchInlineSnapshot(`[]`);
  });

  test('three parents, two with children', () => {
    const {messages, storage, cacheHitCounts, hydrate} = fetchTest({
      ...threeParentsTwoWithChildrenTest,
      existsType: 'NOT EXISTS',
    });
    expect(messages).toMatchInlineSnapshot(`
      {
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
    expect(storage).toMatchInlineSnapshot(`{}`);
    expect(cacheHitCounts).toMatchInlineSnapshot(`Map {}`);
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
    const {messages, storage, cacheHitCounts, hydrate} = fetchTest(
      {
        ...threeParentsTwoWithChildrenTest,
        existsType: 'NOT EXISTS',
      },
      true,
    );
    expect(messages).toMatchInlineSnapshot(`
      {
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
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i4",
              },
            },
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i4",
              },
            },
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i4",
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
                "id": "i4",
              },
            },
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i4",
              },
            },
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i4",
              },
            },
          ],
        ],
      }
    `);
    expect(storage).toMatchInlineSnapshot(`{}`);
    expect(cacheHitCounts).toMatchInlineSnapshot(`
      Map {
        "["i1"]" => 1,
        "["i3"]" => 2,
        "["i4"]" => 1,
      }
    `);
    expect(hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "from_comments": [],
          },
          "row": {
            "id": "c6",
            "issueID": "i4",
          },
        },
        {
          "relationships": {
            "from_comments": [],
          },
          "row": {
            "id": "c7",
            "issueID": "i4",
          },
        },
      ]
    `);
  });

  test('three parents, no children', () => {
    const {messages, storage, cacheHitCounts, hydrate} = fetchTest({
      ...threeParentsNoChildrenTest,
      existsType: 'NOT EXISTS',
    });
    expect(messages).toMatchInlineSnapshot(`
      {
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
    expect(storage).toMatchInlineSnapshot(`{}`);
    expect(cacheHitCounts).toMatchInlineSnapshot(`Map {}`);
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
  const {messages, storage, cacheHitCounts, hydrate} = fetchTest(
    {
      ...threeParentsNoChildrenTest,
      existsType: 'NOT EXISTS',
    },
    true,
  );

  expect(messages).toMatchInlineSnapshot(`
    {
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
  expect(cacheHitCounts).toMatchInlineSnapshot(`Map {}`);
  expect(hydrate).toMatchInlineSnapshot(`[]`);
});

test('Exists forwards beginFilter/endFilter', () => {
  const mockInput = {
    setFilterOutput: vi.fn(),
    getSchema: vi.fn(() => ({
      relationships: {
        rel: {
          type: 'many',
          source: 'child',
          sourceField: ['childID'],
          destField: ['id'],
        },
      },
      primaryKey: ['id'],
    })),
    destroy: vi.fn(),
  } as unknown as FilterInput;

  const exists = new Exists(mockInput, 'rel', ['id'], 'EXISTS');

  const mockOutput = {
    push: vi.fn(),
    filter: vi.fn(),
    beginFilter: vi.fn(),
    endFilter: vi.fn(),
  };

  exists.setFilterOutput(mockOutput);

  exists.beginFilter();
  expect(mockOutput.beginFilter).toHaveBeenCalled();

  exists.endFilter();
  expect(mockOutput.endFilter).toHaveBeenCalled();
});

// This test runs the join through two phases:
// initial fetch and fetch.
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
      consume(source.push({type: 'add', row}));
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
  const cacheHitCounts = new Map();

  const filter = buildFilterPipeline(
    new Join({
      parent: sources[0].snitch,
      child: sources[1].snitch,
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
    mockDelegate,
    filterInput =>
      new Exists(
        filterInput,
        reverse ? 'from_' + t.join.relationshipName : t.join.relationshipName,
        reverse ? t.join.childKey : t.join.parentKey,
        t.existsType,
        cacheHitCounts,
      ),
  );

  const result: FetchTestResults = {
    hydrate: [],
    storage: {},
    messages: {
      initialFetch: [],
      fetch: [],
    },
    cacheHitCounts,
  };
  for (const fetchType of ['initialFetch', 'fetch'] as const) {
    log.length = 0;

    const prevCacheHitCounts = new Map(cacheHitCounts);
    cacheHitCounts.clear();

    const c = new Catch(filter);
    const r = c.fetch({});
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
        expect(cacheHitCounts).toEqual(prevCacheHitCounts);
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
  };
  storage: Record<string, JSONValue>;
  hydrate: CaughtNode[];
  cacheHitCounts: Map<string, number>;
};
