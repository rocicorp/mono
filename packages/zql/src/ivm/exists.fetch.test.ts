import {expect, suite, test} from 'vitest';
import {unreachable} from '../../../shared/src/asserts.js';
import type {JSONValue} from '../../../shared/src/json.js';
import type {CompoundKey, Ordering} from '../../../zero-protocol/src/ast.js';
import type {Row} from '../../../zero-protocol/src/data.js';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.js';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.js';
import {Catch} from './catch.js';
import type {Node} from './data.js';
import {Exists} from './exists.js';
import {Join} from './join.js';
import {MemoryStorage} from './memory-storage.js';
import {Snitch, type SnitchMessage} from './snitch.js';
import {createSource} from './test/source-factory.js';

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
            "0",
            "fetch",
            {
              "start": {
                "basis": "at",
                "row": {
                  "id": "i1",
                },
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
        "["size",["i1"]]": 1,
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
            "0",
            "fetch",
            {
              "start": {
                "basis": "at",
                "row": {
                  "id": "i1",
                },
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
        "["size",["i1"]]": 0,
      }
    `);
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
            "0",
            "fetch",
            {
              "start": {
                "basis": "at",
                "row": {
                  "id": "i1",
                },
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
            "0",
            "fetch",
            {
              "start": {
                "basis": "at",
                "row": {
                  "id": "i2",
                },
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
            "0",
            "fetch",
            {
              "start": {
                "basis": "at",
                "row": {
                  "id": "i3",
                },
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
        "["size",["i1"]]": 1,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 1,
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
            "0",
            "fetch",
            {
              "start": {
                "basis": "at",
                "row": {
                  "id": "i1",
                },
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
            "0",
            "fetch",
            {
              "start": {
                "basis": "at",
                "row": {
                  "id": "i2",
                },
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
            "0",
            "fetch",
            {
              "start": {
                "basis": "at",
                "row": {
                  "id": "i3",
                },
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
        "["size",["i1"]]": 0,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 0,
      }
    `);
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
            "0",
            "fetch",
            {
              "start": {
                "basis": "at",
                "row": {
                  "id": "i1",
                },
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
        "["size",["i1"]]": 1,
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
            "0",
            "fetch",
            {
              "start": {
                "basis": "at",
                "row": {
                  "id": "i1",
                },
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
        "["size",["i1"]]": 0,
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
            "0",
            "fetch",
            {
              "start": {
                "basis": "at",
                "row": {
                  "id": "i1",
                },
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
            "0",
            "fetch",
            {
              "start": {
                "basis": "at",
                "row": {
                  "id": "i2",
                },
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
            "0",
            "fetch",
            {
              "start": {
                "basis": "at",
                "row": {
                  "id": "i3",
                },
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
        "["size",["i1"]]": 1,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 1,
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
  test('three parents no children', () => {
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
            "0",
            "fetch",
            {
              "start": {
                "basis": "at",
                "row": {
                  "id": "i1",
                },
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
            "0",
            "fetch",
            {
              "start": {
                "basis": "at",
                "row": {
                  "id": "i2",
                },
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
            "0",
            "fetch",
            {
              "start": {
                "basis": "at",
                "row": {
                  "id": "i3",
                },
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
        "["size",["i1"]]": 0,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 0,
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

// This test runs the join through three phases:
// initial fetch, fetch, and cleanup.
function fetchTest(t: FetchTest): FetchTestResults {
  const log: SnitchMessage[] = [];

  const sources = t.sources.map((rows, i) => {
    const ordering = t.sorts?.[i] ?? [['id', 'asc']];
    const source = createSource(`t${i}`, t.columns[i], t.primaryKeys[i]);
    for (const row of rows) {
      source.push({type: 'add', row});
    }
    const snitch = new Snitch(source.connect(ordering), String(i), log);
    return {
      source,
      snitch,
    };
  });

  const existsStorage = new MemoryStorage();
  const exists = new Exists(
    new Join({
      parent: sources[0].snitch,
      child: sources[1].snitch,
      storage: new MemoryStorage(),
      ...t.join,
      hidden: false,
      system: 'client',
    }),
    existsStorage,
    t.join.relationshipName,
    t.existsType,
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

    const c = new Catch(exists);
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
  hydrate: Node[];
};
