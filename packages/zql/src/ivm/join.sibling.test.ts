import {expect, suite, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.js';
import type {JSONValue} from '../../../shared/src/json.js';
import type {CompoundKey, Ordering} from '../../../zero-protocol/src/ast.js';
import type {Row} from '../../../zero-protocol/src/data.js';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.js';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.js';
import {Catch, type CaughtChange} from './catch.js';
import {Join} from './join.js';
import {MemoryStorage} from './memory-storage.js';
import type {Input} from './operator.js';
import {Snitch, type SnitchMessage} from './snitch.js';
import type {SourceChange} from './source.js';
import {createSource} from './test/source-factory.js';

suite('sibling relationships tests with issues, comments, and owners', () => {
  const base = {
    columns: [
      {id: {type: 'string'}, ownerId: {type: 'string'}},
      {id: {type: 'string'}, issueId: {type: 'string'}},
      {id: {type: 'string'}},
    ],
    primaryKeys: [['id'], ['id'], ['id']],
    joins: [
      {
        parentKey: ['id'],
        childKey: ['issueId'],
        relationshipName: 'comments',
      },
      {
        parentKey: ['ownerId'],
        childKey: ['id'],
        relationshipName: 'owners',
      },
    ],
  } as const;

  test('create two issues, two comments each, one owner each, push a new issue with existing owner', () => {
    const {log, storage, output} = pushSiblingTest({
      ...base,
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
    });
    expect(log).toMatchInlineSnapshot(`
      [
        [
          "0",
          "push",
          {
            "row": {
              "id": "i3",
              "ownerId": "o2",
            },
            "type": "add",
          },
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "issueId": "i3",
            },
          },
        ],
        [
          "2",
          "fetch",
          {
            "constraint": {
              "id": "o2",
            },
          },
        ],
        [
          "1",
          "fetchCount",
          {
            "constraint": {
              "issueId": "i3",
            },
          },
          0,
        ],
        [
          "2",
          "fetchCount",
          {
            "constraint": {
              "id": "o2",
            },
          },
          1,
        ],
      ]
    `);
    expect(storage).toMatchInlineSnapshot(`
      [
        {
          ""pKeySet","i1","i1",": true,
          ""pKeySet","i2","i2",": true,
          ""pKeySet","i3","i3",": true,
        },
        {
          ""pKeySet","o1","i1",": true,
          ""pKeySet","o2","i2",": true,
          ""pKeySet","o2","i3",": true,
        },
      ]
    `);
    expect(output).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "comments": [],
              "owners": [
                {
                  "relationships": {},
                  "row": {
                    "id": "o2",
                  },
                },
              ],
            },
            "row": {
              "id": "i3",
              "ownerId": "o2",
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('push owner', () => {
    const {log, storage, output} = pushSiblingTest({
      ...base,
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
    });
    expect(log).toMatchInlineSnapshot(`
      [
        [
          "2",
          "push",
          {
            "row": {
              "id": "o2",
            },
            "type": "add",
          },
        ],
        [
          "0",
          "fetch",
          {
            "constraint": {
              "ownerId": "o2",
            },
          },
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "issueId": "i2",
            },
          },
        ],
        [
          "0",
          "fetchCount",
          {
            "constraint": {
              "ownerId": "o2",
            },
          },
          1,
        ],
      ]
    `);
    expect(storage).toMatchInlineSnapshot(`
      [
        {
          ""pKeySet","i1","i1",": true,
          ""pKeySet","i2","i2",": true,
        },
        {
          ""pKeySet","o1","i1",": true,
          ""pKeySet","o2","i2",": true,
        },
      ]
    `);
    expect(output).toMatchInlineSnapshot(`
      [
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "id": "o2",
                },
              },
              "type": "add",
            },
            "relationshipName": "owners",
          },
          "row": {
            "id": "i2",
            "ownerId": "o2",
          },
          "type": "child",
        },
      ]
    `);
  });

  test('push comment', () => {
    const {log, storage, output} = pushSiblingTest({
      ...base,
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
    });
    expect(log).toMatchInlineSnapshot(`
      [
        [
          "1",
          "push",
          {
            "row": {
              "id": "c5",
              "issueId": "i1",
            },
            "type": "add",
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
          "fetchCount",
          {
            "constraint": {
              "id": "i1",
            },
          },
          1,
        ],
      ]
    `);
    expect(storage).toMatchInlineSnapshot(`
      [
        {
          ""pKeySet","i1","i1",": true,
          ""pKeySet","i2","i2",": true,
        },
        {
          ""pKeySet","o1","i1",": true,
          ""pKeySet","o2","i2",": true,
        },
      ]
    `);
    expect(output).toMatchInlineSnapshot(`
      [
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "id": "c5",
                  "issueId": "i1",
                },
              },
              "type": "add",
            },
            "relationshipName": "comments",
          },
          "row": {
            "id": "i1",
            "ownerId": "o1",
          },
          "type": "child",
        },
      ]
    `);
  });

  test('remove owner', () => {
    const {log, storage, output} = pushSiblingTest({
      ...base,
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
    });
    expect(log).toMatchInlineSnapshot(`
      [
        [
          "2",
          "push",
          {
            "row": {
              "id": "o2",
            },
            "type": "remove",
          },
        ],
        [
          "0",
          "fetch",
          {
            "constraint": {
              "ownerId": "o2",
            },
          },
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "issueId": "i2",
            },
          },
        ],
        [
          "0",
          "fetchCount",
          {
            "constraint": {
              "ownerId": "o2",
            },
          },
          1,
        ],
      ]
    `);
    expect(storage).toMatchInlineSnapshot(`
      [
        {
          ""pKeySet","i1","i1",": true,
          ""pKeySet","i2","i2",": true,
        },
        {
          ""pKeySet","o1","i1",": true,
          ""pKeySet","o2","i2",": true,
        },
      ]
    `);
    expect(output).toMatchInlineSnapshot(`
      [
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "id": "o2",
                },
              },
              "type": "remove",
            },
            "relationshipName": "owners",
          },
          "row": {
            "id": "i2",
            "ownerId": "o2",
          },
          "type": "child",
        },
      ]
    `);
  });

  test('remove comment', () => {
    const {log, storage, output} = pushSiblingTest({
      ...base,
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
    });
    expect(log).toMatchInlineSnapshot(`
      [
        [
          "1",
          "push",
          {
            "row": {
              "id": "c4",
              "issueId": "i2",
            },
            "type": "remove",
          },
        ],
        [
          "0",
          "fetch",
          {
            "constraint": {
              "id": "i2",
            },
          },
        ],
        [
          "0",
          "fetchCount",
          {
            "constraint": {
              "id": "i2",
            },
          },
          1,
        ],
      ]
    `);
    expect(storage).toMatchInlineSnapshot(`
      [
        {
          ""pKeySet","i1","i1",": true,
          ""pKeySet","i2","i2",": true,
        },
        {
          ""pKeySet","o1","i1",": true,
          ""pKeySet","o2","i2",": true,
        },
      ]
    `);
    expect(output).toMatchInlineSnapshot(`
      [
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "id": "c4",
                  "issueId": "i2",
                },
              },
              "type": "remove",
            },
            "relationshipName": "comments",
          },
          "row": {
            "id": "i2",
            "ownerId": "o2",
          },
          "type": "child",
        },
      ]
    `);
  });

  const normalBase = base;

  suite('edit', () => {
    const base = {
      ...normalBase,
      columns: [
        {
          id: {type: 'string'},
          ownerId: {type: 'string'},
          text: {type: 'string'},
        },
        {
          id: {type: 'string'},
          issueId: {type: 'string'},
          text: {type: 'string'},
        },
        {id: {type: 'string'}, text: {type: 'string'}},
      ],
      sources: [
        [
          {id: 'i1', ownerId: 'o1', text: 'issue 1'},
          {id: 'i2', ownerId: 'o2', text: 'issue 2'},
        ],
        [
          {id: 'c1', issueId: 'i1', text: 'comment 1'},
          {id: 'c2', issueId: 'i1', text: 'comment 2'},
          {id: 'c3', issueId: 'i2', text: 'comment 3'},
          {id: 'c4', issueId: 'i2', text: 'comment 4'},
        ],
        [
          {id: 'o1', text: 'owner 1'},
          {id: 'o2', text: 'owner 2'},
        ],
      ],
    } as const;

    test('edit issue', () => {
      const {log, storage, output} = pushSiblingTest({
        ...base,
        pushes: [
          [
            0,
            {
              type: 'edit',
              oldRow: {id: 'i1', ownerId: 'o1', text: 'issue 1'},
              row: {id: 'i1', ownerId: 'o1', text: 'issue 1 changed'},
            },
          ],
        ],
      });
      expect(log).toMatchInlineSnapshot(`
        [
          [
            "0",
            "push",
            {
              "oldRow": {
                "id": "i1",
                "ownerId": "o1",
                "text": "issue 1",
              },
              "row": {
                "id": "i1",
                "ownerId": "o1",
                "text": "issue 1 changed",
              },
              "type": "edit",
            },
          ],
          [
            "1",
            "cleanup",
            {
              "constraint": {
                "issueId": "i1",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueId": "i1",
              },
            },
          ],
          [
            "2",
            "cleanup",
            {
              "constraint": {
                "id": "o1",
              },
            },
          ],
          [
            "2",
            "fetch",
            {
              "constraint": {
                "id": "o1",
              },
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        [
          {
            ""pKeySet","i1","i1",": true,
            ""pKeySet","i2","i2",": true,
          },
          {
            ""pKeySet","o1","i1",": true,
            ""pKeySet","o2","i2",": true,
          },
        ]
      `);
      expect(output).toMatchInlineSnapshot(`
        [
          {
            "oldRow": {
              "id": "i1",
              "ownerId": "o1",
              "text": "issue 1",
            },
            "row": {
              "id": "i1",
              "ownerId": "o1",
              "text": "issue 1 changed",
            },
            "type": "edit",
          },
        ]
      `);
    });

    test('edit comment', () => {
      const {log, storage, output} = pushSiblingTest({
        ...base,
        pushes: [
          [
            1,
            {
              type: 'edit',
              oldRow: {id: 'c4', issueId: 'i2', text: 'comment 4'},
              row: {id: 'c4', issueId: 'i2', text: 'comment 4 changed'},
            },
          ],
        ],
      });
      expect(log).toMatchInlineSnapshot(`
        [
          [
            "1",
            "push",
            {
              "oldRow": {
                "id": "c4",
                "issueId": "i2",
                "text": "comment 4",
              },
              "row": {
                "id": "c4",
                "issueId": "i2",
                "text": "comment 4 changed",
              },
              "type": "edit",
            },
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i2",
              },
            },
          ],
          [
            "0",
            "fetchCount",
            {
              "constraint": {
                "id": "i2",
              },
            },
            1,
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        [
          {
            ""pKeySet","i1","i1",": true,
            ""pKeySet","i2","i2",": true,
          },
          {
            ""pKeySet","o1","i1",": true,
            ""pKeySet","o2","i2",": true,
          },
        ]
      `);
      expect(output).toMatchInlineSnapshot(`
        [
          {
            "child": {
              "change": {
                "oldRow": {
                  "id": "c4",
                  "issueId": "i2",
                  "text": "comment 4",
                },
                "row": {
                  "id": "c4",
                  "issueId": "i2",
                  "text": "comment 4 changed",
                },
                "type": "edit",
              },
              "relationshipName": "comments",
            },
            "row": {
              "id": "i2",
              "ownerId": "o2",
              "text": "issue 2",
            },
            "type": "child",
          },
        ]
      `);
    });

    test('edit owner', () => {
      const {log, storage, output} = pushSiblingTest({
        ...base,
        pushes: [
          [
            2,
            {
              type: 'edit',
              oldRow: {id: 'o2', text: 'owner 2'},
              row: {id: 'o2', text: 'owner 2 changed'},
            },
          ],
        ],
      });
      expect(log).toMatchInlineSnapshot(`
        [
          [
            "2",
            "push",
            {
              "oldRow": {
                "id": "o2",
                "text": "owner 2",
              },
              "row": {
                "id": "o2",
                "text": "owner 2 changed",
              },
              "type": "edit",
            },
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "ownerId": "o2",
              },
            },
          ],
          [
            "1",
            "fetch",
            {
              "constraint": {
                "issueId": "i2",
              },
            },
          ],
          [
            "0",
            "fetchCount",
            {
              "constraint": {
                "ownerId": "o2",
              },
            },
            1,
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        [
          {
            ""pKeySet","i1","i1",": true,
            ""pKeySet","i2","i2",": true,
          },
          {
            ""pKeySet","o1","i1",": true,
            ""pKeySet","o2","i2",": true,
          },
        ]
      `);
      expect(output).toMatchInlineSnapshot(`
        [
          {
            "child": {
              "change": {
                "oldRow": {
                  "id": "o2",
                  "text": "owner 2",
                },
                "row": {
                  "id": "o2",
                  "text": "owner 2 changed",
                },
                "type": "edit",
              },
              "relationshipName": "owners",
            },
            "row": {
              "id": "i2",
              "ownerId": "o2",
              "text": "issue 2",
            },
            "type": "child",
          },
        ]
      `);
    });
  });
});

function pushSiblingTest(t: PushTestSibling): PushTestSiblingResults {
  assert(t.sources.length > 0);
  assert(t.joins.length === t.sources.length - 1);

  const log: SnitchMessage[] = [];

  const sources = t.sources.map((rows, i) => {
    const ordering = t.sorts?.[i] ?? [['id', 'asc']];
    const source = createSource('test', t.columns[i], t.primaryKeys[i]);
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

    const join = new Join({
      parent,
      child,
      storage,
      ...info,
      hidden: false,
      system: 'client',
    });

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

  const storage: Record<string, JSONValue>[] = [];
  for (const j of joins.values()) {
    storage.push(j.storage.cloneData());
  }

  return {
    log,
    storage,
    output: c.pushes,
  };
}

type PushTestSibling = {
  columns: readonly Record<string, SchemaValue>[];
  primaryKeys: readonly PrimaryKey[];
  sources: readonly (readonly Row[])[];
  sorts?: Record<number, Ordering> | undefined;
  joins: readonly {
    parentKey: CompoundKey;
    childKey: CompoundKey;
    relationshipName: string;
  }[];
  pushes: [sourceIndex: number, change: SourceChange][];
};

type PushTestSiblingResults = {
  log: SnitchMessage[];
  storage: Record<string, JSONValue>[];
  output: CaughtChange[];
};
