import {expect, suite, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import type {CompoundKey, Ordering} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.ts';
import {Catch, type CaughtChange} from './catch.ts';
import type {Input} from './operator.ts';
import {Snitch, type SnitchMessage} from './snitch.ts';
import type {SourceChange} from './source.ts';
import {createSource} from './test/source-factory.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {FlippedJoin} from './flipped-join.ts';
import {consume} from './stream.ts';

const lc = createSilentLogContext();

/**
 * These tests are based on join.sibling.test.ts.  Uses same cases.
 * Most of the data snapshots are the same expect for when
 * the difference between Join being left join and FlippedJoin being inner join
 * effects the data results.
 * The log snapshots are as expected quite different.
 */

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
    const {log, output} = pushSiblingTest({
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
          "1",
          "fetchCount",
          {
            "constraint": {
              "issueId": "i3",
            },
          },
          0,
        ],
      ]
    `);
    expect(output).toMatchInlineSnapshot(`[]`);
  });

  test('push owner', () => {
    const {log, output} = pushSiblingTest({
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
          "1",
          "fetch",
          {},
        ],
        [
          "1",
          "fetchCount",
          {},
          4,
        ],
        [
          "0",
          "fetch",
          {
            "constraint": {
              "id": "i1",
              "ownerId": "o2",
            },
          },
        ],
        [
          "0",
          "fetch",
          {
            "constraint": {
              "id": "i1",
              "ownerId": "o2",
            },
          },
        ],
        [
          "0",
          "fetch",
          {
            "constraint": {
              "id": "i2",
              "ownerId": "o2",
            },
          },
        ],
        [
          "0",
          "fetch",
          {
            "constraint": {
              "id": "i2",
              "ownerId": "o2",
            },
          },
        ],
        [
          "0",
          "fetchCount",
          {
            "constraint": {
              "id": "i1",
              "ownerId": "o2",
            },
          },
          0,
        ],
        [
          "0",
          "fetchCount",
          {
            "constraint": {
              "id": "i1",
              "ownerId": "o2",
            },
          },
          0,
        ],
        [
          "0",
          "fetchCount",
          {
            "constraint": {
              "id": "i2",
              "ownerId": "o2",
            },
          },
          1,
        ],
        [
          "0",
          "fetchCount",
          {
            "constraint": {
              "id": "i2",
              "ownerId": "o2",
            },
          },
          1,
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
    expect(output).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "comments": [
                {
                  "relationships": {},
                  "row": {
                    "id": "c3",
                    "issueId": "i2",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "id": "c4",
                    "issueId": "i2",
                  },
                },
              ],
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
              "id": "i2",
              "ownerId": "o2",
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('push comment', () => {
    const {log, output} = pushSiblingTest({
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
          "1",
          "fetch",
          {
            "constraint": {
              "issueId": "i1",
            },
          },
        ],
        [
          "1",
          "fetchCount",
          {
            "constraint": {
              "issueId": "i1",
            },
          },
          1,
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
        [
          "2",
          "fetchCount",
          {
            "constraint": {
              "id": "o1",
            },
          },
          1,
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
    const {log, output} = pushSiblingTest({
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
          "1",
          "fetch",
          {},
        ],
        [
          "1",
          "fetchCount",
          {},
          4,
        ],
        [
          "0",
          "fetch",
          {
            "constraint": {
              "id": "i1",
              "ownerId": "o2",
            },
          },
        ],
        [
          "0",
          "fetch",
          {
            "constraint": {
              "id": "i1",
              "ownerId": "o2",
            },
          },
        ],
        [
          "0",
          "fetch",
          {
            "constraint": {
              "id": "i2",
              "ownerId": "o2",
            },
          },
        ],
        [
          "0",
          "fetch",
          {
            "constraint": {
              "id": "i2",
              "ownerId": "o2",
            },
          },
        ],
        [
          "0",
          "fetchCount",
          {
            "constraint": {
              "id": "i1",
              "ownerId": "o2",
            },
          },
          0,
        ],
        [
          "0",
          "fetchCount",
          {
            "constraint": {
              "id": "i1",
              "ownerId": "o2",
            },
          },
          0,
        ],
        [
          "0",
          "fetchCount",
          {
            "constraint": {
              "id": "i2",
              "ownerId": "o2",
            },
          },
          1,
        ],
        [
          "0",
          "fetchCount",
          {
            "constraint": {
              "id": "i2",
              "ownerId": "o2",
            },
          },
          1,
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
          "2",
          "fetchCount",
          {
            "constraint": {
              "id": "o2",
            },
          },
          0,
        ],
      ]
    `);
    expect(output).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "comments": [
                {
                  "relationships": {},
                  "row": {
                    "id": "c3",
                    "issueId": "i2",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "id": "c4",
                    "issueId": "i2",
                  },
                },
              ],
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
              "id": "i2",
              "ownerId": "o2",
            },
          },
          "type": "remove",
        },
      ]
    `);
  });

  test('remove comment', () => {
    const {log, output} = pushSiblingTest({
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
          "1",
          "fetch",
          {
            "constraint": {
              "issueId": "i2",
            },
          },
        ],
        [
          "1",
          "fetchCount",
          {
            "constraint": {
              "issueId": "i2",
            },
          },
          1,
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
          "2",
          "fetchCount",
          {
            "constraint": {
              "id": "o2",
            },
          },
          1,
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
      const {log, output} = pushSiblingTest({
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
            "fetch",
            {
              "constraint": {
                "issueId": "i1",
              },
            },
          ],
          [
            "1",
            "fetchCount",
            {
              "constraint": {
                "issueId": "i1",
              },
            },
            1,
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
          [
            "2",
            "fetchCount",
            {
              "constraint": {
                "id": "o1",
              },
            },
            1,
          ],
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
      const {log, output} = pushSiblingTest({
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
            "2",
            "fetch",
            {
              "constraint": {
                "id": "o2",
              },
            },
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
      const {log, output} = pushSiblingTest({
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
            "1",
            "fetch",
            {},
          ],
          [
            "1",
            "fetchCount",
            {},
            4,
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i1",
                "ownerId": "o2",
              },
            },
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i1",
                "ownerId": "o2",
              },
            },
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i2",
                "ownerId": "o2",
              },
            },
          ],
          [
            "0",
            "fetch",
            {
              "constraint": {
                "id": "i2",
                "ownerId": "o2",
              },
            },
          ],
          [
            "0",
            "fetchCount",
            {
              "constraint": {
                "id": "i1",
                "ownerId": "o2",
              },
            },
            0,
          ],
          [
            "0",
            "fetchCount",
            {
              "constraint": {
                "id": "i1",
                "ownerId": "o2",
              },
            },
            0,
          ],
          [
            "0",
            "fetchCount",
            {
              "constraint": {
                "id": "i2",
                "ownerId": "o2",
              },
            },
            1,
          ],
          [
            "0",
            "fetchCount",
            {
              "constraint": {
                "id": "i2",
                "ownerId": "o2",
              },
            },
            1,
          ],
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
    const source = createSource(
      lc,
      testLogConfig,
      'test',
      t.columns[i],
      t.primaryKeys[i],
    );
    for (const row of rows) {
      consume(source.push({type: 'add', row}));
    }
    const snitch = new Snitch(source.connect(ordering), String(i), log, [
      'fetch',
      'fetchCount',
      'push',
    ]);
    return {
      source,
      snitch,
    };
  });

  const joins: FlippedJoin[] = [];

  let parent: Input = sources[0].snitch;

  for (let i = 0; i < t.joins.length; i++) {
    const info = t.joins[i];
    const child = sources[i + 1].snitch;

    const join = new FlippedJoin({
      parent,
      child,
      ...info,
      hidden: false,
      system: 'client',
    });

    joins[i] = join;

    parent = join;
  }

  const finalJoin = joins[joins.length - 1];

  const c = new Catch(finalJoin);
  c.fetch();

  log.length = 0;

  for (const [sourceIndex, change] of t.pushes) {
    consume(sources[sourceIndex].source.push(change));
  }

  return {
    log,
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
  output: CaughtChange[];
};
