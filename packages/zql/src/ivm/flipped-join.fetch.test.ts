import {expect, suite, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {assert} from '../../../shared/src/asserts.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {CompoundKey, Ordering} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.ts';
import {Catch, type CaughtNode} from './catch.ts';
import {canonicalKey, FlippedJoin} from './flipped-join.ts';
import type {FetchRequest} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {Snitch, type SnitchMessage} from './snitch.ts';
import {consume} from './stream.ts';
import {createSource} from './test/source-factory.ts';

import {makeSourceChangeAdd} from './source.ts';
/**
 * These tests are based on join.fetch.test.ts.  Uses same cases.
 * Most of the data snapshots are the same expect for when
 * the difference between Join being left join and FlippedJoin being inner join
 * effects the data results.
 * The fetchMessages snapshots are as expected quite different.
 */

const lc = createSilentLogContext();

suite('fetch one:many', () => {
  const base = {
    columns: [
      {id: {type: 'string'}},
      {id: {type: 'string'}, issueID: {type: 'string'}},
    ],
    primaryKeys: [['id'], ['id']],
    joins: [
      {
        parentKey: ['id'],
        childKey: ['issueID'],
        relationshipName: 'comments',
      },
    ],
  } as const;

  test('no data', () => {
    const results = fetchTest({
      ...base,
      sources: [[], []],
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "1",
          "fetch",
          {},
        ],
      ]
    `);
  });

  test('no parent', () => {
    const results = fetchTest({
      ...base,
      sources: [[], [{id: 'c1', issueID: 'i1'}]],
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
      ]
    `);
  });

  test('parent, no children', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1'}], []],
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "1",
          "fetch",
          {},
        ],
      ]
    `);
  });

  test('one parent, one child', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1'}], [{id: 'c1', issueID: 'i1'}]],
    });

    expect(results.data).toMatchInlineSnapshot(`
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

    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
      ]
    `);
  });

  test('one parent, wrong child', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1'}], [{id: 'c1', issueID: 'i2'}]],
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
              "id": "i2",
            },
          },
        ],
      ]
    `);
  });

  test('one parent, one child + one wrong child', () => {
    const results = fetchTest({
      ...base,
      sources: [
        [{id: 'i1'}],
        [
          {id: 'c2', issueID: 'i2'},
          {id: 'c1', issueID: 'i1'},
        ],
      ],
    });

    expect(results.data).toMatchInlineSnapshot(`
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
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
              "id": "i2",
            },
          },
        ],
      ]
    `);
  });

  test('two parents, each with two children', () => {
    const results = fetchTest({
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
    });

    expect(results.data).toMatchInlineSnapshot(`
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
                  "issueID": "i2",
                },
              },
              {
                "relationships": {},
                "row": {
                  "id": "c4",
                  "issueID": "i2",
                },
              },
            ],
          },
          "row": {
            "id": "i2",
          },
        },
      ]
    `);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
              "id": "i2",
            },
          },
        ],
      ]
    `);
  });
});

suite('fetch many:one', () => {
  const base = {
    columns: [
      {id: {type: 'string'}, ownerID: {type: 'string'}},
      {id: {type: 'string'}},
    ],
    primaryKeys: [['id'], ['id']],
    joins: [
      {
        parentKey: ['ownerID'],
        childKey: ['id'],
        relationshipName: 'owner',
      },
    ],
  } as const;

  test('no data', () => {
    const results = fetchTest({
      ...base,
      sources: [[], []],
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "1",
          "fetch",
          {},
        ],
      ]
    `);
  });

  test('one parent, no child', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1', ownerID: 'u1'}], []],
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "1",
          "fetch",
          {},
        ],
      ]
    `);
  });

  test('no parent, one child', () => {
    const results = fetchTest({
      ...base,
      sources: [[], [{id: 'u1'}]],
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
              "ownerID": "u1",
            },
          },
        ],
      ]
    `);
  });

  test('one parent, one child', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1', ownerID: 'u1'}], [{id: 'u1'}]],
    });

    expect(results.data).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "owner": [
              {
                "relationships": {},
                "row": {
                  "id": "u1",
                },
              },
            ],
          },
          "row": {
            "id": "i1",
            "ownerID": "u1",
          },
        },
      ]
    `);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
              "ownerID": "u1",
            },
          },
        ],
      ]
    `);
  });

  test('two parents, one child', () => {
    const results = fetchTest({
      ...base,
      sources: [
        [
          {id: 'i2', ownerID: 'u1'},
          {id: 'i1', ownerID: 'u1'},
        ],
        [{id: 'u1'}],
      ],
    });

    expect(results.data).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "owner": [
              {
                "relationships": {},
                "row": {
                  "id": "u1",
                },
              },
            ],
          },
          "row": {
            "id": "i1",
            "ownerID": "u1",
          },
        },
        {
          "relationships": {
            "owner": [
              {
                "relationships": {},
                "row": {
                  "id": "u1",
                },
              },
            ],
          },
          "row": {
            "id": "i2",
            "ownerID": "u1",
          },
        },
      ]
    `);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
              "ownerID": "u1",
            },
          },
        ],
      ]
    `);
  });

  test('two parents, two children', () => {
    const results = fetchTest({
      ...base,
      sources: [
        [
          {id: 'i2', ownerID: 'u2'},
          {id: 'i1', ownerID: 'u1'},
        ],
        [{id: 'u2'}, {id: 'u1'}],
      ],
    });

    expect(results.data).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "owner": [
              {
                "relationships": {},
                "row": {
                  "id": "u1",
                },
              },
            ],
          },
          "row": {
            "id": "i1",
            "ownerID": "u1",
          },
        },
        {
          "relationships": {
            "owner": [
              {
                "relationships": {},
                "row": {
                  "id": "u2",
                },
              },
            ],
          },
          "row": {
            "id": "i2",
            "ownerID": "u2",
          },
        },
      ]
    `);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
              "ownerID": "u1",
            },
          },
        ],
        [
          "0",
          "fetch",
          {
            "constraint": {
              "ownerID": "u2",
            },
          },
        ],
      ]
    `);
  });
});

suite('fetch one:many:many', () => {
  const base = {
    columns: [
      {id: {type: 'string'}},
      {id: {type: 'string'}, issueID: {type: 'string'}},
      {id: {type: 'string'}, commentID: {type: 'string'}},
    ],
    primaryKeys: [['id'], ['id'], ['id']],
    joins: [
      {
        parentKey: ['id'],
        childKey: ['issueID'],
        relationshipName: 'comments',
      },
      {
        parentKey: ['id'],
        childKey: ['commentID'],
        relationshipName: 'revisions',
      },
    ],
  } as const;

  test('no data', () => {
    const results = fetchTest({
      ...base,
      sources: [[], [], []],
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "2",
          "fetch",
          {},
        ],
      ]
    `);
  });

  test('no parent, one comment, no revision', () => {
    const results = fetchTest({
      ...base,
      sources: [[], [{id: 'c1', issueID: 'i1'}], []],
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "2",
          "fetch",
          {},
        ],
      ]
    `);
  });

  test('no parent, one comment, one revision', () => {
    const results = fetchTest({
      ...base,
      sources: [[], [{id: 'c1', issueID: 'i1'}], [{id: 'r1', commentID: 'c1'}]],
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "2",
          "fetch",
          {},
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "id": "c1",
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
      ]
    `);
  });

  test('one issue, no comments or revisions', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1'}], [], []],
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "2",
          "fetch",
          {},
        ],
      ]
    `);
  });

  test('one issue, one comment, one revision', () => {
    const results = fetchTest({
      ...base,
      sources: [
        [{id: 'i1'}],
        [{id: 'c1', issueID: 'i1'}],
        [{id: 'r1', commentID: 'c1'}],
      ],
    });

    expect(results.data).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "comments": [
              {
                "relationships": {
                  "revisions": [
                    {
                      "relationships": {},
                      "row": {
                        "commentID": "c1",
                        "id": "r1",
                      },
                    },
                  ],
                },
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
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "2",
          "fetch",
          {},
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "id": "c1",
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
      ]
    `);
  });

  test('two issues, four comments, eight revisions', () => {
    const results = fetchTest({
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
    });

    expect(results.data).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "comments": [
              {
                "relationships": {
                  "revisions": [
                    {
                      "relationships": {},
                      "row": {
                        "commentID": "c1",
                        "id": "r1",
                      },
                    },
                    {
                      "relationships": {},
                      "row": {
                        "commentID": "c1",
                        "id": "r2",
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
                  "revisions": [
                    {
                      "relationships": {},
                      "row": {
                        "commentID": "c2",
                        "id": "r3",
                      },
                    },
                    {
                      "relationships": {},
                      "row": {
                        "commentID": "c2",
                        "id": "r4",
                      },
                    },
                  ],
                },
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
                "relationships": {
                  "revisions": [
                    {
                      "relationships": {},
                      "row": {
                        "commentID": "c3",
                        "id": "r5",
                      },
                    },
                    {
                      "relationships": {},
                      "row": {
                        "commentID": "c3",
                        "id": "r6",
                      },
                    },
                  ],
                },
                "row": {
                  "id": "c3",
                  "issueID": "i2",
                },
              },
              {
                "relationships": {
                  "revisions": [
                    {
                      "relationships": {},
                      "row": {
                        "commentID": "c4",
                        "id": "r7",
                      },
                    },
                    {
                      "relationships": {},
                      "row": {
                        "commentID": "c4",
                        "id": "r8",
                      },
                    },
                  ],
                },
                "row": {
                  "id": "c4",
                  "issueID": "i2",
                },
              },
            ],
          },
          "row": {
            "id": "i2",
          },
        },
      ]
    `);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "2",
          "fetch",
          {},
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "id": "c1",
            },
          },
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "id": "c2",
            },
          },
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "id": "c3",
            },
          },
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "id": "c4",
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
              "id": "i2",
            },
          },
        ],
      ]
    `);
  });
});

suite('fetch one:many:one', () => {
  const base = {
    columns: [
      {id: {type: 'string'}},
      {issueID: {type: 'string'}, labelID: {type: 'string'}},
      {id: {type: 'string'}},
    ],
    primaryKeys: [['id'], ['issueID', 'labelID'], ['id']],
    joins: [
      {
        parentKey: ['id'],
        childKey: ['issueID'],
        relationshipName: 'issuelabels',
      },
      {
        parentKey: ['labelID'],
        childKey: ['id'],
        relationshipName: 'labels',
      },
    ],
  } as const;

  const sorts = [
    undefined,
    [
      ['issueID', 'asc'],
      ['labelID', 'asc'],
    ] as const,
  ];

  test('no data', () => {
    const results = fetchTest({
      ...base,
      sources: [[], [], []],
      sorts,
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "2",
          "fetch",
          {},
        ],
      ]
    `);
  });

  test('no issues, one issuelabel, one label', () => {
    const results = fetchTest({
      ...base,
      sources: [[], [{issueID: 'i1', labelID: 'l1'}], [{id: 'l1'}]],
      sorts,
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "2",
          "fetch",
          {},
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "labelID": "l1",
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
      ]
    `);
  });

  test('one issue, no issuelabels, no labels', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1'}], [], []],
      sorts,
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "2",
          "fetch",
          {},
        ],
      ]
    `);
  });

  test('one issue, one issuelabel, no labels', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1'}], [{issueID: 'i1', labelID: 'l1'}], []],
      sorts,
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "2",
          "fetch",
          {},
        ],
      ]
    `);
  });

  test('one issue, one issuelabel, one label', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1'}], [{issueID: 'i1', labelID: 'l1'}], [{id: 'l1'}]],
      sorts,
    });

    expect(results.data).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "issuelabels": [
              {
                "relationships": {
                  "labels": [
                    {
                      "relationships": {},
                      "row": {
                        "id": "l1",
                      },
                    },
                  ],
                },
                "row": {
                  "issueID": "i1",
                  "labelID": "l1",
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
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "2",
          "fetch",
          {},
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "labelID": "l1",
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
      ]
    `);
  });

  test('one issue, two issuelabels, two labels', () => {
    const results = fetchTest({
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
    });

    expect(results.data).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "issuelabels": [
              {
                "relationships": {
                  "labels": [
                    {
                      "relationships": {},
                      "row": {
                        "id": "l1",
                      },
                    },
                  ],
                },
                "row": {
                  "issueID": "i1",
                  "labelID": "l1",
                },
              },
              {
                "relationships": {
                  "labels": [
                    {
                      "relationships": {},
                      "row": {
                        "id": "l2",
                      },
                    },
                  ],
                },
                "row": {
                  "issueID": "i1",
                  "labelID": "l2",
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
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "2",
          "fetch",
          {},
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "labelID": "l1",
            },
          },
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "labelID": "l2",
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
      ]
    `);
  });

  test('two issues, two issuelabels, two labels', () => {
    const results = fetchTest({
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
    });

    expect(results.data).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "issuelabels": [
              {
                "relationships": {
                  "labels": [
                    {
                      "relationships": {},
                      "row": {
                        "id": "l1",
                      },
                    },
                  ],
                },
                "row": {
                  "issueID": "i1",
                  "labelID": "l1",
                },
              },
              {
                "relationships": {
                  "labels": [
                    {
                      "relationships": {},
                      "row": {
                        "id": "l2",
                      },
                    },
                  ],
                },
                "row": {
                  "issueID": "i1",
                  "labelID": "l2",
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
            "issuelabels": [
              {
                "relationships": {
                  "labels": [
                    {
                      "relationships": {},
                      "row": {
                        "id": "l1",
                      },
                    },
                  ],
                },
                "row": {
                  "issueID": "i2",
                  "labelID": "l1",
                },
              },
              {
                "relationships": {
                  "labels": [
                    {
                      "relationships": {},
                      "row": {
                        "id": "l2",
                      },
                    },
                  ],
                },
                "row": {
                  "issueID": "i2",
                  "labelID": "l2",
                },
              },
            ],
          },
          "row": {
            "id": "i2",
          },
        },
      ]
    `);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "2",
          "fetch",
          {},
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "labelID": "l1",
            },
          },
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "labelID": "l2",
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
              "id": "i2",
            },
          },
        ],
      ]
    `);
  });
});

suite('compound join keys', () => {
  const base = {
    columns: [
      {
        id: {type: 'number'},
        a1: {type: 'number'},
        a2: {type: 'number'},
        a3: {type: 'number'},
      },
      {
        id: {type: 'number'},
        b1: {type: 'number'},
        b2: {type: 'number'},
        b3: {type: 'number'},
      },
    ],
    primaryKeys: [['id'], ['id']],
    joins: [
      {
        parentKey: ['a1', 'a2'],
        childKey: ['b2', 'b1'],
        relationshipName: 'ab',
      },
    ],
  } as const;

  test('no data', () => {
    const results = fetchTest({
      ...base,
      sources: [[], []],
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "1",
          "fetch",
          {},
        ],
      ]
    `);
  });

  test('no parent', () => {
    const results = fetchTest({
      ...base,
      sources: [[], [{id: 0, b1: 1, b2: 2, b3: 3}]],
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
              "a1": 2,
              "a2": 1,
            },
          },
        ],
      ]
    `);
  });

  test('parent, no children', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 0, a1: 1, a2: 2, a3: 3}], []],
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "1",
          "fetch",
          {},
        ],
      ]
    `);
  });

  test('one parent, one child', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 0, a1: 1, a2: 2, a3: 3}], [{id: 0, b1: 2, b2: 1, b3: 3}]],
    });

    expect(results.data).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "ab": [
              {
                "relationships": {},
                "row": {
                  "b1": 2,
                  "b2": 1,
                  "b3": 3,
                  "id": 0,
                },
              },
            ],
          },
          "row": {
            "a1": 1,
            "a2": 2,
            "a3": 3,
            "id": 0,
          },
        },
      ]
    `);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
              "a1": 1,
              "a2": 2,
            },
          },
        ],
      ]
    `);
  });

  test('one parent, wrong child', () => {
    const results = fetchTest({
      ...base,
      // join is on a1 = b2 and a2 = b1 so this will not match
      sources: [[{id: 0, a1: 1, a2: 2, a3: 3}], [{id: 0, b1: 1, b2: 2, b3: 3}]],
    });

    expect(results.data).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
              "a1": 2,
              "a2": 1,
            },
          },
        ],
      ]
    `);
  });

  test('one parent, one child + one wrong child', () => {
    const results = fetchTest({
      ...base,
      sources: [
        [{id: 0, a1: 1, a2: 2, a3: 3}],
        [
          {id: 0, b1: 2, b2: 1, b3: 3},
          {id: 1, b1: 4, b2: 5, b3: 6},
        ],
      ],
    });

    expect(results.data).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "ab": [
              {
                "relationships": {},
                "row": {
                  "b1": 2,
                  "b2": 1,
                  "b3": 3,
                  "id": 0,
                },
              },
            ],
          },
          "row": {
            "a1": 1,
            "a2": 2,
            "a3": 3,
            "id": 0,
          },
        },
      ]
    `);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
              "a1": 1,
              "a2": 2,
            },
          },
        ],
        [
          "0",
          "fetch",
          {
            "constraint": {
              "a1": 5,
              "a2": 4,
            },
          },
        ],
      ]
    `);
  });

  test('two parents, each with two children', () => {
    const results = fetchTest({
      ...base,
      sources: [
        [
          {id: 0, a1: 1, a2: 2, a3: 3},
          {id: 1, a1: 4, a2: 5, a3: 6},
        ],
        [
          {id: 0, b1: 2, b2: 1, b3: 3},
          {id: 1, b1: 2, b2: 1, b3: 4},
          {id: 2, b1: 5, b2: 4, b3: 6},
          {id: 3, b1: 5, b2: 4, b3: 7},
        ],
      ],
    });

    expect(results.data).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "ab": [
              {
                "relationships": {},
                "row": {
                  "b1": 2,
                  "b2": 1,
                  "b3": 3,
                  "id": 0,
                },
              },
              {
                "relationships": {},
                "row": {
                  "b1": 2,
                  "b2": 1,
                  "b3": 4,
                  "id": 1,
                },
              },
            ],
          },
          "row": {
            "a1": 1,
            "a2": 2,
            "a3": 3,
            "id": 0,
          },
        },
        {
          "relationships": {
            "ab": [
              {
                "relationships": {},
                "row": {
                  "b1": 5,
                  "b2": 4,
                  "b3": 6,
                  "id": 2,
                },
              },
              {
                "relationships": {},
                "row": {
                  "b1": 5,
                  "b2": 4,
                  "b3": 7,
                  "id": 3,
                },
              },
            ],
          },
          "row": {
            "a1": 4,
            "a2": 5,
            "a3": 6,
            "id": 1,
          },
        },
      ]
    `);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
              "a1": 1,
              "a2": 2,
            },
          },
        ],
        [
          "0",
          "fetch",
          {
            "constraint": {
              "a1": 4,
              "a2": 5,
            },
          },
        ],
      ]
    `);
  });

  // Three children sharing one parent-key tuple collapse to a single
  // parent fetch (not three).  Children appear in the relationship in
  // their original input order, since #fetchMergeSort appends to the
  // per-key index list in iteration order.
  test('one parent, three children sharing a parent-key', () => {
    const results = fetchTest({
      ...base,
      sources: [
        [{id: 0, a1: 1, a2: 2, a3: 3}],
        [
          {id: 10, b1: 2, b2: 1, b3: 100},
          {id: 11, b1: 2, b2: 1, b3: 101},
          {id: 12, b1: 2, b2: 1, b3: 102},
        ],
      ],
    });

    expect(results.data).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "ab": [
              {
                "relationships": {},
                "row": {
                  "b1": 2,
                  "b2": 1,
                  "b3": 100,
                  "id": 10,
                },
              },
              {
                "relationships": {},
                "row": {
                  "b1": 2,
                  "b2": 1,
                  "b3": 101,
                  "id": 11,
                },
              },
              {
                "relationships": {},
                "row": {
                  "b1": 2,
                  "b2": 1,
                  "b3": 102,
                  "id": 12,
                },
              },
            ],
          },
          "row": {
            "a1": 1,
            "a2": 2,
            "a3": 3,
            "id": 0,
          },
        },
      ]
    `);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
              "a1": 1,
              "a2": 2,
            },
          },
        ],
      ]
    `);
  });

  // Asymmetric multiplicities: three children share parent-key {a1:1,a2:2}
  // and one child has parent-key {a1:4,a2:5}.  Two distinct fetches, and
  // the larger group preserves input order.
  test('two parents with asymmetric child multiplicities', () => {
    const results = fetchTest({
      ...base,
      sources: [
        [
          {id: 0, a1: 1, a2: 2, a3: 3},
          {id: 1, a1: 4, a2: 5, a3: 6},
        ],
        [
          {id: 10, b1: 2, b2: 1, b3: 100},
          {id: 11, b1: 5, b2: 4, b3: 200},
          {id: 12, b1: 2, b2: 1, b3: 101},
          {id: 13, b1: 2, b2: 1, b3: 102},
        ],
      ],
    });

    expect(results.data).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "ab": [
              {
                "relationships": {},
                "row": {
                  "b1": 2,
                  "b2": 1,
                  "b3": 100,
                  "id": 10,
                },
              },
              {
                "relationships": {},
                "row": {
                  "b1": 2,
                  "b2": 1,
                  "b3": 101,
                  "id": 12,
                },
              },
              {
                "relationships": {},
                "row": {
                  "b1": 2,
                  "b2": 1,
                  "b3": 102,
                  "id": 13,
                },
              },
            ],
          },
          "row": {
            "a1": 1,
            "a2": 2,
            "a3": 3,
            "id": 0,
          },
        },
        {
          "relationships": {
            "ab": [
              {
                "relationships": {},
                "row": {
                  "b1": 5,
                  "b2": 4,
                  "b3": 200,
                  "id": 11,
                },
              },
            ],
          },
          "row": {
            "a1": 4,
            "a2": 5,
            "a3": 6,
            "id": 1,
          },
        },
      ]
    `);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
              "a1": 1,
              "a2": 2,
            },
          },
        ],
        [
          "0",
          "fetch",
          {
            "constraint": {
              "a1": 4,
              "a2": 5,
            },
          },
        ],
      ]
    `);
  });
});

suite('canonicalKey', () => {
  // canonicalKey tags each value by type so two different values can't
  // collide on the same key string.  The dedup map in #fetchMergeSort
  // depends on this — without tagging, e.g. `1` (number) and `"1"`
  // (string) would group together, and the wrong children would be
  // attached to a parent row.
  test('distinguishes number from string with same printed form', () => {
    expect(canonicalKey({k: 1}, ['k'])).not.toBe(canonicalKey({k: '1'}, ['k']));
  });

  test('distinguishes boolean true/false from strings "t"/"f"', () => {
    expect(canonicalKey({k: true}, ['k'])).not.toBe(
      canonicalKey({k: 't'}, ['k']),
    );
    expect(canonicalKey({k: false}, ['k'])).not.toBe(
      canonicalKey({k: 'f'}, ['k']),
    );
  });

  test('distinguishes null/undefined from string "n"', () => {
    expect(canonicalKey({k: null}, ['k'])).not.toBe(
      canonicalKey({k: 'n'}, ['k']),
    );
    expect(canonicalKey({k: undefined}, ['k'])).not.toBe(
      canonicalKey({k: 'n'}, ['k']),
    );
  });

  test('treats null and undefined as the same value', () => {
    // buildJoinConstraint produces `undefined` for missing-but-nullable
    // join columns; the source-side constraint shape uses `null`. They
    // must canonicalize identically so dedup groups them together.
    expect(canonicalKey({k: null}, ['k'])).toBe(
      canonicalKey({k: undefined}, ['k']),
    );
  });

  test('distinguishes bigint from number with same numeric value', () => {
    // safeIntegers in zqlite produces bigint at runtime even though the
    // static Value type doesn't list it.
    expect(canonicalKey({k: 1n as unknown as Value}, ['k'])).not.toBe(
      canonicalKey({k: 1}, ['k']),
    );
  });

  test('compound key separator avoids collisions across boundaries', () => {
    // Without a separator, ('ab','c') and ('a','bc') would canonicalize
    // identically. The \x00 delimiter keeps the boundaries distinct for
    // typical id-shaped values.
    expect(canonicalKey({a: 'ab', b: 'c'}, ['a', 'b'])).not.toBe(
      canonicalKey({a: 'a', b: 'bc'}, ['a', 'b']),
    );
  });

  test('equal compound tuples produce equal keys', () => {
    expect(canonicalKey({a: 1, b: 'x'}, ['a', 'b'])).toBe(
      canonicalKey({a: 1, b: 'x'}, ['a', 'b']),
    );
  });

  test('JSON-encoded fallback for object/array values', () => {
    expect(canonicalKey({k: {a: 1} as unknown as Value}, ['k'])).not.toBe(
      canonicalKey({k: {a: 2} as unknown as Value}, ['k']),
    );
    expect(canonicalKey({k: {a: 1} as unknown as Value}, ['k'])).toBe(
      canonicalKey({k: {a: 1} as unknown as Value}, ['k']),
    );
  });
});

suite('fetch one:many with FetchRequest', () => {
  const base = {
    columns: [
      {id: {type: 'string'}},
      {id: {type: 'string'}, issueID: {type: 'string'}},
    ],
    primaryKeys: [['id'], ['id']],
    joins: [
      {
        parentKey: ['id'],
        childKey: ['issueID'],
        relationshipName: 'comments',
      },
    ],
  } as const;
  const sources: Row[][] = [
    [{id: 'i1'}, {id: 'i2'}, {id: 'i3'}],
    [
      {id: 'c1', issueID: 'i1'},
      {id: 'c2', issueID: 'i2'},
      {id: 'c3', issueID: 'i3'},
    ],
  ];

  test('reverse: true yields parents in descending order', () => {
    const results = fetchTest({
      ...base,
      sources,
      fetchRequest: {reverse: true},
    });

    expect(results.data.map(n => (n as CaughtNode & {row: Row}).row.id))
      .toMatchInlineSnapshot(`
      [
        "i3",
        "i2",
        "i1",
      ]
    `);
  });

  test('start basis "at" includes the start row (forward)', () => {
    const results = fetchTest({
      ...base,
      sources,
      fetchRequest: {start: {row: {id: 'i2'}, basis: 'at'}},
    });

    expect(results.data.map(n => (n as CaughtNode & {row: Row}).row.id))
      .toMatchInlineSnapshot(`
      [
        "i2",
        "i3",
      ]
    `);
  });

  test('start basis "after" excludes the start row (forward)', () => {
    const results = fetchTest({
      ...base,
      sources,
      fetchRequest: {start: {row: {id: 'i2'}, basis: 'after'}},
    });

    expect(results.data.map(n => (n as CaughtNode & {row: Row}).row.id))
      .toMatchInlineSnapshot(`
      [
        "i3",
      ]
    `);
  });

  test('start basis "at" with reverse', () => {
    const results = fetchTest({
      ...base,
      sources,
      fetchRequest: {start: {row: {id: 'i2'}, basis: 'at'}, reverse: true},
    });

    expect(results.data.map(n => (n as CaughtNode & {row: Row}).row.id))
      .toMatchInlineSnapshot(`
      [
        "i2",
        "i1",
      ]
    `);
  });

  test('start basis "after" with reverse', () => {
    const results = fetchTest({
      ...base,
      sources,
      fetchRequest: {start: {row: {id: 'i2'}, basis: 'after'}, reverse: true},
    });

    expect(results.data.map(n => (n as CaughtNode & {row: Row}).row.id))
      .toMatchInlineSnapshot(`
      [
        "i1",
      ]
    `);
  });
});

suite('fetch with compound primary key === parentKey', () => {
  // Parent PK is ['orgId', 'slug'].  Children share parents:
  // m1 references org=o1/slug=s1; m2 references org=o1/slug=s1; m3 references
  // org=o2/slug=s2.  So parent (o1, s1) has two related children.
  const base = {
    columns: [
      {orgId: {type: 'string'}, slug: {type: 'string'}, name: {type: 'string'}},
      {
        id: {type: 'string'},
        memberOrgId: {type: 'string'},
        memberSlug: {type: 'string'},
      },
    ],
    primaryKeys: [['orgId', 'slug'], ['id']],
    joins: [
      {
        parentKey: ['orgId', 'slug'],
        childKey: ['memberOrgId', 'memberSlug'],
        relationshipName: 'members',
      },
    ],
  } as const;
  const sorts: Ordering[] = [
    [
      ['orgId', 'asc'],
      ['slug', 'asc'],
    ],
    [['id', 'asc']],
  ];

  test('two parents, one with multiple children', () => {
    const results = fetchTest({
      ...base,
      sorts,
      sources: [
        [
          {orgId: 'o1', slug: 's1', name: 'Org One'},
          {orgId: 'o2', slug: 's2', name: 'Org Two'},
        ],
        [
          {id: 'm1', memberOrgId: 'o1', memberSlug: 's1'},
          {id: 'm2', memberOrgId: 'o1', memberSlug: 's1'},
          {id: 'm3', memberOrgId: 'o2', memberSlug: 's2'},
        ],
      ],
    });

    expect(results.data).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "members": [
              {
                "relationships": {},
                "row": {
                  "id": "m1",
                  "memberOrgId": "o1",
                  "memberSlug": "s1",
                },
              },
              {
                "relationships": {},
                "row": {
                  "id": "m2",
                  "memberOrgId": "o1",
                  "memberSlug": "s1",
                },
              },
            ],
          },
          "row": {
            "name": "Org One",
            "orgId": "o1",
            "slug": "s1",
          },
        },
        {
          "relationships": {
            "members": [
              {
                "relationships": {},
                "row": {
                  "id": "m3",
                  "memberOrgId": "o2",
                  "memberSlug": "s2",
                },
              },
            ],
          },
          "row": {
            "name": "Org Two",
            "orgId": "o2",
            "slug": "s2",
          },
        },
      ]
    `);
  });
});

// Exercises the heap-based K-way merge in #fetchMergeSort with large K
// (other suites cover K=1..2) and reverse: true, which has no other
// coverage.
suite('merge-sort: large K and reverse', () => {
  const base = {
    columns: [
      {id: {type: 'number'}, groupId: {type: 'number'}},
      {id: {type: 'number'}, parentGroupId: {type: 'number'}},
    ],
    primaryKeys: [['id'], ['id']],
    joins: [
      {
        parentKey: ['groupId'],
        childKey: ['parentGroupId'],
        relationshipName: 'children',
      },
    ],
  } as const;

  // Parent ids are assigned round-robin across groups so each per-group
  // cursor's rows are interleaved with every other cursor's rows in the
  // global id ordering. The heap has to compare-and-pop across all K
  // streams throughout, not just within a contiguous run per cursor.
  function makeParents(numGroups: number, perGroup: number): Row[] {
    const out: Row[] = [];
    for (let p = 0; p < perGroup; p++) {
      for (let g = 0; g < numGroups; g++) {
        out.push({id: p * numGroups + g, groupId: g});
      }
    }
    return out;
  }
  function makeChildren(numGroups: number): Row[] {
    return Array.from({length: numGroups}, (_, g) => ({
      id: 1000 + g,
      parentGroupId: g,
    }));
  }

  test('reverse: true with K=2 streams emits parents in descending order', () => {
    const results = fetchTest({
      ...base,
      sources: [makeParents(2, 3), makeChildren(2)],
      fetchRequest: {reverse: true},
    });

    expect(
      results.data.map(n => (n as CaughtNode & {row: Row}).row.id),
    ).toEqual([5, 4, 3, 2, 1, 0]);
  });

  test('K=10 streams interleave correctly under heap merge (forward)', () => {
    const results = fetchTest({
      ...base,
      sources: [makeParents(10, 5), makeChildren(10)],
    });

    expect(
      results.data.map(n => (n as CaughtNode & {row: Row}).row.id),
    ).toEqual(Array.from({length: 50}, (_, i) => i));
  });

  test('K=10 streams with reverse: true emit in descending order', () => {
    const results = fetchTest({
      ...base,
      sources: [makeParents(10, 5), makeChildren(10)],
      fetchRequest: {reverse: true},
    });

    expect(
      results.data.map(n => (n as CaughtNode & {row: Row}).row.id),
    ).toEqual(Array.from({length: 50}, (_, i) => 49 - i));
  });

  // Combines dedup-by-canonical-key with the heap merge: 80 child nodes
  // collapse to K=20 unique cursors, each returning 5 parents. Verifies
  // both the global ordering and that every emitted parent gets all four
  // children sharing its groupId attached (dedup map → children).
  test('K=20 with shared parent-keys exercises dedup → heap together', () => {
    const numGroups = 20;
    const perGroup = 5;
    const childrenPerGroup = 4;
    const parents = makeParents(numGroups, perGroup);
    const children: Row[] = [];
    for (let g = 0; g < numGroups; g++) {
      for (let dup = 0; dup < childrenPerGroup; dup++) {
        children.push({
          id: 1000 + g * childrenPerGroup + dup,
          parentGroupId: g,
        });
      }
    }
    const results = fetchTest({
      ...base,
      sources: [parents, children],
    });

    expect(
      results.data.map(n => (n as CaughtNode & {row: Row}).row.id),
    ).toEqual(Array.from({length: numGroups * perGroup}, (_, i) => i));

    for (const node of results.data) {
      const n = node as CaughtNode & {
        row: Row;
        relationships: Record<string, CaughtNode[]>;
      };
      expect(n.relationships.children).toHaveLength(childrenPerGroup);
    }
  });
});

function fetchTest(t: FetchTest): FetchTestResults {
  assert(t.sources.length > 0, 'Expected at least one source');
  assert(
    t.joins.length === t.sources.length - 1,
    'Expected joins.length to equal sources.length - 1',
  );

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
      consume(source.push(makeSourceChangeAdd(row)));
    }
    const snitch = new Snitch(source.connect(ordering), String(i), log);
    return {
      source,
      snitch,
    };
  });

  const flippedJoins: FlippedJoin[] = [];
  // Although we tend to think of the joins from left to right, we need to
  // build them from right to left.
  for (let i = t.joins.length - 1; i >= 0; i--) {
    const info = t.joins[i];
    const parent = sources[i].snitch;
    const child =
      i === t.joins.length - 1 ? sources[i + 1].snitch : flippedJoins[i + 1];
    flippedJoins[i] = new FlippedJoin({
      parent,
      child,
      parentKey: info.parentKey,
      childKey: info.childKey,
      relationshipName: info.relationshipName,
      hidden: false,
      system: 'client',
    });
  }

  const results: FetchTestResults = {
    data: [],
    fetchMessages: [],
  };

  // By convention we put them in the test bottom up. Why? Easier to think
  // left-to-right.
  const finalJoin = flippedJoins[0];

  let expectedSchema: SourceSchema | undefined;
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
  expect(finalJoin.getSchema()).toStrictEqual(expectedSchema);

  const c = new Catch(finalJoin);
  const r = c.fetch(t.fetchRequest);

  results.data = r;
  expect(c.pushes).toEqual([]);
  results.fetchMessages = [...log];

  return results;
}

type FetchTest = {
  columns: readonly Record<string, SchemaValue>[];
  primaryKeys: readonly PrimaryKey[];
  sources: Row[][];
  sorts?: (Ordering | undefined)[] | undefined;
  joins: readonly {
    parentKey: CompoundKey;
    childKey: CompoundKey;
    relationshipName: string;
  }[];
  fetchRequest?: FetchRequest | undefined;
};

type FetchTestResults = {
  fetchMessages: SnitchMessage[];
  data: CaughtNode[];
};
