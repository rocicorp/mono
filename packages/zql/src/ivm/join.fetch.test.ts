import {expect, suite, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import type {CompoundKey, Ordering} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.ts';
import {Catch, type CaughtNode} from './catch.ts';
import {Join} from './join.ts';
import {MemoryStorage} from './memory-storage.ts';
import type {SourceSchema} from './schema.ts';
import {Snitch, type SnitchMessage} from './snitch.ts';
import {createSource} from './test/source-factory.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {consume} from './stream.ts';

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

    expect(results.hydrate).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
      ]
    `);
  });

  test('no parent', () => {
    const results = fetchTest({
      ...base,
      sources: [[], [{id: 'c1', issueID: 'i1'}]],
    });

    expect(results.hydrate).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
      ]
    `);
  });

  test('parent, no children', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1'}], []],
    });

    expect(results.hydrate).toMatchInlineSnapshot(`
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
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
      ]
    `);
  });

  test('one parent, one child', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1'}], [{id: 'c1', issueID: 'i1'}]],
    });

    expect(results.hydrate).toMatchInlineSnapshot(`
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
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
      ]
    `);
  });

  test('one parent, wrong child', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1'}], [{id: 'c1', issueID: 'i2'}]],
    });

    expect(results.hydrate).toMatchInlineSnapshot(`
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
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
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

    expect(results.hydrate).toMatchInlineSnapshot(`
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
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
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

    expect(results.hydrate).toMatchInlineSnapshot(`
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
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
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

    expect(results.hydrate).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
      ]
    `);
  });

  test('one parent, no child', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1', ownerID: 'u1'}], []],
    });

    expect(results.hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "owner": [],
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
          "0",
          "fetch",
          {},
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "id": "u1",
            },
          },
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
      ]
    `);
  });

  test('no parent, one child', () => {
    const results = fetchTest({
      ...base,
      sources: [[], [{id: 'u1'}]],
    });

    expect(results.hydrate).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
      ]
    `);
  });

  test('one parent, one child', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1', ownerID: 'u1'}], [{id: 'u1'}]],
    });

    expect(results.hydrate).toMatchInlineSnapshot(`
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
          "0",
          "fetch",
          {},
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "id": "u1",
            },
          },
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
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

    expect(results.hydrate).toMatchInlineSnapshot(`
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
          "0",
          "fetch",
          {},
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "id": "u1",
            },
          },
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "id": "u1",
            },
          },
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
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

    expect(results.hydrate).toMatchInlineSnapshot(`
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
          "0",
          "fetch",
          {},
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "id": "u1",
            },
          },
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "id": "u2",
            },
          },
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
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

    expect(results.hydrate).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
        {},
      ]
    `);
  });

  test('no parent, one comment, no revision', () => {
    const results = fetchTest({
      ...base,
      sources: [[], [{id: 'c1', issueID: 'i1'}], []],
    });

    expect(results.hydrate).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
        {},
      ]
    `);
  });

  test('no parent, one comment, one revision', () => {
    const results = fetchTest({
      ...base,
      sources: [[], [{id: 'c1', issueID: 'i1'}], [{id: 'r1', commentID: 'c1'}]],
    });

    expect(results.hydrate).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
        {},
      ]
    `);
  });

  test('one issue, no comments or revisions', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1'}], [], []],
    });

    expect(results.hydrate).toMatchInlineSnapshot(`
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
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
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
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
        {},
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

    expect(results.hydrate).toMatchInlineSnapshot(`
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
          "2",
          "fetch",
          {
            "constraint": {
              "commentID": "c1",
            },
          },
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
        {},
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

    expect(results.hydrate).toMatchInlineSnapshot(`
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
          "2",
          "fetch",
          {
            "constraint": {
              "commentID": "c1",
            },
          },
        ],
        [
          "2",
          "fetch",
          {
            "constraint": {
              "commentID": "c2",
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
          "2",
          "fetch",
          {
            "constraint": {
              "commentID": "c3",
            },
          },
        ],
        [
          "2",
          "fetch",
          {
            "constraint": {
              "commentID": "c4",
            },
          },
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
        {},
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

    expect(results.hydrate).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
        {},
      ]
    `);
  });

  test('no issues, one issuelabel, one label', () => {
    const results = fetchTest({
      ...base,
      sources: [[], [{issueID: 'i1', labelID: 'l1'}], [{id: 'l1'}]],
      sorts,
    });

    expect(results.hydrate).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
        {},
      ]
    `);
  });

  test('one issue, no issuelabels, no labels', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1'}], [], []],
      sorts,
    });

    expect(results.hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "issuelabels": [],
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
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
        {},
      ]
    `);
  });

  test('one issue, one issuelabel, no labels', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1'}], [{issueID: 'i1', labelID: 'l1'}], []],
      sorts,
    });

    expect(results.hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "issuelabels": [
              {
                "relationships": {
                  "labels": [],
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
          "2",
          "fetch",
          {
            "constraint": {
              "id": "l1",
            },
          },
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
        {},
      ]
    `);
  });

  test('one issue, one issuelabel, one label', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 'i1'}], [{issueID: 'i1', labelID: 'l1'}], [{id: 'l1'}]],
      sorts,
    });

    expect(results.hydrate).toMatchInlineSnapshot(`
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
          "2",
          "fetch",
          {
            "constraint": {
              "id": "l1",
            },
          },
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
        {},
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

    expect(results.hydrate).toMatchInlineSnapshot(`
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
          "2",
          "fetch",
          {
            "constraint": {
              "id": "l1",
            },
          },
        ],
        [
          "2",
          "fetch",
          {
            "constraint": {
              "id": "l2",
            },
          },
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
        {},
      ]
    `);
  });

  test('one issue, two issuelabels, two labels', () => {
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

    expect(results.hydrate).toMatchInlineSnapshot(`
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
          "2",
          "fetch",
          {
            "constraint": {
              "id": "l1",
            },
          },
        ],
        [
          "2",
          "fetch",
          {
            "constraint": {
              "id": "l2",
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
          "2",
          "fetch",
          {
            "constraint": {
              "id": "l1",
            },
          },
        ],
        [
          "2",
          "fetch",
          {
            "constraint": {
              "id": "l2",
            },
          },
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
        {},
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

    expect(results.hydrate).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
      ]
    `);
  });

  test('no parent', () => {
    const results = fetchTest({
      ...base,
      sources: [[], [{id: 0, b1: 1, b2: 2, b3: 3}]],
    });

    expect(results.hydrate).toMatchInlineSnapshot(`[]`);
    expect(results.fetchMessages).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
      ]
    `);
  });

  test('parent, no children', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 0, a1: 1, a2: 2, a3: 3}], []],
    });

    expect(results.hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "ab": [],
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
          "0",
          "fetch",
          {},
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "b1": 2,
              "b2": 1,
            },
          },
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
      ]
    `);
  });

  test('one parent, one child', () => {
    const results = fetchTest({
      ...base,
      sources: [[{id: 0, a1: 1, a2: 2, a3: 3}], [{id: 0, b1: 2, b2: 1, b3: 3}]],
    });

    expect(results.hydrate).toMatchInlineSnapshot(`
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
          "0",
          "fetch",
          {},
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "b1": 2,
              "b2": 1,
            },
          },
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
      ]
    `);
  });

  test('one parent, wrong child', () => {
    const results = fetchTest({
      ...base,
      // join is on a1 = b2 and a2 = b1 so this will not match
      sources: [[{id: 0, a1: 1, a2: 2, a3: 3}], [{id: 0, b1: 1, b2: 2, b3: 3}]],
    });

    expect(results.hydrate).toMatchInlineSnapshot(`
      [
        {
          "relationships": {
            "ab": [],
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
          "0",
          "fetch",
          {},
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "b1": 2,
              "b2": 1,
            },
          },
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
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

    expect(results.hydrate).toMatchInlineSnapshot(`
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
          "0",
          "fetch",
          {},
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "b1": 2,
              "b2": 1,
            },
          },
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
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

    expect(results.hydrate).toMatchInlineSnapshot(`
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
          "0",
          "fetch",
          {},
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "b1": 2,
              "b2": 1,
            },
          },
        ],
        [
          "1",
          "fetch",
          {
            "constraint": {
              "b1": 5,
              "b2": 4,
            },
          },
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {},
      ]
    `);
  });
});

// Despite the name, this test runs the join through two phases:
// hydrate, fetch.
function fetchTest(t: FetchTest): FetchTestResults {
  assert(t.sources.length > 0);
  assert(t.joins.length === t.sources.length - 1);

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
    const join = new Join({
      parent,
      child,
      ...info,
      hidden: false,
      system: 'client',
    });
    joins[i] = {
      join,
      storage,
    };
  }

  const results: FetchTestResults = {
    hydrate: [],
    storage: [],
    fetchMessages: [],
  };
  for (const phase of ['hydrate', 'fetch'] as const) {
    log.length = 0;

    // By convention we put them in the test bottom up. Why? Easier to think
    // left-to-right.
    const finalJoin = joins[0];

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
    expect(finalJoin.join.getSchema()).toStrictEqual(expectedSchema);

    const c = new Catch(finalJoin.join);
    const r = c.fetch({});

    if (phase === 'hydrate') {
      results.hydrate = r;
    } else {
      expect(r).toEqual(results.hydrate);
    }
    expect(c.pushes).toEqual([]);

    for (const [i, j] of joins.entries()) {
      const {storage} = j;
      if (phase === 'hydrate') {
        results.storage[i] = storage.cloneData();
      } else {
        phase satisfies 'fetch';
        expect(storage.cloneData()).toEqual(results.storage[i]);
      }
    }

    if (phase === 'hydrate') {
      results.fetchMessages = [...log];
    } else {
      phase satisfies 'fetch';
      // should be the same as for hydrate
      expect(log).toEqual(results.fetchMessages);
    }
  }

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
};

type FetchTestResults = {
  fetchMessages: SnitchMessage[];
  hydrate: CaughtNode[];
  storage: Record<string, JSONValue>[];
};
