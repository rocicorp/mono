import {expect, suite, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.js';
import type {Ordering} from '../../../zero-protocol/src/ast.js';
import type {Row} from '../../../zero-protocol/src/data.js';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.js';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.js';
import {Catch} from './catch.js';
import type {Node} from './data.js';
import {
  Join,
  createPrimaryKeySetStorageKey,
  createPrimaryKeySetStorageKeyPrefix,
} from './join.js';
import {MemorySource} from './memory-source.js';
import {MemoryStorage} from './memory-storage.js';
import type {SourceSchema} from './schema.js';
import {Snitch, type SnitchMessage} from './snitch.js';
import type {JSONValue} from '../../../shared/src/json.js';

suite('fetch one:many', () => {
  const base = {
    columns: [
      {id: {type: 'string'}},
      {id: {type: 'string'}, issueID: {type: 'string'}},
    ],
    primaryKeys: [['id'], ['id']],
    joins: [
      {
        parentKey: 'id',
        childKey: 'issueID',
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.fetch).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
        [
          "0",
          "cleanup",
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.fetch).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
        [
          "0",
          "cleanup",
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
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
    expect(results.messages.fetch).toMatchInlineSnapshot(`
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
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
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
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {
          ""pKeySet","i1","i1",": true,
        },
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
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
    expect(results.messages.fetch).toMatchInlineSnapshot(`
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
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
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
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {
          ""pKeySet","i1","i1",": true,
        },
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
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
    expect(results.messages.fetch).toMatchInlineSnapshot(`
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
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
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
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {
          ""pKeySet","i1","i1",": true,
        },
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
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
    expect(results.messages.fetch).toMatchInlineSnapshot(`
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
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
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
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {
          ""pKeySet","i1","i1",": true,
        },
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
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
    expect(results.messages.fetch).toMatchInlineSnapshot(`
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
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
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
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {
          ""pKeySet","i1","i1",": true,
          ""pKeySet","i2","i2",": true,
        },
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
        parentKey: 'ownerID',
        childKey: 'id',
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.fetch).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
        [
          "0",
          "cleanup",
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
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
    expect(results.messages.fetch).toMatchInlineSnapshot(`
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
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
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
              "id": "u1",
            },
          },
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {
          ""pKeySet","u1","i1",": true,
        },
      ]
    `);
  });

  test('no parent, one child', () => {
    const results = fetchTest({
      ...base,
      sources: [[], [{id: 'u1'}]],
    });

    expect(results.hydrate).toMatchInlineSnapshot(`[]`);
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.fetch).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
        [
          "0",
          "cleanup",
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
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
    expect(results.messages.fetch).toMatchInlineSnapshot(`
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
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
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
              "id": "u1",
            },
          },
        ],
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {
          ""pKeySet","u1","i1",": true,
        },
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
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
    expect(results.messages.fetch).toMatchInlineSnapshot(`
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
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
        [
          "0",
          "cleanup",
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
          "cleanup",
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
        {
          ""pKeySet","u1","i1",": true,
          ""pKeySet","u1","i2",": true,
        },
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
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
    expect(results.messages.fetch).toMatchInlineSnapshot(`
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
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
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
              "id": "u1",
            },
          },
        ],
        [
          "1",
          "cleanup",
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
        {
          ""pKeySet","u1","i1",": true,
          ""pKeySet","u2","i2",": true,
        },
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
  } as const;

  test('no data', () => {
    const results = fetchTest({
      ...base,
      sources: [[], [], []],
    });

    expect(results.hydrate).toMatchInlineSnapshot(`[]`);
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.fetch).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
        [
          "0",
          "cleanup",
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.fetch).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
        [
          "0",
          "cleanup",
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.fetch).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
        [
          "0",
          "cleanup",
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
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
    expect(results.messages.fetch).toMatchInlineSnapshot(`
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
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
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
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {
          ""pKeySet","i1","i1",": true,
        },
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
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
    expect(results.messages.fetch).toMatchInlineSnapshot(`
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
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
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
          "2",
          "cleanup",
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
        {
          ""pKeySet","i1","i1",": true,
        },
        {
          ""pKeySet","c1","c1",": true,
        },
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
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
    expect(results.messages.fetch).toMatchInlineSnapshot(`
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
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
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
          "2",
          "cleanup",
          {
            "constraint": {
              "commentID": "c1",
            },
          },
        ],
        [
          "2",
          "cleanup",
          {
            "constraint": {
              "commentID": "c2",
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
          "2",
          "cleanup",
          {
            "constraint": {
              "commentID": "c3",
            },
          },
        ],
        [
          "2",
          "cleanup",
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
        {
          ""pKeySet","i1","i1",": true,
          ""pKeySet","i2","i2",": true,
        },
        {
          ""pKeySet","c1","c1",": true,
          ""pKeySet","c2","c2",": true,
          ""pKeySet","c3","c3",": true,
          ""pKeySet","c4","c4",": true,
        },
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.fetch).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
        [
          "0",
          "cleanup",
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.fetch).toMatchInlineSnapshot(`
      [
        [
          "0",
          "fetch",
          {},
        ],
      ]
    `);
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
        [
          "0",
          "cleanup",
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
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
    expect(results.messages.fetch).toMatchInlineSnapshot(`
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
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
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
      ]
    `);
    expect(results.storage).toMatchInlineSnapshot(`
      [
        {
          ""pKeySet","i1","i1",": true,
        },
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
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
    expect(results.messages.fetch).toMatchInlineSnapshot(`
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
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
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
          "2",
          "cleanup",
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
        {
          ""pKeySet","i1","i1",": true,
        },
        {
          ""pKeySet","l1","i1","l1",": true,
        },
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
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
    expect(results.messages.fetch).toMatchInlineSnapshot(`
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
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
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
          "2",
          "cleanup",
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
        {
          ""pKeySet","i1","i1",": true,
        },
        {
          ""pKeySet","l1","i1","l1",": true,
        },
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
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
    expect(results.messages.fetch).toMatchInlineSnapshot(`
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
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
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
          "2",
          "cleanup",
          {
            "constraint": {
              "id": "l1",
            },
          },
        ],
        [
          "2",
          "cleanup",
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
        {
          ""pKeySet","i1","i1",": true,
        },
        {
          ""pKeySet","l1","i1","l1",": true,
          ""pKeySet","l2","i1","l2",": true,
        },
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
    expect(results.messages.hydrate).toMatchInlineSnapshot(`
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
    expect(results.messages.fetch).toMatchInlineSnapshot(`
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
    expect(results.messages.cleanup).toMatchInlineSnapshot(`
      [
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
          "cleanup",
          {
            "constraint": {
              "issueID": "i2",
            },
          },
        ],
        [
          "2",
          "cleanup",
          {
            "constraint": {
              "id": "l1",
            },
          },
        ],
        [
          "2",
          "cleanup",
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
        {
          ""pKeySet","i1","i1",": true,
          ""pKeySet","i2","i2",": true,
        },
        {
          ""pKeySet","l1","i1","l1",": true,
          ""pKeySet","l1","i2","l1",": true,
          ""pKeySet","l2","i1","l2",": true,
          ""pKeySet","l2","i2","l2",": true,
        },
      ]
    `);
  });
});

// Despite the name, this test runs the join through all three phases:
// initial fetch, fetch, and cleanup.
function fetchTest(t: FetchTest): FetchTestResults {
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
    const join = new Join({
      parent,
      child,
      storage,
      ...info,
      hidden: false,
    });
    joins[i] = {
      join,
      storage,
    };
  }

  const results: FetchTestResults = {
    hydrate: [],
    storage: [],
    messages: {
      hydrate: [],
      fetch: [],
      cleanup: [],
    },
  };
  for (const [phase, fetchType] of [
    ['hydrate', 'fetch'],
    ['fetch', 'fetch'],
    ['cleanup', 'cleanup'],
  ] as const) {
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
    const r = c[fetchType]();

    if (phase === 'hydrate') {
      results.hydrate = r;
    }
    expect(c.pushes).toEqual([]);

    for (const [i, j] of joins.entries()) {
      const {storage} = j;
      if (phase === 'hydrate') {
        results.storage[i] = storage.cloneData();
      } else if (phase === 'fetch') {
        expect(storage.cloneData()).toEqual(results.storage[i]);
      } else {
        phase satisfies 'cleanup';
        expect(storage.cloneData()).toEqual({});
      }
    }
    results.messages[phase].push(...log);
  }

  return results;
}

type FetchTest = {
  columns: readonly Record<string, SchemaValue>[];
  primaryKeys: readonly PrimaryKey[];
  sources: Row[][];
  sorts?: (Ordering | undefined)[] | undefined;
  joins: readonly {
    parentKey: string;
    childKey: string;
    relationshipName: string;
  }[];
};

type FetchTestResults = {
  messages: {
    hydrate: SnitchMessage[];
    fetch: SnitchMessage[];
    cleanup: SnitchMessage[];
  };
  hydrate: Node[];
  storage: Record<string, JSONValue>[];
};

test('createPrimaryKeySetStorageKey', () => {
  const k123 = createPrimaryKeySetStorageKey([123, 'id1']);
  const k1234 = createPrimaryKeySetStorageKey([1234, 'id1']);

  expect(k123.startsWith(createPrimaryKeySetStorageKeyPrefix(123))).true;
  expect(k123.startsWith(createPrimaryKeySetStorageKeyPrefix(124))).false;

  expect(k1234.startsWith(createPrimaryKeySetStorageKeyPrefix(123))).false;
  expect(k1234.startsWith(createPrimaryKeySetStorageKeyPrefix(1234))).true;
});
