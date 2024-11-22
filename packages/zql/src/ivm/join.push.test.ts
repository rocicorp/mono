import {describe, expect, suite, test} from 'vitest';
import {
  pushTest,
  runJoinTest,
  type Joins,
  type Sources,
} from './test/join-push-tests.js';
import type {Format} from './view.js';

suite('push one:many', () => {
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

  pushTest({
    ...base,
    name: 'fetch one parent, remove parent',
    sources: [[{id: 'i1'}], []],
    pushes: [[0, {type: 'remove', row: {id: 'i1'}}]],
    expectedLog: [
      ['0', 'push', {type: 'remove', row: {id: 'i1'}}],
      ['1', 'cleanup', {constraint: {issueID: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[]],
    expectedOutput: [
      {
        type: 'remove',
        node: {
          row: {
            id: 'i1',
          },
          relationships: {
            comments: [],
          },
        },
      },
    ],
  });

  pushTest({
    ...base,
    name: 'fetch one child, remove child',
    sources: [[], [{id: 'c1', issueID: 'i1'}]],
    pushes: [[1, {type: 'remove', row: {id: 'c1', issueID: 'i1'}}]],
    expectedLog: [
      ['1', 'push', {type: 'remove', row: {id: 'c1', issueID: 'i1'}}],
      ['0', 'fetch', {constraint: {id: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[]],
    expectedOutput: [],
  });

  pushTest({
    ...base,
    name: 'fetch one child, add parent',
    sources: [[], [{id: 'c1', issueID: 'i1'}]],
    pushes: [[0, {type: 'add', row: {id: 'i1'}}]],
    expectedLog: [
      ['0', 'push', {type: 'add', row: {id: 'i1'}}],
      ['1', 'fetch', {constraint: {issueID: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']]],
    expectedOutput: [
      {
        type: 'add',
        node: {
          row: {
            id: 'i1',
          },
          relationships: {
            comments: [{row: {id: 'c1', issueID: 'i1'}, relationships: {}}],
          },
        },
      },
    ],
  });

  pushTest({
    ...base,
    name: 'fetch two children, add parent',
    sources: [
      [],
      [
        {id: 'c1', issueID: 'i1'},
        {id: 'c2', issueID: 'i1'},
      ],
    ],
    pushes: [[0, {type: 'add', row: {id: 'i1'}}]],
    expectedLog: [
      ['0', 'push', {type: 'add', row: {id: 'i1'}}],
      ['1', 'fetch', {constraint: {issueID: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']]],
    expectedOutput: [
      {
        type: 'add',
        node: {
          row: {
            id: 'i1',
          },
          relationships: {
            comments: [
              {row: {id: 'c1', issueID: 'i1'}, relationships: {}},
              {row: {id: 'c2', issueID: 'i1'}, relationships: {}},
            ],
          },
        },
      },
    ],
  });

  pushTest({
    ...base,
    name: 'fetch one child, add wrong parent',
    sources: [[], [{id: 'c1', issueID: 'i1'}]],
    pushes: [[0, {type: 'add', row: {id: 'i2'}}]],
    expectedLog: [
      ['0', 'push', {type: 'add', row: {id: 'i2'}}],
      ['1', 'fetch', {constraint: {issueID: 'i2'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i2', 'i2']]],
    expectedOutput: [
      {
        type: 'add',
        node: {
          row: {
            id: 'i2',
          },
          relationships: {
            comments: [],
          },
        },
      },
    ],
  });

  pushTest({
    ...base,
    name: 'fetch one parent, add child',
    sources: [[{id: 'i1'}], []],
    pushes: [[1, {type: 'add', row: {id: 'c1', issueID: 'i1'}}]],
    expectedLog: [
      ['1', 'push', {type: 'add', row: {id: 'c1', issueID: 'i1'}}],
      ['0', 'fetch', {constraint: {id: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']]],
    expectedOutput: [
      {
        type: 'child',
        row: {
          id: 'i1',
        },
        child: {
          relationshipName: 'comments',
          change: {
            type: 'add',
            node: {
              row: {
                id: 'c1',
                issueID: 'i1',
              },
              relationships: {},
            },
          },
        },
      },
    ],
  });

  pushTest({
    ...base,
    name: 'fetch one parent, add wrong child',
    sources: [[{id: 'i1'}], []],
    pushes: [[1, {type: 'add', row: {id: 'c1', issueID: 'i2'}}]],
    expectedLog: [
      ['1', 'push', {type: 'add', row: {id: 'c1', issueID: 'i2'}}],
      ['0', 'fetch', {constraint: {id: 'i2'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']]],
    expectedOutput: [],
  });

  pushTest({
    ...base,
    name: 'fetch one parent, one child, remove parent',
    sources: [[{id: 'i1'}], [{id: 'c1', issueID: 'i1'}]],
    pushes: [[0, {type: 'remove', row: {id: 'i1'}}]],
    expectedLog: [
      ['0', 'push', {type: 'remove', row: {id: 'i1'}}],
      ['1', 'cleanup', {constraint: {issueID: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[]],
    expectedOutput: [
      {
        type: 'remove',
        node: {
          row: {
            id: 'i1',
          },
          relationships: {
            comments: [{row: {id: 'c1', issueID: 'i1'}, relationships: {}}],
          },
        },
      },
    ],
  });

  pushTest({
    ...base,
    name: 'fetch one parent, two children, remove parent',
    sources: [
      [{id: 'i1'}],
      [
        {id: 'c1', issueID: 'i1'},
        {id: 'c2', issueID: 'i1'},
      ],
    ],
    pushes: [[0, {type: 'remove', row: {id: 'i1'}}]],
    expectedLog: [
      ['0', 'push', {type: 'remove', row: {id: 'i1'}}],
      ['1', 'cleanup', {constraint: {issueID: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[]],
    expectedOutput: [
      {
        type: 'remove',
        node: {
          row: {
            id: 'i1',
          },
          relationships: {
            comments: [
              {row: {id: 'c1', issueID: 'i1'}, relationships: {}},
              {row: {id: 'c2', issueID: 'i1'}, relationships: {}},
            ],
          },
        },
      },
    ],
  });

  pushTest({
    ...base,
    name: 'no fetch, add parent, add child, add child, remove child, remove parent',
    sources: [[], []],
    pushes: [
      [0, {type: 'add', row: {id: 'i1'}}],
      [1, {type: 'add', row: {id: 'c1', issueID: 'i1'}}],
      [1, {type: 'add', row: {id: 'c2', issueID: 'i1'}}],
      [1, {type: 'remove', row: {id: 'c1', issueID: 'i1'}}],
      [0, {type: 'remove', row: {id: 'i1'}}],
    ],
    expectedLog: [
      ['0', 'push', {type: 'add', row: {id: 'i1'}}],
      ['1', 'fetch', {constraint: {issueID: 'i1'}}],
      ['1', 'push', {type: 'add', row: {id: 'c1', issueID: 'i1'}}],
      ['0', 'fetch', {constraint: {id: 'i1'}}],
      ['1', 'push', {type: 'add', row: {id: 'c2', issueID: 'i1'}}],
      ['0', 'fetch', {constraint: {id: 'i1'}}],
      ['1', 'push', {type: 'remove', row: {id: 'c1', issueID: 'i1'}}],
      ['0', 'fetch', {constraint: {id: 'i1'}}],
      ['0', 'push', {type: 'remove', row: {id: 'i1'}}],
      ['1', 'cleanup', {constraint: {issueID: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[], []],
    expectedOutput: [
      {
        type: 'add',
        node: {
          row: {
            id: 'i1',
          },
          relationships: {
            comments: [],
          },
        },
      },
      {
        type: 'child',
        row: {
          id: 'i1',
        },
        child: {
          relationshipName: 'comments',
          change: {
            type: 'add',
            node: {
              row: {
                id: 'c1',
                issueID: 'i1',
              },
              relationships: {},
            },
          },
        },
      },
      {
        type: 'child',
        row: {
          id: 'i1',
        },
        child: {
          relationshipName: 'comments',
          change: {
            type: 'add',
            node: {
              row: {
                id: 'c2',
                issueID: 'i1',
              },
              relationships: {},
            },
          },
        },
      },
      {
        type: 'child',
        row: {
          id: 'i1',
        },
        child: {
          relationshipName: 'comments',
          change: {
            type: 'remove',
            node: {
              row: {
                id: 'c1',
                issueID: 'i1',
              },
              relationships: {},
            },
          },
        },
      },
      {
        type: 'remove',
        node: {
          row: {
            id: 'i1',
          },
          relationships: {
            comments: [{row: {id: 'c2', issueID: 'i1'}, relationships: {}}],
          },
        },
      },
    ],
  });

  suite('edit', () => {
    const base = {
      columns: [
        {id: {type: 'string'}, text: {type: 'string'}},
        {
          id: {type: 'string'},
          issueID: {type: 'string'},
          text: {type: 'string'},
        },
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

    pushTest({
      ...base,
      name: 'edit issue text',
      sources: [
        [{id: 'i1', text: 'issue 1'}],
        [
          {id: 'c1', issueID: 'i1', text: 'comment 1'},
          {id: 'c2', issueID: 'i1', text: 'comment 2'},
        ],
      ],
      pushes: [
        [
          0,
          {
            type: 'edit',
            oldRow: {id: 'i1', text: 'issue 1'},
            row: {id: 'i1', text: 'issue 1 edited'},
          },
        ],
      ],
      expectedLog: [
        [
          '0',
          'push',
          {
            type: 'edit',
            oldRow: {id: 'i1', text: 'issue 1'},
            row: {id: 'i1', text: 'issue 1 edited'},
          },
        ],
        [
          '1',
          'cleanup',
          {
            constraint: {
              issueID: 'i1',
            },
          },
        ],
        [
          '1',
          'fetch',
          {
            constraint: {
              issueID: 'i1',
            },
          },
        ],
      ],
      expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']]],
      expectedOutput: [
        {
          type: 'edit',
          row: {
            id: 'i1',
            text: 'issue 1 edited',
          },
          oldRow: {
            id: 'i1',
            text: 'issue 1',
          },
        },
      ],
    });

    pushTest({
      ...base,
      name: 'edit comment text',
      sources: [
        [{id: 'i1', text: 'issue 1'}],
        [
          {id: 'c1', issueID: 'i1', text: 'comment 1'},
          {id: 'c2', issueID: 'i1', text: 'comment 2'},
        ],
      ],
      pushes: [
        [
          1,
          {
            type: 'edit',
            oldRow: {id: 'c1', issueID: 'i1', text: 'comment 1'},
            row: {id: 'c1', issueID: 'i1', text: 'comment 1 edited'},
          },
        ],
      ],
      expectedLog: [
        [
          '1',
          'push',
          {
            type: 'edit',
            oldRow: {id: 'c1', issueID: 'i1', text: 'comment 1'},
            row: {id: 'c1', issueID: 'i1', text: 'comment 1 edited'},
          },
        ],
        ['0', 'fetch', {constraint: {id: 'i1'}}],
      ],
      expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']]],
      expectedOutput: [
        {
          type: 'child',
          row: {
            id: 'i1',
            text: 'issue 1',
          },
          child: {
            change: {
              oldRow: {
                id: 'c1',
                issueID: 'i1',
                text: 'comment 1',
              },
              row: {
                id: 'c1',
                issueID: 'i1',
                text: 'comment 1 edited',
              },
              type: 'edit',
            },
            relationshipName: 'comments',
          },
        },
      ],
    });

    pushTest({
      ...base,
      name: 'edit issueID of comment',
      sources: [
        [
          {id: 'i1', text: 'issue 1'},
          {id: 'i2', text: 'issue 2'},
        ],
        [
          {id: 'c1', issueID: 'i1', text: 'comment 1'},
          {id: 'c2', issueID: 'i1', text: 'comment 2'},
        ],
      ],
      pushes: [
        [
          1,
          {
            type: 'edit',
            oldRow: {id: 'c1', issueID: 'i1', text: 'comment 1'},
            row: {id: 'c1', issueID: 'i2', text: 'comment 1.2'},
          },
        ],
      ],
      expectedLog: [
        [
          '1',
          'push',
          {
            type: 'edit',
            oldRow: {id: 'c1', issueID: 'i1', text: 'comment 1'},
            row: {id: 'c1', issueID: 'i2', text: 'comment 1.2'},
          },
        ],
        ['0', 'fetch', {constraint: {id: 'i1'}}],
        ['0', 'fetch', {constraint: {id: 'i2'}}],
      ],
      expectedPrimaryKeySetStorageKeys: [
        [
          ['i1', 'i1'],
          ['i2', 'i2'],
        ],
      ],
      expectedOutput: [
        {
          type: 'child',
          row: {
            id: 'i1',
            text: 'issue 1',
          },
          child: {
            change: {
              type: 'remove',
              node: {
                row: {
                  id: 'c1',
                  issueID: 'i1',
                  text: 'comment 1',
                },
                relationships: {},
              },
            },
            relationshipName: 'comments',
          },
        },
        {
          type: 'child',
          row: {
            id: 'i2',
            text: 'issue 2',
          },
          child: {
            change: {
              type: 'add',
              node: {
                row: {
                  id: 'c1',
                  issueID: 'i2',
                  text: 'comment 1.2',
                },
                relationships: {},
              },
            },
            relationshipName: 'comments',
          },
        },
      ],
    });

    pushTest({
      ...base,
      name: 'edit id of issue',
      sources: [
        [
          {id: 'i1', text: 'issue 1'},
          {id: 'i2', text: 'issue 2'},
        ],
        [
          {id: 'c1', issueID: 'i1', text: 'comment 1'},
          {id: 'c2', issueID: 'i2', text: 'comment 2'},
          {id: 'c3', issueID: 'i3', text: 'comment 3'},
        ],
      ],
      pushes: [
        [
          0,
          {
            type: 'edit',
            oldRow: {id: 'i1', text: 'issue 1'},
            row: {id: 'i3', text: 'issue 1.3'},
          },
        ],
      ],
      expectedLog: [
        [
          '0',
          'push',
          {
            type: 'edit',
            oldRow: {id: 'i1', text: 'issue 1'},
            row: {id: 'i3', text: 'issue 1.3'},
          },
        ],
        ['1', 'cleanup', {constraint: {issueID: 'i1'}}],
        ['1', 'fetch', {constraint: {issueID: 'i3'}}],
      ],
      expectedPrimaryKeySetStorageKeys: [
        [
          ['i2', 'i2'],
          ['i3', 'i3'],
        ],
      ],
      expectedOutput: [
        {
          node: {
            relationships: {
              comments: [
                {
                  relationships: {},
                  row: {
                    id: 'c1',
                    issueID: 'i1',
                    text: 'comment 1',
                  },
                },
              ],
            },
            row: {
              id: 'i1',
              text: 'issue 1',
            },
          },
          type: 'remove',
        },
        {
          node: {
            relationships: {
              comments: [
                {
                  relationships: {},
                  row: {
                    id: 'c3',
                    issueID: 'i3',
                    text: 'comment 3',
                  },
                },
              ],
            },
            row: {
              id: 'i3',
              text: 'issue 1.3',
            },
          },
          type: 'add',
        },
      ],
    });
  });
});

suite('push many:one', () => {
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

  pushTest({
    ...base,
    name: 'fetch one child, add parent',
    sources: [[], [{id: 'u1'}]],
    pushes: [[0, {type: 'add', row: {id: 'i1', ownerID: 'u1'}}]],
    expectedLog: [
      ['0', 'push', {type: 'add', row: {id: 'i1', ownerID: 'u1'}}],
      ['1', 'fetch', {constraint: {id: 'u1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['u1', 'i1']]],
    expectedOutput: [
      {
        type: 'add',
        node: {
          row: {
            id: 'i1',
            ownerID: 'u1',
          },
          relationships: {
            owner: [{row: {id: 'u1'}, relationships: {}}],
          },
        },
      },
    ],
  });

  pushTest({
    ...base,
    name: 'fetch one child, add wrong parent',
    sources: [[], [{id: 'u1'}]],
    pushes: [[0, {type: 'add', row: {id: 'i1', ownerID: 'u2'}}]],
    expectedLog: [
      ['0', 'push', {type: 'add', row: {id: 'i1', ownerID: 'u2'}}],
      ['1', 'fetch', {constraint: {id: 'u2'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['u2', 'i1']]],
    expectedOutput: [
      {
        type: 'add',
        node: {
          row: {
            id: 'i1',
            ownerID: 'u2',
          },
          relationships: {
            owner: [],
          },
        },
      },
    ],
  });

  pushTest({
    ...base,
    name: 'fetch one parent, add child',
    sources: [[{id: 'i1', ownerID: 'u1'}], []],
    pushes: [[1, {type: 'add', row: {id: 'u1'}}]],
    expectedLog: [
      ['1', 'push', {type: 'add', row: {id: 'u1'}}],
      ['0', 'fetch', {constraint: {ownerID: 'u1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['u1', 'i1']]],
    expectedOutput: [
      {
        type: 'child',
        row: {
          id: 'i1',
          ownerID: 'u1',
        },
        child: {
          relationshipName: 'owner',
          change: {
            type: 'add',
            node: {
              row: {
                id: 'u1',
              },
              relationships: {},
            },
          },
        },
      },
    ],
  });

  pushTest({
    ...base,
    name: 'fetch two parents, add one child',
    sources: [
      [
        {id: 'i1', ownerID: 'u1'},
        {id: 'i2', ownerID: 'u1'},
      ],
      [],
    ],
    pushes: [[1, {type: 'add', row: {id: 'u1'}}]],
    expectedLog: [
      ['1', 'push', {type: 'add', row: {id: 'u1'}}],
      ['0', 'fetch', {constraint: {ownerID: 'u1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [
      [
        ['u1', 'i1'],
        ['u1', 'i2'],
      ],
    ],
    expectedOutput: [
      {
        type: 'child',
        row: {
          id: 'i1',
          ownerID: 'u1',
        },
        child: {
          relationshipName: 'owner',
          change: {
            type: 'add',
            node: {
              row: {
                id: 'u1',
              },
              relationships: {},
            },
          },
        },
      },
      {
        type: 'child',
        row: {
          id: 'i2',
          ownerID: 'u1',
        },
        child: {
          relationshipName: 'owner',
          change: {
            type: 'add',
            node: {
              row: {
                id: 'u1',
              },
              relationships: {},
            },
          },
        },
      },
    ],
  });

  suite('edit', () => {
    const base = {
      columns: [
        {
          id: {type: 'string'},
          ownerID: {type: 'string'},
          text: {type: 'string'},
        },
        {id: {type: 'string'}, text: {type: 'string'}},
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

    pushTest({
      ...base,
      name: 'edit child to make it match to parents',
      sources: [
        [
          {id: 'i1', ownerID: 'u1', text: 'item 1'},
          {id: 'i2', ownerID: 'u1', text: 'item 2'},
        ],
        [{id: 'u2', text: 'user 2'}],
      ],
      pushes: [
        [
          1,
          {
            type: 'edit',
            row: {id: 'u1', text: 'user 1'},
            oldRow: {id: 'u2', text: 'user 2'},
          },
        ],
      ],
      expectedLog: [
        [
          '1',
          'push',
          {
            type: 'edit',
            row: {id: 'u1', text: 'user 1'},
            oldRow: {id: 'u2', text: 'user 2'},
          },
        ],
        ['0', 'fetch', {constraint: {ownerID: 'u2'}}],
        ['0', 'fetch', {constraint: {ownerID: 'u1'}}],
      ],
      expectedPrimaryKeySetStorageKeys: [
        [
          ['u1', 'i1'],
          ['u1', 'i2'],
        ],
      ],
      expectedOutput: [
        {
          type: 'child',
          row: {
            id: 'i1',
            ownerID: 'u1',
            text: 'item 1',
          },
          child: {
            relationshipName: 'owner',
            change: {
              type: 'add',
              node: {
                row: {
                  id: 'u1',
                  text: 'user 1',
                },
                relationships: {},
              },
            },
          },
        },
        {
          type: 'child',
          row: {
            id: 'i2',
            ownerID: 'u1',
            text: 'item 2',
          },
          child: {
            relationshipName: 'owner',
            change: {
              type: 'add',
              node: {
                row: {
                  id: 'u1',
                  text: 'user 1',
                },
                relationships: {},
              },
            },
          },
        },
      ],
    });

    pushTest({
      columns: [
        {id: {type: 'string'}, text: {type: 'string'}},
        {
          id: {type: 'string'},
          ownerID: {type: 'string'},
          text: {type: 'string'},
        },
        {
          id: {type: 'string'},
          issueID: {type: 'string'},
        },
      ],
      primaryKeys: [['id'], ['id'], ['id']],
      joins: [
        {
          parentKey: ['id'],
          childKey: ['ownerID'],
          relationshipName: 'issues',
        },
        {
          parentKey: ['id'],
          childKey: ['issueID'],
          relationshipName: 'comments',
        },
      ],
      name: 'edit child to make it match to parents, 1 to many to many',
      sources: [
        [
          {id: 'u1', text: 'user 1'},
          {id: 'u2', text: 'user 2'},
        ],
        [
          {id: 'i1', ownerID: 'u1', text: 'item 1'},
          {id: 'i2', ownerID: 'u2', text: 'item 2'},
        ],
        [
          {id: 'c1', issueID: 'i1'},
          {id: 'c2', issueID: 'i2'},
        ],
      ],
      pushes: [
        [
          1,
          {
            type: 'edit',
            row: {id: 'i2', ownerID: 'u1', text: 'item 2'},
            oldRow: {id: 'i2', ownerID: 'u2', text: 'item 2'},
          },
        ],
      ],
      expectedLog: [
        [
          '1',
          'push',
          {
            type: 'edit',
            row: {id: 'i2', ownerID: 'u1', text: 'item 2'},
            oldRow: {id: 'i2', ownerID: 'u2', text: 'item 2'},
          },
        ],
        ['2', 'cleanup', {constraint: {issueID: 'i2'}}],
        ['2', 'fetch', {constraint: {issueID: 'i2'}}],
        ['0', 'fetch', {constraint: {id: 'u2'}}],
        ['0', 'fetch', {constraint: {id: 'u1'}}],
      ],
      expectedPrimaryKeySetStorageKeys: [
        [
          ['u1', 'u1'],
          ['u2', 'u2'],
        ],
        [
          ['i1', 'i1'],
          ['i2', 'i2'],
        ],
      ],
      expectedOutput: [
        {
          type: 'child',
          row: {
            id: 'u2',
            text: 'user 2',
          },
          child: {
            relationshipName: 'issues',
            change: {
              type: 'remove',
              node: {
                row: {id: 'i2', ownerID: 'u2', text: 'item 2'},
                relationships: {
                  comments: [
                    {row: {id: 'c2', issueID: 'i2'}, relationships: {}},
                  ],
                },
              },
            },
          },
        },
        {
          type: 'child',
          row: {
            id: 'u1',
            text: 'user 1',
          },
          child: {
            relationshipName: 'issues',
            change: {
              type: 'add',
              node: {
                row: {id: 'i2', ownerID: 'u1', text: 'item 2'},
                relationships: {
                  comments: [
                    {row: {id: 'c2', issueID: 'i2'}, relationships: {}},
                  ],
                },
              },
            },
          },
        },
      ],
    });

    pushTest({
      ...base,
      name: 'edit non matching child',
      sources: [
        [
          {id: 'i1', ownerID: 'u1', text: 'item 1'},
          {id: 'i2', ownerID: 'u1', text: 'item 2'},
        ],
        [{id: 'u2', text: 'user 2'}],
      ],
      pushes: [
        [
          1,
          {
            type: 'edit',
            row: {id: 'u2', text: 'user 2 changed'},
            oldRow: {id: 'u2', text: 'user 2'},
          },
        ],
      ],
      expectedLog: [
        [
          '1',
          'push',
          {
            type: 'edit',
            row: {id: 'u2', text: 'user 2 changed'},
            oldRow: {id: 'u2', text: 'user 2'},
          },
        ],
        ['0', 'fetch', {constraint: {ownerID: 'u2'}}],
      ],
      expectedPrimaryKeySetStorageKeys: [
        [
          ['u1', 'i1'],
          ['u1', 'i2'],
        ],
      ],
      expectedOutput: [],
    });

    pushTest({
      ...base,
      name: 'edit matching child',
      sources: [
        [
          {id: 'i1', ownerID: 'u1', text: 'item 1'},
          {id: 'i2', ownerID: 'u1', text: 'item 2'},
        ],
        [{id: 'u1', text: 'user 1'}],
      ],
      pushes: [
        [
          1,
          {
            type: 'edit',
            row: {id: 'u1', text: 'user 1 changed'},
            oldRow: {id: 'u1', text: 'user 1'},
          },
        ],
      ],
      expectedLog: [
        [
          '1',
          'push',
          {
            type: 'edit',
            row: {id: 'u1', text: 'user 1 changed'},
            oldRow: {id: 'u1', text: 'user 1'},
          },
        ],
        ['0', 'fetch', {constraint: {ownerID: 'u1'}}],
      ],
      expectedPrimaryKeySetStorageKeys: [
        [
          ['u1', 'i1'],
          ['u1', 'i2'],
        ],
      ],
      expectedOutput: [
        {
          type: 'child',
          row: {
            id: 'i1',
            ownerID: 'u1',
            text: 'item 1',
          },
          child: {
            change: {
              type: 'edit',
              oldRow: {
                id: 'u1',
                text: 'user 1',
              },
              row: {
                id: 'u1',
                text: 'user 1 changed',
              },
            },
            relationshipName: 'owner',
          },
        },
        {
          type: 'child',
          row: {
            id: 'i2',
            ownerID: 'u1',
            text: 'item 2',
          },
          child: {
            change: {
              type: 'edit',
              oldRow: {
                id: 'u1',
                text: 'user 1',
              },
              row: {
                id: 'u1',
                text: 'user 1 changed',
              },
            },
            relationshipName: 'owner',
          },
        },
      ],
    });
  });
});

suite('push one:many:many', () => {
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

  pushTest({
    ...base,
    name: 'fetch one parent, one child, add grandchild',
    sources: [[{id: 'i1'}], [{id: 'c1', issueID: 'i1'}], []],
    pushes: [[2, {type: 'add', row: {id: 'r1', commentID: 'c1'}}]],
    expectedLog: [
      ['2', 'push', {type: 'add', row: {id: 'r1', commentID: 'c1'}}],
      ['1', 'fetch', {constraint: {id: 'c1'}}],
      ['0', 'fetch', {constraint: {id: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']], [['c1', 'c1']]],
    expectedOutput: [
      {
        type: 'child',
        row: {
          id: 'i1',
        },
        child: {
          relationshipName: 'comments',
          change: {
            type: 'child',
            row: {
              id: 'c1',
              issueID: 'i1',
            },
            child: {
              relationshipName: 'revisions',
              change: {
                type: 'add',
                node: {
                  row: {
                    id: 'r1',
                    commentID: 'c1',
                  },
                  relationships: {},
                },
              },
            },
          },
        },
      },
    ],
  });

  pushTest({
    ...base,
    name: 'fetch one parent, one grandchild, add child',
    sources: [[{id: 'i1'}], [], [{id: 'r1', commentID: 'c1'}]],
    pushes: [[1, {type: 'add', row: {id: 'c1', issueID: 'i1'}}]],
    expectedLog: [
      ['1', 'push', {type: 'add', row: {id: 'c1', issueID: 'i1'}}],
      ['2', 'fetch', {constraint: {commentID: 'c1'}}],
      ['0', 'fetch', {constraint: {id: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']], [['c1', 'c1']]],
    expectedOutput: [
      {
        type: 'child',
        row: {
          id: 'i1',
        },
        child: {
          relationshipName: 'comments',
          change: {
            type: 'add',
            node: {
              row: {
                id: 'c1',
                issueID: 'i1',
              },
              relationships: {
                revisions: [
                  {row: {id: 'r1', commentID: 'c1'}, relationships: {}},
                ],
              },
            },
          },
        },
      },
    ],
  });

  pushTest({
    ...base,
    name: 'fetch one child, one grandchild, add parent',
    sources: [[], [{id: 'c1', issueID: 'i1'}], [{id: 'r1', commentID: 'c1'}]],
    pushes: [[0, {type: 'add', row: {id: 'i1'}}]],
    expectedLog: [
      ['0', 'push', {type: 'add', row: {id: 'i1'}}],
      ['1', 'fetch', {constraint: {issueID: 'i1'}}],
      ['2', 'fetch', {constraint: {commentID: 'c1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']], [['c1', 'c1']]],
    expectedOutput: [
      {
        type: 'add',
        node: {
          row: {
            id: 'i1',
          },
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
      },
    ],
  });

  pushTest({
    ...base,
    name: 'fetch one parent, one child, one grandchild, remove parent',
    sources: [
      [{id: 'i1'}],
      [{id: 'c1', issueID: 'i1'}],
      [{id: 'r1', commentID: 'c1'}],
    ],
    pushes: [[0, {type: 'remove', row: {id: 'i1'}}]],
    expectedLog: [
      ['0', 'push', {type: 'remove', row: {id: 'i1'}}],
      ['1', 'cleanup', {constraint: {issueID: 'i1'}}],
      ['2', 'cleanup', {constraint: {commentID: 'c1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[], []],
    expectedOutput: [
      {
        type: 'remove',
        node: {
          row: {
            id: 'i1',
          },
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
      },
    ],
  });
});

suite('push one:many:one', () => {
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

  const sorts = {
    1: [
      ['issueID', 'asc'],
      ['labelID', 'asc'],
    ] as const,
  };

  pushTest({
    ...base,
    name: 'fetch one parent, one child, add grandchild',
    sources: [[{id: 'i1'}], [{issueID: 'i1', labelID: 'l1'}], []],
    sorts,
    pushes: [[2, {type: 'add', row: {id: 'l1'}}]],
    expectedLog: [
      ['2', 'push', {type: 'add', row: {id: 'l1'}}],
      ['1', 'fetch', {constraint: {labelID: 'l1'}}],
      ['0', 'fetch', {constraint: {id: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']], [['l1', 'i1', 'l1']]],
    expectedOutput: [
      {
        type: 'child',
        row: {
          id: 'i1',
        },
        child: {
          relationshipName: 'issuelabels',
          change: {
            type: 'child',
            row: {
              issueID: 'i1',
              labelID: 'l1',
            },
            child: {
              relationshipName: 'labels',
              change: {
                type: 'add',
                node: {
                  row: {
                    id: 'l1',
                  },
                  relationships: {},
                },
              },
            },
          },
        },
      },
    ],
  });

  pushTest({
    ...base,
    name: 'fetch one parent, one grandchild, add child',
    sources: [[{id: 'i1'}], [], [{id: 'l1'}]],
    sorts,
    pushes: [[1, {type: 'add', row: {issueID: 'i1', labelID: 'l1'}}]],
    expectedLog: [
      ['1', 'push', {type: 'add', row: {issueID: 'i1', labelID: 'l1'}}],
      ['2', 'fetch', {constraint: {id: 'l1'}}],
      ['0', 'fetch', {constraint: {id: 'i1'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [[['i1', 'i1']], [['l1', 'i1', 'l1']]],
    expectedOutput: [
      {
        type: 'child',
        row: {
          id: 'i1',
        },
        child: {
          relationshipName: 'issuelabels',
          change: {
            type: 'add',
            node: {
              row: {
                issueID: 'i1',
                labelID: 'l1',
              },
              relationships: {
                labels: [{row: {id: 'l1'}, relationships: {}}],
              },
            },
          },
        },
      },
    ],
  });

  pushTest({
    ...base,
    name: 'fetch two parents, two children, add one grandchild',
    sources: [
      [{id: 'i1'}, {id: 'i2'}],
      [
        {issueID: 'i1', labelID: 'l1'},
        {issueID: 'i2', labelID: 'l1'},
      ],
      [],
    ],
    sorts,
    pushes: [[2, {type: 'add', row: {id: 'l1'}}]],
    expectedLog: [
      ['2', 'push', {type: 'add', row: {id: 'l1'}}],
      ['1', 'fetch', {constraint: {labelID: 'l1'}}],
      ['0', 'fetch', {constraint: {id: 'i1'}}],
      ['0', 'fetch', {constraint: {id: 'i2'}}],
    ],
    expectedPrimaryKeySetStorageKeys: [
      [
        ['i1', 'i1'],
        ['i2', 'i2'],
      ],
      [
        ['l1', 'i1', 'l1'],
        ['l1', 'i2', 'l1'],
      ],
    ],
    expectedOutput: [
      {
        type: 'child',
        row: {
          id: 'i1',
        },
        child: {
          relationshipName: 'issuelabels',
          change: {
            type: 'child',
            row: {
              issueID: 'i1',
              labelID: 'l1',
            },
            child: {
              relationshipName: 'labels',
              change: {
                type: 'add',
                node: {
                  row: {
                    id: 'l1',
                  },
                  relationships: {},
                },
              },
            },
          },
        },
      },
      {
        type: 'child',
        row: {
          id: 'i2',
        },
        child: {
          relationshipName: 'issuelabels',
          change: {
            type: 'child',
            row: {
              issueID: 'i2',
              labelID: 'l1',
            },
            child: {
              relationshipName: 'labels',
              change: {
                type: 'add',
                node: {
                  row: {
                    id: 'l1',
                  },
                  relationships: {},
                },
              },
            },
          },
        },
      },
    ],
  });
});

describe('edit assignee', () => {
  const sources: Sources = {
    issue: {
      columns: {
        issueID: {type: 'string'},
        text: {type: 'string'},
        assigneeID: {type: 'string', optional: true},
        creatorID: {type: 'string'},
      },
      primaryKeys: ['issueID'],
      sorts: [['issueID', 'asc']],
      rows: [
        {
          issueID: 'i1',
          text: 'first issue',
          assigneeID: undefined,
          creatorID: 'u1',
        },
        {
          issueID: 'i2',
          text: 'second issue',
          assigneeID: 'u2',
          creatorID: 'u2',
        },
      ],
    },
    user: {
      columns: {
        userID: {type: 'string'},
        name: {type: 'string'},
      },
      primaryKeys: ['userID'],
      sorts: [['userID', 'asc']],
      rows: [
        {userID: 'u1', name: 'user 1'},
        {userID: 'u2', name: 'user 2'},
      ],
    },
  };

  const joins: Joins = {
    creator: {
      parentKey: ['creatorID'],
      parentSource: 'issue',
      childKey: ['userID'],
      childSource: 'user',
      relationshipName: 'creator',
    },
    assignee: {
      parentKey: ['assigneeID'],
      parentSource: 'creator',
      childKey: ['userID'],
      childSource: 'user',
      relationshipName: 'assignee',
    },
  };

  const format: Format = {
    singular: false,
    relationships: {
      creator: {
        singular: false,
        relationships: {},
      },
      assignee: {
        singular: false,
        relationships: {},
      },
    },
  };

  test('from none to one', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      joins,
      pushes: [
        [
          'issue',
          {
            type: 'edit',
            oldRow: {
              issueID: 'i1',
              text: 'first issue',
              assigneeID: undefined,
              creatorID: 'u1',
            },
            row: {
              issueID: 'i1',
              text: 'first issue',
              assigneeID: 'u1',
              creatorID: 'u1',
            },
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "assignee": [
            {
              "name": "user 1",
              "userID": "u1",
            },
          ],
          "assigneeID": "u1",
          "creator": [
            {
              "name": "user 1",
              "userID": "u1",
            },
          ],
          "creatorID": "u1",
          "issueID": "i1",
          "text": "first issue",
        },
        {
          "assignee": [
            {
              "name": "user 2",
              "userID": "u2",
            },
          ],
          "assigneeID": "u2",
          "creator": [
            {
              "name": "user 2",
              "userID": "u2",
            },
          ],
          "creatorID": "u2",
          "issueID": "i2",
          "text": "second issue",
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
          "push",
          {
            "oldRow": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "edit",
          },
        ],
        [
          "user",
          "cleanup",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "user",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "creator",
          "push",
          {
            "oldRow": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "edit",
          },
        ],
        [
          "user",
          "cleanup",
          {
            "constraint": {
              "userID": undefined,
            },
          },
        ],
        [
          "assignee",
          "push",
          {
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "remove",
          },
        ],
        [
          "user",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "assignee",
          "push",
          {
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "add",
          },
        ],
      ]
    `);

    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "assignee": [],
              "creator": [
                {
                  "relationships": {},
                  "row": {
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
              ],
            },
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
          },
          "type": "remove",
        },
        {
          "node": {
            "relationships": {
              "assignee": [
                {
                  "relationships": {},
                  "row": {
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
              ],
              "creator": [
                {
                  "relationships": {},
                  "row": {
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
              ],
            },
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
          },
          "type": "add",
        },
      ]
    `);

    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "assignee": {
          ""pKeySet","u1","i1",": true,
          ""pKeySet","u2","i2",": true,
        },
        "creator": {
          ""pKeySet","u1","i1",": true,
          ""pKeySet","u2","i2",": true,
        },
      }
    `);
  });

  test('from none to many', () => {
    const localSources: Sources = {
      ...sources,
      user: {
        columns: {
          userID: {type: 'string'},
          id: {type: 'number'},
          name: {type: 'string'},
        },
        primaryKeys: ['userID', 'id'],
        sorts: [
          ['userID', 'asc'],
          ['id', 'asc'],
        ],
        rows: [
          {userID: 'u1', id: 1, name: 'user 1'},
          {userID: 'u1', id: 1.5, name: 'user 1.5'},
          {userID: 'u2', id: 2, name: 'user 2'},
        ],
      },
    };

    const {log, data, actualStorage, pushes} = runJoinTest({
      sources: localSources,
      joins,
      pushes: [
        [
          'issue',
          {
            type: 'edit',
            oldRow: {
              issueID: 'i1',
              text: 'first issue',
              assigneeID: undefined,
              creatorID: 'u1',
            },
            row: {
              issueID: 'i1',
              text: 'first issue',
              assigneeID: 'u1',
              creatorID: 'u1',
            },
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "assignee": [
            {
              "id": 1,
              "name": "user 1",
              "userID": "u1",
            },
            {
              "id": 1.5,
              "name": "user 1.5",
              "userID": "u1",
            },
          ],
          "assigneeID": "u1",
          "creator": [
            {
              "id": 1,
              "name": "user 1",
              "userID": "u1",
            },
            {
              "id": 1.5,
              "name": "user 1.5",
              "userID": "u1",
            },
          ],
          "creatorID": "u1",
          "issueID": "i1",
          "text": "first issue",
        },
        {
          "assignee": [
            {
              "id": 2,
              "name": "user 2",
              "userID": "u2",
            },
          ],
          "assigneeID": "u2",
          "creator": [
            {
              "id": 2,
              "name": "user 2",
              "userID": "u2",
            },
          ],
          "creatorID": "u2",
          "issueID": "i2",
          "text": "second issue",
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
          "push",
          {
            "oldRow": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "edit",
          },
        ],
        [
          "user",
          "cleanup",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "user",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "creator",
          "push",
          {
            "oldRow": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "edit",
          },
        ],
        [
          "user",
          "cleanup",
          {
            "constraint": {
              "userID": undefined,
            },
          },
        ],
        [
          "assignee",
          "push",
          {
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "remove",
          },
        ],
        [
          "user",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "assignee",
          "push",
          {
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "add",
          },
        ],
      ]
    `);

    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "assignee": [],
              "creator": [
                {
                  "relationships": {},
                  "row": {
                    "id": 1,
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "id": 1.5,
                    "name": "user 1.5",
                    "userID": "u1",
                  },
                },
              ],
            },
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
          },
          "type": "remove",
        },
        {
          "node": {
            "relationships": {
              "assignee": [
                {
                  "relationships": {},
                  "row": {
                    "id": 1,
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "id": 1.5,
                    "name": "user 1.5",
                    "userID": "u1",
                  },
                },
              ],
              "creator": [
                {
                  "relationships": {},
                  "row": {
                    "id": 1,
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "id": 1.5,
                    "name": "user 1.5",
                    "userID": "u1",
                  },
                },
              ],
            },
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
          },
          "type": "add",
        },
      ]
    `);

    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "assignee": {
          ""pKeySet","u1","i1",": true,
          ""pKeySet","u2","i2",": true,
        },
        "creator": {
          ""pKeySet","u1","i1",": true,
          ""pKeySet","u2","i2",": true,
        },
      }
    `);
  });

  test('from one to none', () => {
    const localSources = structuredClone(sources);
    localSources.issue.rows[0].assigneeID = 'u1';

    const {log, data, actualStorage, pushes} = runJoinTest({
      sources: localSources,
      joins,
      pushes: [
        [
          'issue',
          {
            type: 'edit',
            oldRow: {
              issueID: 'i1',
              text: 'first issue',
              assigneeID: 'u1',
              creatorID: 'u1',
            },
            row: {
              issueID: 'i1',
              text: 'first issue',
              assigneeID: undefined,
              creatorID: 'u1',
            },
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "assignee": [],
          "assigneeID": undefined,
          "creator": [
            {
              "name": "user 1",
              "userID": "u1",
            },
          ],
          "creatorID": "u1",
          "issueID": "i1",
          "text": "first issue",
        },
        {
          "assignee": [
            {
              "name": "user 2",
              "userID": "u2",
            },
          ],
          "assigneeID": "u2",
          "creator": [
            {
              "name": "user 2",
              "userID": "u2",
            },
          ],
          "creatorID": "u2",
          "issueID": "i2",
          "text": "second issue",
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
          "push",
          {
            "oldRow": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "edit",
          },
        ],
        [
          "user",
          "cleanup",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "user",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "creator",
          "push",
          {
            "oldRow": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "edit",
          },
        ],
        [
          "user",
          "cleanup",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "assignee",
          "push",
          {
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "remove",
          },
        ],
        [
          "user",
          "fetch",
          {
            "constraint": {
              "userID": undefined,
            },
          },
        ],
        [
          "assignee",
          "push",
          {
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "add",
          },
        ],
      ]
    `);

    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "assignee": [
                {
                  "relationships": {},
                  "row": {
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
              ],
              "creator": [
                {
                  "relationships": {},
                  "row": {
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
              ],
            },
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
          },
          "type": "remove",
        },
        {
          "node": {
            "relationships": {
              "assignee": [],
              "creator": [
                {
                  "relationships": {},
                  "row": {
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
              ],
            },
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
          },
          "type": "add",
        },
      ]
    `);

    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "assignee": {
          ""pKeySet","u2","i2",": true,
          ""pKeySet",null,"i1",": true,
        },
        "creator": {
          ""pKeySet","u1","i1",": true,
          ""pKeySet","u2","i2",": true,
        },
      }
    `);
  });

  test('from many to none', () => {
    const issue = structuredClone(sources.issue);
    issue.rows[0].assigneeID = 'u1';
    const localSources: Sources = {
      issue,
      user: {
        columns: {
          userID: {type: 'string'},
          id: {type: 'number'},
          name: {type: 'string'},
        },
        primaryKeys: ['userID', 'id'],
        sorts: [
          ['userID', 'asc'],
          ['id', 'asc'],
        ],
        rows: [
          {userID: 'u1', id: 1, name: 'user 1'},
          {userID: 'u1', id: 1.5, name: 'user 1.5'},
          {userID: 'u2', id: 2, name: 'user 2'},
        ],
      },
    };
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources: localSources,
      joins,
      pushes: [
        [
          'issue',
          {
            type: 'edit',
            oldRow: {
              issueID: 'i1',
              text: 'first issue',
              assigneeID: 'u1',
              creatorID: 'u1',
            },
            row: {
              issueID: 'i1',
              text: 'first issue',
              assigneeID: undefined,
              creatorID: 'u1',
            },
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "assignee": [],
          "assigneeID": undefined,
          "creator": [
            {
              "id": 1,
              "name": "user 1",
              "userID": "u1",
            },
            {
              "id": 1.5,
              "name": "user 1.5",
              "userID": "u1",
            },
          ],
          "creatorID": "u1",
          "issueID": "i1",
          "text": "first issue",
        },
        {
          "assignee": [
            {
              "id": 2,
              "name": "user 2",
              "userID": "u2",
            },
          ],
          "assigneeID": "u2",
          "creator": [
            {
              "id": 2,
              "name": "user 2",
              "userID": "u2",
            },
          ],
          "creatorID": "u2",
          "issueID": "i2",
          "text": "second issue",
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
          "push",
          {
            "oldRow": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "edit",
          },
        ],
        [
          "user",
          "cleanup",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "user",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "creator",
          "push",
          {
            "oldRow": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "edit",
          },
        ],
        [
          "user",
          "cleanup",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "assignee",
          "push",
          {
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "remove",
          },
        ],
        [
          "user",
          "fetch",
          {
            "constraint": {
              "userID": undefined,
            },
          },
        ],
        [
          "assignee",
          "push",
          {
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "add",
          },
        ],
      ]
    `);

    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "assignee": [
                {
                  "relationships": {},
                  "row": {
                    "id": 1,
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "id": 1.5,
                    "name": "user 1.5",
                    "userID": "u1",
                  },
                },
              ],
              "creator": [
                {
                  "relationships": {},
                  "row": {
                    "id": 1,
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "id": 1.5,
                    "name": "user 1.5",
                    "userID": "u1",
                  },
                },
              ],
            },
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
          },
          "type": "remove",
        },
        {
          "node": {
            "relationships": {
              "assignee": [],
              "creator": [
                {
                  "relationships": {},
                  "row": {
                    "id": 1,
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "id": 1.5,
                    "name": "user 1.5",
                    "userID": "u1",
                  },
                },
              ],
            },
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
          },
          "type": "add",
        },
      ]
    `);

    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "assignee": {
          ""pKeySet","u2","i2",": true,
          ""pKeySet",null,"i1",": true,
        },
        "creator": {
          ""pKeySet","u1","i1",": true,
          ""pKeySet","u2","i2",": true,
        },
      }
    `);
  });
});

describe('joins with compound join keys', () => {
  const sources: Sources = {
    a: {
      columns: {
        id: {type: 'number'},
        a1: {type: 'number'},
        a2: {type: 'number'},
        a3: {type: 'number'},
      },
      primaryKeys: ['id'],
      rows: [
        {id: 0, a1: 1, a2: 2, a3: 3},
        {id: 1, a1: 4, a2: 5, a3: 6},
      ],
      sorts: [['id', 'asc']],
    },
    b: {
      columns: {
        id: {type: 'number'},
        b1: {type: 'number'},
        b2: {type: 'number'},
        b3: {type: 'number'},
      },
      primaryKeys: ['id'],
      rows: [
        {id: 0, b1: 2, b2: 1, b3: 3},
        {id: 1, b1: 5, b2: 4, b3: 6},
      ],
      sorts: [['id', 'asc']],
    },
  };

  const joins: Joins = {
    ab: {
      parentSource: 'a',
      parentKey: ['a1', 'a2'],
      childSource: 'b',
      childKey: ['b2', 'b1'], // not the same order as parentKey
      relationshipName: 'ab',
    },
  };

  const format: Format = {
    singular: false,
    relationships: {
      ab: {
        singular: false,
        relationships: {},
      },
    },
  };

  test('add parent and child', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      joins,
      pushes: [
        [
          'a',
          {
            type: 'add',
            row: {id: 2, a1: 7, a2: 8, a3: 9},
          },
        ],
        [
          'b',
          {
            type: 'add',
            row: {id: 2, b1: 8, b2: 7, b3: 9},
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "a1": 1,
          "a2": 2,
          "a3": 3,
          "ab": [
            {
              "b1": 2,
              "b2": 1,
              "b3": 3,
              "id": 0,
            },
          ],
          "id": 0,
        },
        {
          "a1": 4,
          "a2": 5,
          "a3": 6,
          "ab": [
            {
              "b1": 5,
              "b2": 4,
              "b3": 6,
              "id": 1,
            },
          ],
          "id": 1,
        },
        {
          "a1": 7,
          "a2": 8,
          "a3": 9,
          "ab": [
            {
              "b1": 8,
              "b2": 7,
              "b3": 9,
              "id": 2,
            },
          ],
          "id": 2,
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "a",
          "push",
          {
            "row": {
              "a1": 7,
              "a2": 8,
              "a3": 9,
              "id": 2,
            },
            "type": "add",
          },
        ],
        [
          "b",
          "fetch",
          {
            "constraint": {
              "b1": 8,
              "b2": 7,
            },
          },
        ],
        [
          "ab",
          "push",
          {
            "row": {
              "a1": 7,
              "a2": 8,
              "a3": 9,
              "id": 2,
            },
            "type": "add",
          },
        ],
        [
          "b",
          "push",
          {
            "row": {
              "b1": 8,
              "b2": 7,
              "b3": 9,
              "id": 2,
            },
            "type": "add",
          },
        ],
        [
          "a",
          "fetch",
          {
            "constraint": {
              "a1": 7,
              "a2": 8,
            },
          },
        ],
        [
          "ab",
          "push",
          {
            "child": {
              "row": {
                "b1": 8,
                "b2": 7,
                "b3": 9,
                "id": 2,
              },
              "type": "add",
            },
            "row": {
              "a1": 7,
              "a2": 8,
              "a3": 9,
              "id": 2,
            },
            "type": "child",
          },
        ],
      ]
    `);

    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "ab": [],
            },
            "row": {
              "a1": 7,
              "a2": 8,
              "a3": 9,
              "id": 2,
            },
          },
          "type": "add",
        },
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "b1": 8,
                  "b2": 7,
                  "b3": 9,
                  "id": 2,
                },
              },
              "type": "add",
            },
            "relationshipName": "ab",
          },
          "row": {
            "a1": 7,
            "a2": 8,
            "a3": 9,
            "id": 2,
          },
          "type": "child",
        },
      ]
    `);

    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "ab": {
          ""pKeySet",1,2,0,": true,
          ""pKeySet",4,5,1,": true,
          ""pKeySet",7,8,2,": true,
        },
      }
    `);
  });

  test('edit child with moving it', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      joins,
      pushes: [
        [
          'a',
          {
            type: 'edit',
            oldRow: {id: 0, a1: 1, a2: 2, a3: 3},
            row: {id: 0, a1: 1, a2: 2, a3: 33},
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "a1": 1,
          "a2": 2,
          "a3": 33,
          "ab": [
            {
              "b1": 2,
              "b2": 1,
              "b3": 3,
              "id": 0,
            },
          ],
          "id": 0,
        },
        {
          "a1": 4,
          "a2": 5,
          "a3": 6,
          "ab": [
            {
              "b1": 5,
              "b2": 4,
              "b3": 6,
              "id": 1,
            },
          ],
          "id": 1,
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "a",
          "push",
          {
            "oldRow": {
              "a1": 1,
              "a2": 2,
              "a3": 3,
              "id": 0,
            },
            "row": {
              "a1": 1,
              "a2": 2,
              "a3": 33,
              "id": 0,
            },
            "type": "edit",
          },
        ],
        [
          "b",
          "cleanup",
          {
            "constraint": {
              "b1": 2,
              "b2": 1,
            },
          },
        ],
        [
          "b",
          "fetch",
          {
            "constraint": {
              "b1": 2,
              "b2": 1,
            },
          },
        ],
        [
          "ab",
          "push",
          {
            "oldRow": {
              "a1": 1,
              "a2": 2,
              "a3": 3,
              "id": 0,
            },
            "row": {
              "a1": 1,
              "a2": 2,
              "a3": 33,
              "id": 0,
            },
            "type": "edit",
          },
        ],
      ]
    `);

    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "oldRow": {
            "a1": 1,
            "a2": 2,
            "a3": 3,
            "id": 0,
          },
          "row": {
            "a1": 1,
            "a2": 2,
            "a3": 33,
            "id": 0,
          },
          "type": "edit",
        },
      ]
    `);

    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "ab": {
          ""pKeySet",1,2,0,": true,
          ""pKeySet",4,5,1,": true,
        },
      }
    `);
  });
});
