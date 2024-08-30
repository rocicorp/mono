import {expect, suite, test} from 'vitest';
import {Join, createPrimaryKeySetStorageKey} from './join.js';
import {MemorySource} from './memory-source.js';
import {MemoryStorage} from './memory-storage.js';
import {SnitchMessage, Snitch} from './snitch.js';
import type {NormalizedValue, Row} from './data.js';
import {assert} from 'shared/src/asserts.js';
import type {Ordering} from '../ast/ast.js';
import {Catch} from './catch.js';
import type {Change} from './change.js';
import type {SourceChange} from './source.js';
import type {ValueType} from './schema.js';

suite('push one:many', () => {
  const base = {
    columns: [
      {id: 'string' as const},
      {id: 'string', issueID: 'string'} as const,
    ],
    primaryKeys: [['id'], ['id']],
    joins: [
      {
        parentKey: 'id',
        childKey: 'issueID',
        relationshipName: 'comments',
      },
    ],
  };

  pushTest({
    ...base,
    name: 'fetch one parent, remove parent',
    sources: [[{id: 'i1'}], []],
    pushes: [[0, {type: 'remove', row: {id: 'i1'}}]],
    expectedLog: [
      ['0', 'push', {type: 'remove', row: {id: 'i1'}}],
      ['1', 'cleanup', {constraint: {key: 'issueID', value: 'i1'}}],
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
      ['0', 'fetch', {constraint: {key: 'id', value: 'i1'}}],
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
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
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
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
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
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i2'}}],
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
      ['0', 'fetch', {constraint: {key: 'id', value: 'i1'}}],
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
      ['0', 'fetch', {constraint: {key: 'id', value: 'i2'}}],
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
      ['1', 'cleanup', {constraint: {key: 'issueID', value: 'i1'}}],
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
      ['1', 'cleanup', {constraint: {key: 'issueID', value: 'i1'}}],
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
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
      ['1', 'push', {type: 'add', row: {id: 'c1', issueID: 'i1'}}],
      ['0', 'fetch', {constraint: {key: 'id', value: 'i1'}}],
      ['1', 'push', {type: 'add', row: {id: 'c2', issueID: 'i1'}}],
      ['0', 'fetch', {constraint: {key: 'id', value: 'i1'}}],
      ['1', 'push', {type: 'remove', row: {id: 'c1', issueID: 'i1'}}],
      ['0', 'fetch', {constraint: {key: 'id', value: 'i1'}}],
      ['0', 'push', {type: 'remove', row: {id: 'i1'}}],
      ['1', 'cleanup', {constraint: {key: 'issueID', value: 'i1'}}],
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
});

suite('push many:one', () => {
  const base = {
    columns: [
      {id: 'string', ownerID: 'string'} as const,
      {id: 'string'} as const,
    ],
    primaryKeys: [['id'], ['id']],
    joins: [
      {
        parentKey: 'ownerID',
        childKey: 'id',
        relationshipName: 'owner',
      },
    ],
  };

  pushTest({
    ...base,
    name: 'fetch one child, add parent',
    sources: [[], [{id: 'u1'}]],
    pushes: [[0, {type: 'add', row: {id: 'i1', ownerID: 'u1'}}]],
    expectedLog: [
      ['0', 'push', {type: 'add', row: {id: 'i1', ownerID: 'u1'}}],
      ['1', 'fetch', {constraint: {key: 'id', value: 'u1'}}],
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
      ['1', 'fetch', {constraint: {key: 'id', value: 'u2'}}],
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
      ['0', 'fetch', {constraint: {key: 'ownerID', value: 'u1'}}],
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
      ['0', 'fetch', {constraint: {key: 'ownerID', value: 'u1'}}],
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
});

suite('push one:many:many', () => {
  const base = {
    columns: [
      {id: 'string'} as const,
      {id: 'string', issueID: 'string'} as const,
      {id: 'string', labelID: 'string'} as const,
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
  };

  pushTest({
    ...base,
    name: 'fetch one parent, one child, add grandchild',
    sources: [[{id: 'i1'}], [{id: 'c1', issueID: 'i1'}], []],
    pushes: [[2, {type: 'add', row: {id: 'r1', commentID: 'c1'}}]],
    expectedLog: [
      ['2', 'push', {type: 'add', row: {id: 'r1', commentID: 'c1'}}],
      ['1', 'fetch', {constraint: {key: 'id', value: 'c1'}}],
      ['0', 'fetch', {constraint: {key: 'id', value: 'i1'}}],
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
      ['2', 'fetch', {constraint: {key: 'commentID', value: 'c1'}}],
      ['0', 'fetch', {constraint: {key: 'id', value: 'i1'}}],
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
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
      ['2', 'fetch', {constraint: {key: 'commentID', value: 'c1'}}],
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
      ['1', 'cleanup', {constraint: {key: 'issueID', value: 'i1'}}],
      ['2', 'cleanup', {constraint: {key: 'commentID', value: 'c1'}}],
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
      {id: 'string'} as const,
      {issueID: 'string', labelID: 'string'} as const,
      {id: 'string'} as const,
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
  };

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
      ['1', 'fetch', {constraint: {key: 'labelID', value: 'l1'}}],
      ['0', 'fetch', {constraint: {key: 'id', value: 'i1'}}],
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
      ['2', 'fetch', {constraint: {key: 'id', value: 'l1'}}],
      ['0', 'fetch', {constraint: {key: 'id', value: 'i1'}}],
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
      ['1', 'fetch', {constraint: {key: 'labelID', value: 'l1'}}],
      ['0', 'fetch', {constraint: {key: 'id', value: 'i1'}}],
      ['0', 'fetch', {constraint: {key: 'id', value: 'i2'}}],
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

function pushTest(t: PushTest) {
  test(t.name, () => {
    assert(t.sources.length > 0);
    assert(t.joins.length === t.sources.length - 1);

    const log: SnitchMessage[] = [];

    const sources = t.sources.map((fetch, i) => {
      const ordering = t.sorts?.[i] ?? [['id', 'asc']];
      const source = new MemorySource('test', t.columns[i], t.primaryKeys[i]);
      for (const row of fetch) {
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

    // By convention we put them in the test bottom up. Why? Easier to think
    // left-to-right.
    const finalJoin = joins[0];
    const c = new Catch(finalJoin.join);

    c.fetch();
    log.length = 0;

    for (const [sourceIndex, change] of t.pushes) {
      sources[sourceIndex].source.push(change);
    }

    for (const [i, j] of joins.entries()) {
      const {storage} = j;
      const expectedStorageKeys = t.expectedPrimaryKeySetStorageKeys[i];
      const expectedStorage: Record<string, boolean> = {};
      for (const k of expectedStorageKeys) {
        expectedStorage[createPrimaryKeySetStorageKey(k)] = true;
      }
      expect(storage.cloneData()).toEqual(expectedStorage);
    }

    expect(log).toEqual(t.expectedLog);
    expect(c.pushes).toEqual(t.expectedOutput);
  });
}

type PushTest = {
  name: string;
  columns: Record<string, ValueType>[];
  primaryKeys: readonly string[][];
  sources: Row[][];
  sorts?: Record<number, Ordering> | undefined;
  joins: {
    parentKey: string;
    childKey: string;
    relationshipName: string;
  }[];
  pushes: [sourceIndex: number, change: SourceChange][];
  expectedLog: SnitchMessage[];
  expectedPrimaryKeySetStorageKeys: NormalizedValue[][][];
  expectedOutput: Change[];
};
