import {describe, expect, test} from 'vitest';
import {newWriteLocal} from '../../../replicache/src/db/write.ts';
import * as FormatVersion from '../../../replicache/src/format-version-enum.ts';
import {SYNC_HEAD_NAME} from '../../../replicache/src/sync/sync-head-name.ts';
import {must} from '../../../shared/src/must.ts';
import {
  schema,
  type Issue,
  type IssueLabel,
  type Label,
  type Revision,
} from '../../../zql/src/query/test/test-schemas.ts';
import {initFromStore, IVMSourceBranch} from './ivm-branch.ts';

import type {FrozenJSONValue} from '../../../replicache/src/frozen-json.ts';
import type {Hash} from '../../../replicache/src/hash.ts';
import type {Diff} from '../../../replicache/src/sync/patch.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {Node} from '../../../zql/src/ivm/data.ts';
import {consume} from '../../../zql/src/ivm/stream.ts';
import {ENTITIES_KEY_PREFIX} from './keys.ts';
import {createDb} from './test/create-db.ts';

import {makeSourceChangeAdd} from '../../../zql/src/ivm/source.ts';
test('fork', () => {
  const main = new IVMSourceBranch({
    users: {
      name: 'users',
      columns: {
        id: {type: 'string'},
        name: {type: 'string'},
      },
      primaryKey: ['id'],
    },
  });
  const mainConnection = main.getSource('users')!.connect([['id', 'asc']]);

  // Add initial data to main
  const usersSource = main.getSource('users')!;
  consume(usersSource.push(makeSourceChangeAdd({id: 'u1', name: 'Alice'})));

  // Fork should have same initial data
  const fork = main.fork();
  const forkConnection = fork.getSource('users')!.connect([['id', 'asc']]);
  expect([...forkConnection.fetch({})]).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "id": "u1",
          "name": "Alice",
        },
      },
    ]
  `);

  // Mutate main
  consume(usersSource.push(makeSourceChangeAdd({id: 'u2', name: 'Bob'})));

  // Mutate fork differently
  consume(
    fork
      .getSource('users')!
      .push(makeSourceChangeAdd({id: 'u3', name: 'Charlie'})),
  );

  // Verify main and fork evolved independently
  expect([...mainConnection.fetch({})]).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "id": "u1",
          "name": "Alice",
        },
      },
      {
        "relationships": {},
        "row": {
          "id": "u2",
          "name": "Bob",
        },
      },
    ]
  `);

  expect([...forkConnection.fetch({})]).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "id": "u1",
          "name": "Alice",
        },
      },
      {
        "relationships": {},
        "row": {
          "id": "u3",
          "name": "Charlie",
        },
      },
    ]
  `);
});

let timestamp = 42;
const lc = createSilentLogContext();
describe('advance', () => {
  test('initFromStore', async () => {
    const branch = new IVMSourceBranch(schema.tables);
    const {dagStore, syncHash} = await createDb(
      [
        [
          `${ENTITIES_KEY_PREFIX}issue/sdf`,
          {
            id: 'sdf',
            title: 'test',
            description: 'test',
            closed: false,
            ownerId: null,
            createdAt: 1743018138477,
          },
        ],
      ],
      timestamp++,
    );
    await initFromStore(branch, syncHash, dagStore);

    expect([
      ...must(branch.getSource('issue'))
        .connect([['id', 'asc']])
        .fetch({}),
    ]).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "closed": false,
            "createdAt": 1743018138477,
            "description": "test",
            "id": "sdf",
            "ownerId": null,
            "title": "test",
          },
        },
      ]
    `);
  });

  // test various cases of diff operations result in the correct state of the rebase branch
  type DiffCase = {
    name: string;
    initial: Array<[string, Issue | Comment | Label | IssueLabel | Revision]>;
    diffs: Diff[];
    expected: Array<Node>;
  };
  describe('diff cases', () => {
    test.each([
      {
        name: 'add to empty dataset',
        initial: [],
        diffs: [],
        expected: [],
      },
      {
        name: 'add to non-empty dataset',
        initial: [
          [
            `${ENTITIES_KEY_PREFIX}issue/sdf`,
            {
              id: 'sdf',
              title: 'test',
              description: 'test',
              closed: false,
              ownerId: null,
              createdAt: 1743018138477,
            },
          ],
        ],
        diffs: [
          {
            op: 'add',
            key: `${ENTITIES_KEY_PREFIX}issue/def`,
            newValue: {
              id: 'def',
              title: 'test',
              description: 'test',
              closed: false,
              ownerId: null,
              createdAt: 1743018138477,
            } as unknown as FrozenJSONValue,
          },
        ],
        expected: [
          {
            relationships: {},
            row: {
              closed: false,
              description: 'test',
              id: 'def',
              ownerId: null,
              title: 'test',
              createdAt: 1743018138477,
            },
          },
          {
            relationships: {},
            row: {
              closed: false,
              description: 'test',
              id: 'sdf',
              ownerId: null,
              title: 'test',
              createdAt: 1743018138477,
            },
          },
        ],
      },
      {
        name: 'change existing data',
        initial: [
          [
            `${ENTITIES_KEY_PREFIX}issue/sdf`,
            {
              id: 'sdf',
              title: 'test',
              description: 'test',
              closed: false,
              ownerId: null,
              createdAt: 1743018138477,
            },
          ],
        ],
        diffs: [
          {
            op: 'change',
            key: `${ENTITIES_KEY_PREFIX}issue/sdf`,
            oldValue: {
              id: 'sdf',
              title: 'test',
              description: 'test',
              closed: false,
              ownerId: null,
              createdAt: 1743018138477,
            } as unknown as FrozenJSONValue,
            newValue: {
              id: 'sdf',
              title: 'test',
              description: 'test',
              closed: true,
              ownerId: null,
              createdAt: 1743018138477,
            } as unknown as FrozenJSONValue,
          },
        ],
        expected: [
          {
            relationships: {},
            row: {
              closed: true,
              description: 'test',
              id: 'sdf',
              ownerId: null,
              title: 'test',
              createdAt: 1743018138477,
            },
          },
        ],
      },
      // diffs change their own data
      {
        name: 'change existing data',
        initial: [],
        diffs: [
          {
            op: 'add',
            key: `${ENTITIES_KEY_PREFIX}issue/sdf`,
            newValue: {
              id: 'sdf',
              title: 'test',
              description: 'test',
              closed: false,
              ownerId: null,
              createdAt: 1743018138477,
            } as unknown as FrozenJSONValue,
          },
          {
            op: 'change',
            key: `${ENTITIES_KEY_PREFIX}issue/sdf`,
            oldValue: {
              id: 'sdf',
              title: 'test',
              description: 'test',
              closed: false,
              ownerId: null,
              createdAt: 1743018138477,
            } as unknown as FrozenJSONValue,
            newValue: {
              id: 'sdf',
              title: 'changed',
              description: 'changed',
              closed: true,
              ownerId: null,
              createdAt: 1743018138477,
            } as unknown as FrozenJSONValue,
          },
        ],
        expected: [
          {
            relationships: {},
            row: {
              closed: true,
              description: 'changed',
              id: 'sdf',
              ownerId: null,
              title: 'changed',
              createdAt: 1743018138477,
            },
          },
        ],
      },
      {
        name: 'remove existing data',
        initial: [
          [
            `${ENTITIES_KEY_PREFIX}issue/sdf`,
            {
              id: 'sdf',
              title: 'test',
              description: 'test',
              closed: false,
              ownerId: null,
              createdAt: 1743018138477,
            },
          ],
        ],
        diffs: [
          {
            op: 'del',
            key: `${ENTITIES_KEY_PREFIX}issue/sdf`,
            oldValue: {
              id: 'sdf',
              title: 'test',
              description: 'test',
              closed: false,
              ownerId: null,
              createdAt: 1743018138477,
            } as unknown as FrozenJSONValue,
          },
        ],
        expected: [],
      },
      // remove existing data
    ] satisfies DiffCase[])('$name', async ({initial, diffs, expected}) => {
      const branch = new IVMSourceBranch(schema.tables);
      const {dagStore, syncHash} = await createDb(initial, timestamp++);
      await initFromStore(branch, syncHash, dagStore);

      const w = await newWriteLocal(
        syncHash,
        'mutator_name',
        JSON.stringify([]),
        null,
        await dagStore.write(),
        timestamp++,
        'client-id',
        FormatVersion.Latest,
      );
      await Promise.all(
        diffs.map(async diff => {
          switch (diff.op) {
            case 'add':
              await w.put(lc, diff.key, diff.newValue);
              break;
            case 'change':
              await w.put(lc, diff.key, diff.newValue);
              break;
            case 'del':
              await w.del(lc, diff.key);
              break;
          }
        }),
      );
      const head = await w.commit(SYNC_HEAD_NAME);

      await branch.advance(syncHash, head, diffs);
      expect([
        ...must(branch.getSource('issue'))
          .connect([['id', 'asc']])
          .fetch({}),
      ]).toEqual(expected);
    });
  });

  test('poisons the branch with the original error when advance fails', async () => {
    const {dagStore} = await createDb([], timestamp++);
    const branch = new IVMSourceBranch({
      users: {
        name: 'users',
        columns: {
          id: {type: 'string'},
          name: {type: 'string'},
        },
        primaryKey: ['id'],
      },
    });

    let originalError: unknown;
    try {
      branch.advance(undefined, 'head1' as Hash, [
        {
          op: 'add',
          key: `${ENTITIES_KEY_PREFIX}users/u1`,
          newValue: {id: 'u1', name: 'Alice'},
        },
        {
          op: 'change',
          key: `${ENTITIES_KEY_PREFIX}users/u1`,
          oldValue: {id: 'u1', name: 'Alice'},
          newValue: {id: 'u1', name: 'Alicia'},
        },
        {
          op: 'add',
          key: `${ENTITIES_KEY_PREFIX}users/u1`,
          newValue: {id: 'u1', name: 'Alice'},
        },
      ]);
    } catch (e) {
      originalError = e;
    }

    expect(originalError).toBeInstanceOf(Error);
    expect(String(originalError)).toContain('Row already exists');

    try {
      branch.advance('head1' as Hash, 'head2' as Hash, []);
      expect.fail('Expected poisoned branch advance to throw');
    } catch (e) {
      expect(e).toBe(originalError);
    }

    try {
      branch.getSource('users');
      expect.fail('Expected poisoned branch getSource to throw');
    } catch (e) {
      expect(e).toBe(originalError);
    }

    try {
      branch.fork();
      expect.fail('Expected poisoned branch fork to throw');
    } catch (e) {
      expect(e).toBe(originalError);
    }

    await expect(branch.forkToHead(dagStore, 'head2' as Hash)).rejects.toBe(
      originalError,
    );
  });
});

test('advance bulk loads empty sources and matches per-row pushes', () => {
  const tables = {
    users: {
      name: 'users',
      columns: {id: {type: 'string'}, name: {type: 'string'}},
      primaryKey: ['id'],
    },
    // Compound keys are hashed in the replicache key so the scan order is
    // not primary key order.
    likes: {
      name: 'likes',
      columns: {a: {type: 'number'}, b: {type: 'number'}},
      primaryKey: ['a', 'b'],
    },
  } as const;
  const e = ENTITIES_KEY_PREFIX;
  const diffs = [
    {op: 'add', key: `${e}likes/h1`, newValue: {a: 2, b: 1}},
    {op: 'add', key: `${e}likes/h2`, newValue: {a: 1, b: 2}},
    {op: 'add', key: `${e}likes/h3`, newValue: {a: 1, b: 1}},
    {op: 'add', key: `${e}users/u1`, newValue: {id: 'u1', name: 'A'}},
    {op: 'add', key: `${e}users/u2`, newValue: {id: 'u2', name: 'B'}},
    {
      op: 'change',
      key: `${e}users/u3`,
      oldValue: {id: 'u3', name: 'C'},
      newValue: {id: 'u3', name: 'C2'},
    },
  ] as const;

  const bulk = new IVMSourceBranch(tables);
  // u3 does not exist: the edit must still go through push and fail, showing
  // the bulk run stopped at the first non-add.
  expect(() => bulk.advance(undefined, 'h' as Hash, diffs.slice())).toThrow(
    'Row not found',
  );

  const bulk2 = new IVMSourceBranch(tables);
  bulk2.advance(undefined, 'h' as Hash, diffs.slice(0, 5));

  const pushed = new IVMSourceBranch(tables);
  // Connecting first forces the per-row push path.
  pushed.getSource('users')!.connect([['id', 'asc']]);
  pushed.getSource('likes')!.connect([
    ['a', 'asc'],
    ['b', 'asc'],
  ]);
  pushed.advance(undefined, 'h' as Hash, diffs.slice(0, 5));

  for (const name of ['users', 'likes']) {
    expect([...bulk2.getSource(name)!.data]).toEqual([
      ...pushed.getSource(name)!.data,
    ]);
  }
  expect([...bulk2.getSource('likes')!.data]).toEqual([
    {a: 1, b: 1},
    {a: 1, b: 2},
    {a: 2, b: 1},
  ]);
});

describe('forkToHead', () => {
  test('handles when desiredHead is the same as expectedHead', async () => {
    const branch = new IVMSourceBranch(schema.tables);
    const {dagStore, syncHash} = await createDb(
      [
        [
          `${ENTITIES_KEY_PREFIX}issue/sdf`,
          {
            id: 'sdf',
            title: 'test',
            description: 'test',
            closed: false,
            ownerId: null,
            createdAt: 1743018138477,
          },
        ],
      ],
      timestamp++,
    );
    await initFromStore(branch, syncHash, dagStore);
    await branch.forkToHead(dagStore, syncHash);
    expect([
      ...must(branch.getSource('issue'))
        .connect([['id', 'asc']])
        .fetch({}),
    ]).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "closed": false,
            "createdAt": 1743018138477,
            "description": "test",
            "id": "sdf",
            "ownerId": null,
            "title": "test",
          },
        },
      ]
    `);
  });

  test('returns a fork that matches the desired head', async () => {
    const branch = new IVMSourceBranch(schema.tables);
    const {dagStore, syncHash} = await createDb([], timestamp++);
    await initFromStore(branch, syncHash, dagStore);

    const w = await newWriteLocal(
      syncHash,
      'mutator_name',
      JSON.stringify([]),
      null,
      await dagStore.write(),
      timestamp++,
      'client-id',
      FormatVersion.Latest,
    );
    await w.put(lc, `${ENTITIES_KEY_PREFIX}issue/sdf`, {
      id: 'sdf',
      title: 'test',
      description: 'test',
      closed: false,
      ownerId: null,
    } as unknown as FrozenJSONValue);
    const head = await w.commit(SYNC_HEAD_NAME);

    const fork = await branch.forkToHead(dagStore, head);
    expect([
      ...must(fork.getSource('issue'))
        .connect([['id', 'asc']])
        .fetch({}),
    ]).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "closed": false,
            "description": "test",
            "id": "sdf",
            "ownerId": null,
            "title": "test",
          },
        },
      ]
    `);

    // can also re-wind the fork to the original head
    const fork2 = await fork.forkToHead(dagStore, syncHash);
    expect([
      ...must(fork2.getSource('issue'))
        .connect([['id', 'asc']])
        .fetch({}),
    ]).toMatchInlineSnapshot(`
      []
    `);
  });
});

test('advance releases an aggregate source the server empties once no view reads it', () => {
  const name = 'aggregate:q:comments';
  const e = ENTITIES_KEY_PREFIX;
  const row = {issueID: 'i1', value: 2, ['_0_version']: '01'};
  const add = {op: 'add', key: `${e}${name}/i1`, newValue: row} as const;
  const del = {op: 'del', key: `${e}${name}/i1`, oldValue: row} as const;

  const branch = new IVMSourceBranch({});
  const source = branch.getOrCreateAggregateSource(
    name,
    {issueID: {type: 'string'}, value: {type: 'number'}},
    ['issueID'],
    {
      table: 'comment',
      childField: ['issueID'],
      keyField: ['issueID'],
      fn: 'count',
      field: undefined,
      predicate: undefined,
    },
  );
  const registrations = () => [...branch.getOptimisticAggregates('comment')];

  // While a view reads it, an emptied source stays.
  const conn = source.connect([['issueID', 'asc']]);
  branch.advance(undefined, 'h1' as Hash, [add]);
  branch.advance('h1' as Hash, 'h2' as Hash, [del]);
  expect(branch.getSource(name)).toBe(source);
  expect(registrations()).toHaveLength(1);

  // Once nothing reads it, emptying it drops the source and its registration.
  conn.destroy();
  branch.advance('h2' as Hash, 'h3' as Hash, [add]);
  branch.advance('h3' as Hash, 'h4' as Hash, [del]);
  expect(branch.getSource(name)).toBeUndefined();
  expect(registrations()).toHaveLength(0);
});

test('advance releases an aggregate source destroyed before it got a row', () => {
  const name = 'aggregate:q:comments';
  const branch = new IVMSourceBranch({});
  const source = branch.getOrCreateAggregateSource(
    name,
    {issueID: {type: 'string'}, value: {type: 'number'}},
    ['issueID'],
    {
      table: 'comment',
      childField: ['issueID'],
      keyField: ['issueID'],
      fn: 'count',
      field: undefined,
      predicate: undefined,
    },
  );
  source.connect([['issueID', 'asc']]).destroy();
  branch.advance(undefined, 'h1' as Hash, []);
  expect(branch.getSource(name)).toBeUndefined();
  expect([...branch.getOptimisticAggregates('comment')]).toHaveLength(0);
});

test('an aggregate source emptied while read is released by a later advance once the view is gone', () => {
  const name = 'aggregate:q:comments';
  const e = ENTITIES_KEY_PREFIX;
  const row = {issueID: 'i1', value: 2, ['_0_version']: '01'};
  const add = {op: 'add', key: `${e}${name}/i1`, newValue: row} as const;
  const del = {op: 'del', key: `${e}${name}/i1`, oldValue: row} as const;
  const other = {
    op: 'add',
    key: `${e}aggregate:other:comments/i1`,
    newValue: row,
  } as const;

  const branch = new IVMSourceBranch({});
  const source = branch.getOrCreateAggregateSource(
    name,
    {issueID: {type: 'string'}, value: {type: 'number'}},
    ['issueID'],
    {
      table: 'comment',
      childField: ['issueID'],
      keyField: ['issueID'],
      fn: 'count',
      field: undefined,
      predicate: undefined,
    },
  );
  const registrations = () => [...branch.getOptimisticAggregates('comment')];

  // The server empties the source while a view reads it, then the view goes.
  const conn = source.connect([['issueID', 'asc']]);
  branch.advance(undefined, 'h1' as Hash, [add]);
  branch.advance('h1' as Hash, 'h2' as Hash, [del]);
  conn.destroy();
  expect(branch.getSource(name)).toBe(source);

  // The next advance releases it, even though it touches no row of it.
  branch.advance('h2' as Hash, 'h3' as Hash, [other]);
  expect(branch.getSource(name)).toBeUndefined();
  expect(registrations()).toHaveLength(0);
});

test('an aggregate source refilled before its view goes is kept', () => {
  const name = 'aggregate:q:comments';
  const e = ENTITIES_KEY_PREFIX;
  const row = {issueID: 'i1', value: 2, ['_0_version']: '01'};
  const add = {op: 'add', key: `${e}${name}/i1`, newValue: row} as const;
  const del = {op: 'del', key: `${e}${name}/i1`, oldValue: row} as const;

  const branch = new IVMSourceBranch({});
  const source = branch.getOrCreateAggregateSource(
    name,
    {issueID: {type: 'string'}, value: {type: 'number'}},
    ['issueID'],
  );
  const conn = source.connect([['issueID', 'asc']]);
  branch.advance(undefined, 'h1' as Hash, [add]);
  branch.advance('h1' as Hash, 'h2' as Hash, [del]);
  branch.advance('h2' as Hash, 'h3' as Hash, [add]);
  conn.destroy();
  branch.advance('h3' as Hash, 'h4' as Hash, []);
  expect(branch.getSource(name)).toBe(source);
});

test('only the main branch registers optimistic aggregates', () => {
  const optimistic = {
    table: 'comment',
    childField: ['issueID'],
    keyField: ['issueID'],
    fn: 'count',
    field: undefined,
    predicate: undefined,
  } as const;
  const columns = {
    issueID: {type: 'string'},
    _0_value: {type: 'number'},
  } as const;
  const branch = new IVMSourceBranch({});
  const registrations = () => [...branch.getOptimisticAggregates('comment')];

  // A query materialized on a fork (a mutator's `tx.run`) is never synced, so
  // nothing would ever release its registration.
  const fork = branch.fork();
  fork.getOrCreateAggregateSource(
    'aggregate:q1:comments',
    columns,
    ['issueID'],
    optimistic,
  );
  expect(registrations()).toHaveLength(0);

  // The fork shares the registrations of the main branch's views.
  branch.getOrCreateAggregateSource(
    'aggregate:q2:comments',
    columns,
    ['issueID'],
    optimistic,
  );
  expect(registrations().map(r => r.aggTableName)).toEqual([
    'aggregate:q2:comments',
  ]);
  expect(
    Array.from(
      branch.fork().getOptimisticAggregates('comment'),
      r => r.aggTableName,
    ),
  ).toEqual(['aggregate:q2:comments']);
});
