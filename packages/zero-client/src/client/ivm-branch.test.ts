import {describe, expect, test} from 'vitest';
import {initFromStore, IVMSourceBranch} from './ivm-branch.ts';
import {
  schema,
  type Issue,
  type IssueLabel,
  type Label,
  type Revision,
} from '../../../zql/src/query/test/test-schemas.ts';
import * as FormatVersion from '../../../replicache/src/format-version-enum.ts';
import {must} from '../../../shared/src/must.ts';
import {SYNC_HEAD_NAME} from '../../../replicache/src/sync/sync-head-name.ts';
import {newWriteLocal} from '../../../replicache/src/db/write.ts';

import {ENTITIES_KEY_PREFIX} from './keys.ts';
import type {FrozenJSONValue} from '../../../replicache/src/frozen-json.ts';
import type {Diff} from '../../../replicache/src/sync/patch.ts';
import type {Node} from '../../../zql/src/ivm/data.ts';
import {createDb} from './test/create-db.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {consume} from '../../../zql/src/ivm/stream.ts';

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
  consume(
    usersSource.push({
      type: 'add',
      row: {id: 'u1', name: 'Alice'},
    }),
  );

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
  consume(
    usersSource.push({
      type: 'add',
      row: {id: 'u2', name: 'Bob'},
    }),
  );

  // Mutate fork differently
  consume(
    fork.getSource('users')!.push({
      type: 'add',
      row: {id: 'u3', name: 'Charlie'},
    }),
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
