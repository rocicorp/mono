import {describe, expect, test} from 'vitest';
import {IVMSourceBranch, IVMSourceRepo} from './ivm-source-repo.ts';
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
  usersSource.push({
    type: 'add',
    row: {id: 'u1', name: 'Alice'},
  });

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
  usersSource.push({
    type: 'add',
    row: {id: 'u2', name: 'Bob'},
  });

  // Mutate fork differently
  fork.getSource('users')!.push({
    type: 'add',
    row: {id: 'u3', name: 'Charlie'},
  });

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
describe('advanceSyncHead', () => {
  test("sync head is initialized to match replicache's sync head", async () => {
    const repo = new IVMSourceRepo(schema.tables);
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
          },
        ],
      ],
      timestamp++,
    );
    await repo.advanceSyncHead(dagStore, syncHash, []);

    const branches = await repo.getSourcesForTransaction('rebase', undefined);

    for (const branch of Object.values(branches)) {
      expect([
        ...must(branch?.getSource('issue'))
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
    }
  });

  test('sync is advanced via diffs when already initialized', async () => {
    const repo = new IVMSourceRepo(schema.tables);
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
          },
        ],
      ],
      timestamp++,
    );

    // initialize the sync head
    await repo.advanceSyncHead(dagStore, syncHash, []);

    // write something to the db that will not be included in diffs and should not be in the ivm sync head for
    // that reason. This would never happen in practice.
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
    await w.put(lc, `${ENTITIES_KEY_PREFIX}issue/abc`, {
      id: 'abc',
      title: 'test',
      description: 'test',
      closed: false,
      ownerId: null,
    } as unknown as FrozenJSONValue);
    await w.commit(SYNC_HEAD_NAME);

    await repo.advanceSyncHead(dagStore, syncHash, [
      {
        op: 'add',
        key: `${ENTITIES_KEY_PREFIX}issue/def`,
        newValue: {
          id: 'def',
          title: 'test',
          description: 'test',
          closed: false,
          ownerId: null,
        } as unknown as FrozenJSONValue,
      },
    ]);

    const branches = repo.getSourcesForTransaction('rebase', undefined);
    for (const branch of Object.values(branches)) {
      expect([
        ...must(branch?.getSource('issue'))
          .connect([['id', 'asc']])
          .fetch({}),
      ]).toMatchInlineSnapshot(`
        [
          {
            "relationships": {},
            "row": {
              "closed": false,
              "description": "test",
              "id": "def",
              "ownerId": null,
              "title": "test",
            },
          },
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
    }
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
            } as unknown as FrozenJSONValue,
            newValue: {
              id: 'sdf',
              title: 'test',
              description: 'test',
              closed: true,
              ownerId: null,
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
            } as unknown as FrozenJSONValue,
            newValue: {
              id: 'sdf',
              title: 'changed',
              description: 'changed',
              closed: true,
              ownerId: null,
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
            } as unknown as FrozenJSONValue,
          },
        ],
        expected: [],
      },
      // remove existing data
    ] satisfies DiffCase[])('$name', async ({initial, diffs, expected}) => {
      const repo = new IVMSourceRepo(schema.tables);
      const {dagStore, syncHash} = await createDb(initial, timestamp++);
      await repo.advanceSyncHead(dagStore, syncHash, []);

      await repo.advanceSyncHead(dagStore, syncHash, diffs);

      const branches = repo.getSourcesForTransaction('rebase', undefined);
      for (const branch of Object.values(branches)) {
        expect([
          ...must(branch?.getSource('issue'))
            .connect([['id', 'asc']])
            .fetch({}),
        ]).toEqual(expected);
      }
    });
  });
});
