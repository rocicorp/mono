import {
  afterEach,
  beforeEach,
  describe,
  expect,
  expectTypeOf,
  test,
  vi,
} from 'vitest';
import {schema} from '../../../zql/src/query/test/test-schemas.ts';
import {
  TransactionImpl,
  type CustomMutatorDefs,
  type MakeCustomMutatorInterfaces,
} from './custom.ts';
import {MockSocket, zeroForTest} from './test-utils.ts';
import type {InsertValue, Transaction} from '../../../zql/src/mutate/custom.ts';
import {IVMSourceBranch} from './ivm-branch.ts';
import {createDb} from './test/create-db.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {WriteTransaction} from './replicache-types.ts';
import {zeroData} from '../../../replicache/src/transactions.ts';
import {must} from '../../../shared/src/must.ts';
import type {MutationResult} from '../../../zero-protocol/src/push.ts';
import * as ConnectionState from './connection-state-enum.ts';

type Schema = typeof schema;

test('argument types are preserved on the generated mutator interface', () => {
  const mutators = {
    issue: {
      setTitle: (tx, {id, title}: {id: string; title: string}) =>
        tx.mutate.issue.update({id, title}),
      setProps: (
        tx,
        {
          id,
          title,
          status,
          assignee,
        }: {
          id: string;
          title: string;
          status: 'open' | 'closed';
          assignee: string;
        },
      ) =>
        tx.mutate.issue.update({
          id,
          title,
          closed: status === 'closed',
          ownerId: assignee,
        }),
    },
    nonTableNamespace: {
      doThing: (_tx, _a: {arg1: string; arg2: number}) => {
        throw new Error('not implemented');
      },
    },
  } satisfies CustomMutatorDefs<Schema>;

  type MutatorsInterface = MakeCustomMutatorInterfaces<Schema, typeof mutators>;

  expectTypeOf<MutatorsInterface>().toEqualTypeOf<{
    readonly issue: {
      readonly setTitle: (args: {
        id: string;
        title: string;
      }) => Promise<{server?: Promise<MutationResult>}>;
      readonly setProps: (args: {
        id: string;
        title: string;
        status: 'closed' | 'open';
        assignee: string;
      }) => Promise<{server?: Promise<MutationResult>}>;
    };
    readonly nonTableNamespace: {
      readonly doThing: (_a: {
        arg1: string;
        arg2: number;
      }) => Promise<{server?: Promise<MutationResult>}>;
    };
  }>();
});

test('supports mutators without a namespace', async () => {
  const z = zeroForTest({
    logLevel: 'debug',
    schema,
    mutators: {
      createIssue: async (
        tx: Transaction<Schema>,
        args: InsertValue<typeof schema.tables.issue>,
      ) => {
        await tx.mutate.issue.insert(args);
      },
    },
  });

  await z.mutate.createIssue({
    id: '1',
    title: 'no-namespace',
    closed: false,
    ownerId: '',
    description: '',
  });

  const issues = await z.query.issue.run();
  expect(issues[0].title).toEqual('no-namespace');
});

test('detects collisions in mutator names', () => {
  expect(() =>
    zeroForTest({
      logLevel: 'debug',
      schema,
      mutators: {
        'issue': {
          create: async (
            tx: Transaction<Schema>,
            args: InsertValue<typeof schema.tables.issue>,
          ) => {
            await tx.mutate.issue.insert(args);
          },
        },
        'issue|create': async (
          tx: Transaction<Schema>,
          args: InsertValue<typeof schema.tables.issue>,
        ) => {
          await tx.mutate.issue.insert(args);
        },
      },
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: A mutator, or mutator namespace, has already been defined for issue|create]`,
  );
});

test('custom mutators write to the local store', async () => {
  const z = zeroForTest({
    logLevel: 'debug',
    schema,
    mutators: {
      issue: {
        setTitle: async (tx, {id, title}: {id: string; title: string}) => {
          await tx.mutate.issue.update({id, title});
        },
        deleteTwoIssues: async (tx, {id1, id2}: {id1: string; id2: string}) => {
          await Promise.all([
            tx.mutate.issue.delete({id: id1}),
            tx.mutate.issue.delete({id: id2}),
          ]);
        },
        create: async (tx, args: InsertValue<typeof schema.tables.issue>) => {
          await tx.mutate.issue.insert(args);
        },
      },
      customNamespace: {
        clown: async (tx, id: string) => {
          await tx.mutate.issue.update({id, title: '🤡'});
        },
      },
    } satisfies CustomMutatorDefs<Schema>,
  });

  await z.mutate.issue.create({
    id: '1',
    title: 'foo',
    closed: false,
    ownerId: '',
    description: '',
  });

  let issues = await z.query.issue.run();
  expect(issues[0].title).toEqual('foo');

  await z.mutate.issue.setTitle({id: '1', title: 'bar'});
  issues = await z.query.issue.run();
  expect(issues[0].title).toEqual('bar');

  await z.mutate.customNamespace.clown('1');
  issues = await z.query.issue.run();
  expect(issues[0].title).toEqual('🤡');

  await z.mutate.issue.create({
    id: '2',
    title: 'foo',
    closed: false,
    ownerId: '',
    description: '',
  });
  issues = await z.query.issue.run();
  expect(issues.length).toEqual(2);

  await z.mutate.issue.deleteTwoIssues({id1: issues[0].id, id2: issues[1].id});
  issues = await z.query.issue.run();
  expect(issues.length).toEqual(0);
});

test('custom mutators can query the local store during an optimistic mutation', async () => {
  const z = zeroForTest({
    schema,
    mutators: {
      issue: {
        create: async (tx, args: InsertValue<typeof schema.tables.issue>) => {
          await tx.mutate.issue.insert(args);
        },
        closeAll: async tx => {
          const issues = await tx.query.issue.run();
          await Promise.all(
            issues.map(issue =>
              tx.mutate.issue.update({id: issue.id, closed: true}),
            ),
          );
        },
      },
    } as const satisfies CustomMutatorDefs<Schema>,
  });

  await Promise.all(
    Array.from({length: 10}, async (_, i) => {
      await z.mutate.issue.create({
        id: i.toString().padStart(3, '0'),
        title: `issue ${i}`,
        closed: false,
        description: '',
        ownerId: '',
      });
    }),
  );
  let issues = await z.query.issue.where('closed', false).run();
  expect(issues.length).toEqual(10);

  await z.mutate.issue.closeAll();

  issues = await z.query.issue.where('closed', false).run();
  expect(issues.length).toEqual(0);
});

describe('rebasing custom mutators', () => {
  let branch: IVMSourceBranch;
  beforeEach(async () => {
    const {syncHash} = await createDb([], 42);
    branch = new IVMSourceBranch(schema.tables);
    await branch.advance(undefined, syncHash, []);
  });

  test('mutations write to the rebase branch', async () => {
    const tx1 = new TransactionImpl(
      createSilentLogContext(),
      {
        reason: 'rebase',
        has: () => false,
        set: () => {},
        [zeroData]: {
          ivmSources: branch,
        },
      } as unknown as WriteTransaction,
      schema,
      10,
    ) as unknown as Transaction<Schema>;

    await tx1.mutate.issue.insert({
      closed: false,
      description: '',
      id: '1',
      ownerId: '',
      title: 'foo',
    });

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
            "description": "",
            "id": "1",
            "ownerId": "",
            "title": "foo",
          },
        },
      ]
    `);
  });

  test('mutations can read their own writes', async () => {
    const z = zeroForTest({
      schema,
      mutators: {
        issue: {
          createAndReadCreated: async (
            tx,
            args: InsertValue<typeof schema.tables.issue>,
          ) => {
            await tx.mutate.issue.insert(args);
            const readIssue = must(
              await tx.query.issue.where('id', args.id).one().run(),
            );
            await tx.mutate.issue.update({
              ...readIssue,
              title: readIssue.title + ' updated',
              description: 'updated',
            });
          },
        },
      } as const satisfies CustomMutatorDefs<Schema>,
    });

    await z.mutate.issue.createAndReadCreated({
      id: '1',
      title: 'foo',
      description: '',
      closed: false,
    });

    const issue = must(await z.query.issue.where('id', '1').one().run());
    expect(issue.title).toEqual('foo updated');
    expect(issue.description).toEqual('updated');
  });

  test('mutations on main do not change main until they are committed', async () => {
    let mutationRun = false;
    const z = zeroForTest({
      schema,
      mutators: {
        issue: {
          create: async (tx, args: InsertValue<typeof schema.tables.issue>) => {
            await tx.mutate.issue.insert(args);
            // query main. The issue should not be there yet.
            expect(await z.query.issue.run()).length(0);
            // but it is in this tx
            expect(await tx.query.issue.run()).length(1);

            mutationRun = true;
          },
        },
      } as const satisfies CustomMutatorDefs<Schema>,
    });

    await z.mutate.issue.create({
      id: '1',
      title: 'foo',
      closed: false,
      description: '',
      ownerId: '',
    });

    expect(mutationRun).toEqual(true);
  });
});

describe('server results and keeping read queries', () => {
  beforeEach(() => {
    vi.stubGlobal('WebSocket', MockSocket as unknown as typeof WebSocket);
    vi.stubGlobal('fetch', () => Promise.resolve(new Response()));
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  test('waiting for server results', async () => {
    const z = zeroForTest({
      schema,
      mutators: {
        issue: {
          create: async (
            _tx,
            _args: InsertValue<typeof schema.tables.issue>,
          ) => {},

          close: async (_tx, _args: object) => {},
        },
      } as const satisfies CustomMutatorDefs<Schema>,
    });

    await z.triggerConnected();
    await z.waitForConnectionState(ConnectionState.Connected);

    const create = await z.mutate.issue.create({
      id: '1',
      title: 'foo',
      closed: false,
      description: '',
      ownerId: '',
    });

    await z.triggerPushResponse({
      mutations: [
        {
          id: {clientID: z.clientID, id: 1},
          result: {
            data: {
              shortID: '1',
            },
          },
        },
      ],
    });

    expect(await create.server).toEqual({data: {shortID: '1'}});

    const close = await z.mutate.issue.close({});

    await z.triggerPushResponse({
      mutations: [
        {
          id: {clientID: z.clientID, id: 2},
          result: {
            error: 'app',
          },
        },
      ],
    });

    await z.close();

    await expect(close.server).rejects.toEqual({error: 'app'});
  });

  test('changeDesiredQueries:remove is not sent while there are pending mutations', async () => {
    function filter(messages: string[]) {
      return messages.filter(m => m.includes('changeDesiredQueries'));
    }

    const z = zeroForTest({
      schema,
      mutators: {
        issue: {
          create: async (
            tx,
            _args: InsertValue<typeof schema.tables.issue>,
          ) => {
            await tx.query.issue.run();
          },

          close: async (tx, _args: object) => {
            await tx.query.issue.limit(1).run();
          },
        },
      } as const satisfies CustomMutatorDefs<Schema>,
    });

    const mockSocket = await z.socket;
    const messages: string[] = [];
    mockSocket.onUpstream = msg => {
      messages.push(msg);
    };

    await z.triggerConnected();
    await z.waitForConnectionState(ConnectionState.Connected);

    await z.mutate.issue.create({
      id: '1',
      title: 'foo',
      closed: false,
      description: '',
      ownerId: '',
    });

    const q = z.query.issue.limit(1).materialize();
    q.destroy();

    // tick a time to be sure everything is collected
    await new Promise(resolve => setTimeout(resolve, 0));

    // query is not removed, only put.
    expect(filter(messages)).toMatchInlineSnapshot(`
      [
        "["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"put","hash":"1vsd9vcx6ynd4","ast":{"table":"issues","limit":1,"orderBy":[["id","asc"]]},"ttl":0}]}]",
      ]
    `);
    messages.length = 0;

    await z.triggerPushResponse({
      mutations: [
        {
          id: {clientID: z.clientID, id: 1},
          result: {},
        },
      ],
    });

    // mutation is no longer outstanding, query is removed.
    expect(filter(messages)).toMatchInlineSnapshot(`
      [
        "["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"del","hash":"1vsd9vcx6ynd4"}]}]",
      ]
    `);
    messages.length = 0;

    // check the error case
    const q2 = z.query.issue.materialize();
    const close = await z.mutate.issue.close({});
    q2.destroy();
    // tick a time to be sure everything is collected
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(filter(messages)).toMatchInlineSnapshot(`
      [
        "["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"put","hash":"12hwg3ihkijhm","ast":{"table":"issues","orderBy":[["id","asc"]]},"ttl":0}]}]",
      ]
    `);
    messages.length = 0;

    await z.triggerPushResponse({
      mutations: [
        {
          id: {clientID: z.clientID, id: 2},
          result: {
            error: 'app',
          },
        },
      ],
    });

    expect(messages).toMatchInlineSnapshot(`
      [
        "["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"del","hash":"12hwg3ihkijhm"}]}]",
      ]
    `);
    messages.length = 0;

    await z.close();
    await expect(close.server).rejects.toEqual({error: 'app'});
  });
});
