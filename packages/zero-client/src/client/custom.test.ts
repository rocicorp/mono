import {
  afterEach,
  assert,
  beforeEach,
  describe,
  expect,
  expectTypeOf,
  test,
  vi,
} from 'vitest';
import {zeroData} from '../../../replicache/src/transactions.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import {promiseUndefined} from '../../../shared/src/resolved-promises.ts';
import {refCountSymbol} from '../../../zql/src/ivm/view-apply-change.ts';
import type {InsertValue, Transaction} from '../../../zql/src/mutate/custom.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import type {Row} from '../../../zql/src/query/query.ts';
import {schema} from '../../../zql/src/query/test/test-schemas.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import {ConnectionStatus} from './connection-status.ts';
import {
  TransactionImpl,
  type MakeCustomMutatorInterfaces,
  type MutatorResult,
} from './custom.ts';
import {ClientError} from './error.ts';
import {IVMSourceBranch} from './ivm-branch.ts';
import type {WriteTransaction} from './replicache-types.ts';
import {asCustomQuery, MockSocket, queryID, zeroForTest} from './test-utils.ts';
import {createDb} from './test/create-db.ts';
import {getInternalReplicacheImplForTesting} from './zero.ts';

type Schema = typeof schema;
type MutatorTx = Transaction<Schema>;

afterEach(() => vi.restoreAllMocks());

test('argument types are preserved on the generated mutator interface', () => {
  const mutators = {
    issue: {
      setTitle: (tx: MutatorTx, {id, title}: {id: string; title: string}) =>
        tx.mutate.issue.update({id, title}),
      setProps: (
        tx: MutatorTx,
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
      doThing: (_tx: MutatorTx, _a: {arg1: string; arg2: number}) => {
        throw new Error('not implemented');
      },
    },
  } as const;

  type MutatorsInterface = MakeCustomMutatorInterfaces<
    Schema,
    typeof mutators,
    unknown
  >;

  expectTypeOf<MutatorsInterface>().toEqualTypeOf<{
    readonly issue: {
      readonly setTitle: (args: {id: string; title: string}) => MutatorResult;
      readonly setProps: (args: {
        id: string;
        title: string;
        status: 'closed' | 'open';
        assignee: string;
      }) => MutatorResult;
    };
    readonly nonTableNamespace: {
      readonly doThing: (_a: {arg1: string; arg2: number}) => MutatorResult;
    };
  }>();
});

test('argument types are preserved with arbitrary depth nesting', () => {
  const mutators = {
    level1: {
      level2: {
        level3: {
          deepMutator: (
            tx: MutatorTx,
            {id, value}: {id: string; value: number},
          ) => tx.mutate.issue.update({id, closed: value > 0}),
        },
        intermediateMutator: (tx: MutatorTx, id: string) =>
          tx.mutate.issue.delete({id}),
      },
      topMutator: (
        _tx: MutatorTx,
        _args: {a: boolean; b: string; c?: number | undefined},
      ) => Promise.resolve(),
    },
    flatMutator: (_tx: MutatorTx) => Promise.resolve(),
  } as const;

  type MutatorsInterface = MakeCustomMutatorInterfaces<
    Schema,
    typeof mutators,
    unknown
  >;

  expectTypeOf<MutatorsInterface>().toEqualTypeOf<{
    readonly level1: {
      readonly level2: {
        readonly level3: {
          readonly deepMutator: (args: {
            id: string;
            value: number;
          }) => MutatorResult;
        };
        readonly intermediateMutator: (id: string) => MutatorResult;
      };
      readonly topMutator: (args: {
        a: boolean;
        b: string;
        c?: number | undefined;
      }) => MutatorResult;
    };
    readonly flatMutator: () => MutatorResult;
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
    createdAt: 1743018138477,
  }).client;

  const zql = createBuilder(schema);
  const issues = await z.run(zql.issue);
  expect(issues[0].title).toEqual('no-namespace');
});

test('supports arbitrary depth nesting of mutators', async () => {
  const z = zeroForTest({
    logLevel: 'debug',
    schema,
    mutators: {
      level1: {
        level2: {
          level3: {
            createIssue: async (
              tx: Transaction<Schema>,
              args: InsertValue<typeof schema.tables.issue>,
            ) => {
              await tx.mutate.issue.insert(args);
            },
            updateTitle: async (
              tx: Transaction<Schema>,
              {id, title}: {id: string; title: string},
            ) => {
              await tx.mutate.issue.update({id, title});
            },
          },
          anotherMutator: async (
            tx: Transaction<Schema>,
            args: InsertValue<typeof schema.tables.issue>,
          ) => {
            await tx.mutate.issue.insert(args);
          },
        },
        directMutator: async (tx: Transaction<Schema>, id: string) => {
          await tx.mutate.issue.update({id, closed: true});
        },
      },
      topLevel: async (tx: Transaction<Schema>, id: string) => {
        await tx.mutate.issue.update({id, title: 'top-level'});
      },
    },
  });

  // Test deeply nested mutator
  await z.mutate.level1.level2.level3.createIssue({
    id: '1',
    title: 'deeply-nested',
    closed: false,
    ownerId: '',
    description: '',
    createdAt: 1743018138477,
  }).client;

  const zql = createBuilder(schema);

  await z.markQueryAsGot(zql.issue);
  let issues = await z.run(zql.issue);
  expect(issues[0].title).toEqual('deeply-nested');

  // Test deeply nested update
  await z.mutate.level1.level2.level3.updateTitle({
    id: '1',
    title: 'updated-deep',
  }).client;
  issues = await z.run(zql.issue);
  expect(issues[0].title).toEqual('updated-deep');

  // Test intermediate level mutator
  await z.mutate.level1.level2.anotherMutator({
    id: '2',
    title: 'intermediate',
    closed: false,
    ownerId: '',
    description: '',
    createdAt: 1743018138477,
  }).client;
  issues = await z.run(zql.issue);
  expect(issues.length).toEqual(2);
  expect(issues[1].title).toEqual('intermediate');

  // Test level1 direct mutator
  await z.mutate.level1.directMutator('2').client;
  issues = await z.run(zql.issue);
  expect(issues[1].closed).toEqual(true);

  // Test top level mutator
  await z.mutate.topLevel('1').client;
  issues = await z.run(zql.issue);
  expect(issues[0].title).toEqual('top-level');
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
    `[Error: mutator names/namespaces must not include a |]`,
  );
});

test('custom mutators write to the local store', async () => {
  const z = zeroForTest({
    logLevel: 'debug',
    schema,
    mutators: {
      issue: {
        setTitle: async (
          tx: MutatorTx,
          {id, title}: {id: string; title: string},
        ) => {
          await tx.mutate.issue.update({id, title});
        },
        deleteTwoIssues: async (
          tx: MutatorTx,
          {id1, id2}: {id1: string; id2: string},
        ) => {
          await Promise.all([
            tx.mutate.issue.delete({id: id1}),
            tx.mutate.issue.delete({id: id2}),
          ]);
        },
        create: async (
          tx: MutatorTx,
          args: InsertValue<typeof schema.tables.issue>,
        ) => {
          await tx.mutate.issue.insert(args);
        },
      },
      customNamespace: {
        clown: async (tx: MutatorTx, id: string) => {
          await tx.mutate.issue.update({id, title: '🤡'});
        },
      },
    } as const,
  });

  await z.mutate.issue.create({
    id: '1',
    title: 'foo',
    closed: false,
    ownerId: '',
    description: '',
    createdAt: 1743018138477,
  }).client;

  const zql = createBuilder(schema);

  await z.markQueryAsGot(zql.issue);
  let issues = await z.run(zql.issue);
  expect(issues[0].title).toEqual('foo');

  await z.mutate.issue.setTitle({id: '1', title: 'bar'}).client;
  issues = await z.run(zql.issue);
  expect(issues[0].title).toEqual('bar');

  await z.mutate.customNamespace.clown('1').client;
  issues = await z.run(zql.issue);
  expect(issues[0].title).toEqual('🤡');

  await z.mutate.issue.create({
    id: '2',
    title: 'foo',
    closed: false,
    ownerId: '',
    description: '',
    createdAt: 1743018138477,
  }).client;
  issues = await z.run(zql.issue);
  expect(issues.length).toEqual(2);

  await z.mutate.issue.deleteTwoIssues({id1: issues[0].id, id2: issues[1].id})
    .client;
  issues = await z.run(zql.issue);
  expect(issues.length).toEqual(0);
});

describe('custom mutators can query the local store during an optimistic mutation', () => {
  test.each([
    ['tx.run(tx.query.issue)', (tx: MutatorTx) => tx.run(tx.query.issue)],
    ['tx.query.issue.run()', (tx: MutatorTx) => tx.query.issue.run()],
  ] as const)('%s can read data', async (_, runQuery) => {
    let queryResult: readonly Row<typeof schema.tables.issue>[] | undefined;

    const z = zeroForTest({
      schema,
      mutators: {
        issue: {
          createAndQuery: async (
            tx: MutatorTx,
            args: InsertValue<typeof schema.tables.issue>,
          ) => {
            await tx.mutate.issue.insert(args);
            queryResult = await runQuery(tx);
          },
        },
      } as const,
    });

    await z.mutate.issue.createAndQuery({
      id: '1',
      title: 'test issue',
      closed: false,
      description: '',
      ownerId: '',
      createdAt: 1743018138477,
    }).client;

    expect(queryResult).toEqual([
      expect.objectContaining({id: '1', title: 'test issue'}),
    ]);
  });

  test('closeAll using tx.run', async () => {
    const z = zeroForTest({
      schema,
      mutators: {
        issue: {
          create: async (
            tx: MutatorTx,
            args: InsertValue<typeof schema.tables.issue>,
          ) => {
            await tx.mutate.issue.insert(args);
          },
          closeAll: async (tx: MutatorTx) => {
            const issues = await tx.run(tx.query.issue);
            await Promise.all(
              issues.map(issue =>
                tx.mutate.issue.update({id: issue.id, closed: true}),
              ),
            );
          },
        },
      } as const,
    });

    await Promise.all(
      Array.from({length: 10}, async (_, i) => {
        await z.mutate.issue.create({
          id: i.toString().padStart(3, '0'),
          title: `issue ${i}`,
          closed: false,
          description: '',
          ownerId: '',
          createdAt: 1743018138477,
        }).client;
      }),
    );

    const zql = createBuilder(schema);

    const q = zql.issue.where('closed', false);
    await z.markQueryAsGot(q);
    let issues = await z.run(q);
    expect(issues.length).toEqual(10);

    await z.mutate.issue.closeAll().client;

    issues = await z.run(q);
    expect(issues.length).toEqual(0);
  });
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
    ) as unknown as Transaction<Schema>;

    await tx1.mutate.issue.insert({
      closed: false,
      description: '',
      id: '1',
      ownerId: '',
      title: 'foo',
      createdAt: 1743018138477,
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
            "createdAt": 1743018138477,
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
            tx: MutatorTx,
            args: InsertValue<typeof schema.tables.issue>,
          ) => {
            await tx.mutate.issue.insert(args);
            const readIssue = must(
              await tx.run(tx.query.issue.where('id', args.id).one()),
            );
            await tx.mutate.issue.update({
              ...readIssue,
              title: readIssue.title + ' updated',
              description: 'updated',
            });
          },
        },
      } as const,
    });

    await z.mutate.issue.createAndReadCreated({
      id: '1',
      title: 'foo',
      description: '',
      closed: false,
      createdAt: 1743018138477,
    }).client;

    const zql = createBuilder(schema);

    const q = asCustomQuery(zql.issue.where('id', '1').one(), 'a', '1');
    const issue = await z.run(q, {type: 'unknown'});
    expect(issue?.title).toEqual('foo updated');
    expect(issue?.description).toEqual('updated');
    const p = z.run(q, {type: 'complete'});
    let completed = false;
    p.then(
      () => (completed = true),
      () => {},
    );
    await Promise.resolve();
    expect(completed).toEqual(false);

    await z.markQueryAsGot(q);

    // Sanity check that the poke got written to the Dag Store.
    // Pokes are scheduled using raf... give it a macro task.
    await vi.waitFor(async () => {
      const rep = getInternalReplicacheImplForTesting(z);

      expect(await rep.query(tx => tx.has(`g/${queryID(q)}`))).toEqual(true);
    });

    expect(completed).toEqual(true);

    {
      const issue = await p;
      expect(issue?.title).toEqual('foo updated');
      expect(issue?.description).toEqual('updated');
    }
  });

  test('the writes of a mutation are immediately available after awaiting the client promise', async () => {
    const z = zeroForTest({
      schema,
      mutators: {
        issue: {
          create: async (
            tx: MutatorTx,
            args: InsertValue<typeof schema.tables.issue>,
          ) => {
            await tx.mutate.issue.insert(args);
          },
        },
      } as const,
    });

    for (let i = 0; i < 10; i++) {
      await z.mutate.issue.create({
        id: String(i),
        title: 'foo ' + i,
        description: '',
        closed: false,
        createdAt: 1743018138477,
      }).client;

      const zql = createBuilder(schema);

      const result = await z.run(zql.issue.where('id', String(i)).one());
      expect(result?.title).toEqual('foo ' + i);
      expect(result?.id).toEqual(String(i));
    }
  });

  test('mutations on main do not change main until they are committed', async () => {
    let mutationRun = false;

    const zql = createBuilder(schema);

    const z = zeroForTest({
      schema,
      mutators: {
        issue: {
          create: async (
            tx: MutatorTx,
            args: InsertValue<typeof schema.tables.issue>,
          ) => {
            await tx.mutate.issue.insert(args);
            // query main. The issue should not be there yet.
            expect(await z.run(zql.issue)).toHaveLength(0);
            // but it is in this tx
            expect(await tx.run(tx.query.issue)).toHaveLength(1);

            mutationRun = true;
          },
        },
      } as const,
    });

    await z.mutate.issue.create({
      id: '1',
      title: 'foo',
      closed: false,
      description: '',
      ownerId: '',
      createdAt: 1743018138477,
    }).client;

    expect(mutationRun).toEqual(true);
  });
});

describe('error handling', () => {
  test('client-side errors surface as application errors on the client/server promises', async () => {
    const z = zeroForTest({
      schema,
      mutators: {
        issue: {
          // oxlint-disable-next-line require-await
          fail: async (_tx: MutatorTx) => {
            throw new Error('client boom');
          },
        },
      } as const,
    });

    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);

    const result = z.mutate.issue.fail();

    const clientResult = await result.client;
    assert(clientResult.type === 'error');
    expect(clientResult.error.type).toBe('app');
    assert(clientResult.error.type === 'app');
    expect(clientResult.error.details).toBeUndefined();
    expect(clientResult.error.message).toBe('client boom');

    const serverResult = await result.server;
    assert(serverResult.type === 'error');
    expect(serverResult.error.type).toBe('app');
    expect(serverResult.error.message).toBe('client boom');
    assert(serverResult.error.type === 'app');
    expect(serverResult.error.details).toBeUndefined();

    await z.close();
  });

  test('rejects outstanding custom mutation server promises when connection goes offline', async () => {
    const noop = vi.fn(async (_tx: MutatorTx) => {});
    const z = zeroForTest({
      schema,
      mutators: {
        issue: {
          noop,
        },
      } as const,
    });

    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);

    const result = z.mutate.issue.noop();
    await result.client;

    const offlineError = new ClientError({
      kind: ClientErrorKind.Offline,
      message: 'offline',
    });

    z.connectionManager.disconnected(offlineError);

    const serverResult = await result.server;
    assert(serverResult.type === 'error');
    expect(serverResult.error.type).toBe('zero');
    expect(serverResult.error.message).toBe('offline');
    expect(noop).toHaveBeenCalledTimes(1);

    // client promise was already resolved
    const clientResult = await result.client;
    assert(clientResult.type === 'success');

    await z.close();
  });

  test('custom mutators short-circuit while offline and resume after reconnect', async () => {
    const topLevel = vi.fn(async (_tx: MutatorTx) => {});
    const namespaced = vi.fn(async (_tx: MutatorTx, _args: {id: string}) => {});

    const z = zeroForTest({
      schema,
      mutators: {
        topLevel,
        issue: {
          namespaced,
        },
      } as const,
    });

    await z.triggerConnected();

    const offlineError = new ClientError({
      kind: ClientErrorKind.Offline,
      message: 'offline',
    });

    z.connectionManager.disconnected(offlineError);
    await z.waitForConnectionStatus(ConnectionStatus.Disconnected);

    const offlineTop = z.mutate.topLevel();
    const offlineNamespaced = z.mutate.issue.namespaced({id: '123'});

    const offlineTopClient = await offlineTop.client;
    assert(offlineTopClient.type === 'error');
    expect(offlineTopClient.error.type).toBe('zero');
    expect(offlineTopClient.error.message).toBe('offline');

    const offlineTopServer = await offlineTop.server;
    assert(offlineTopServer.type === 'error');
    expect(offlineTopServer.error.type).toBe('zero');
    expect(offlineTopServer.error.message).toBe('offline');

    const offlineNamespacedClient = await offlineNamespaced.client;
    assert(offlineNamespacedClient.type === 'error');
    expect(offlineNamespacedClient.error.type).toBe('zero');

    const offlineNamespacedServer = await offlineNamespaced.server;
    assert(offlineNamespacedServer.type === 'error');
    expect(offlineNamespacedServer.error.type).toBe('zero');

    expect(topLevel).not.toHaveBeenCalled();
    expect(namespaced).not.toHaveBeenCalled();

    z.connectionManager.connected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);

    // wait one tick
    await promiseUndefined;

    const resumedTop = z.mutate.topLevel();
    const resumedNamespaced = z.mutate.issue.namespaced({id: '456'});

    await resumedTop.client;
    await resumedNamespaced.client;
    void resumedTop.server;
    void resumedNamespaced.server;

    expect(topLevel).toHaveBeenCalledTimes(1);
    expect(namespaced).toHaveBeenCalledTimes(1);

    await z.triggerPushResponse({
      mutations: [
        {
          id: {clientID: z.clientID, id: 1},
          result: {},
        },
        {
          id: {clientID: z.clientID, id: 2},
          result: {},
        },
      ],
    });

    await z.close();
  });

  test('run waiting for complete results throws in custom mutations', async () => {
    const z = zeroForTest({
      schema,
      mutators: {
        issue: {
          create: async (tx: MutatorTx) => {
            await tx.run(tx.query.issue, {type: 'complete'});
          },
        },
      } as const,
    });

    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);

    const result = await z.mutate.issue.create().client;
    assert(result.type === 'error');
    expect(result.error.type).toBe('app');
    assert(result.error.type === 'app');
    expect(result.error.message).toBe(
      'Cannot wait for complete results in custom mutations',
    );

    await z.close();
  });

  test('cannot await the promise directly', async () => {
    const z = zeroForTest({
      schema,
      logLevel: 'warn',
      mutators: {
        issue: {
          create: async (tx: MutatorTx) => {
            await tx.query.issue;
          },
        },
      } as const,
    });

    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);

    expect(z.mutate.issue.create()).toBeTypeOf('object');
    expect(z.mutate.issue.create()).toHaveProperty('client');
    expect(z.mutate.issue.create()).toHaveProperty('server');

    await z.close();
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
            _tx: MutatorTx,
            _args: InsertValue<typeof schema.tables.issue>,
          ) => {},

          close: async (_tx: MutatorTx, _args: object) => {},
        },
      } as const,
    });

    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);

    const create = z.mutate.issue.create({
      id: '1',
      title: 'foo',
      closed: false,
      description: '',
      ownerId: '',
      createdAt: 1743018138477,
    });
    await create.client;

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

    const createServerResult = await create.server;
    expect(createServerResult.type).toBe('success');

    const close = z.mutate.issue.close({});
    await close.client;

    await z.triggerPushResponse({
      mutations: [
        {
          id: {clientID: z.clientID, id: 2},
          result: {
            error: 'app',
            message: 'application error',
            details: {
              code: 'APP_ERROR',
              other: 'some other detail',
            },
          },
        },
      ],
    });

    await z.close();

    const closeServerResult = await close.server;
    assert(closeServerResult.type === 'error');
    expect(closeServerResult.error.type).toBe('app');
    expect(closeServerResult.error.message).toBe('application error');
    assert(closeServerResult.error.type === 'app');
    expect(closeServerResult.error.details).toEqual({
      code: 'APP_ERROR',
      other: 'some other detail',
    });
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
            tx: MutatorTx,
            _args: InsertValue<typeof schema.tables.issue>,
          ) => {
            await tx.query.issue;
          },

          close: async (tx: MutatorTx, _args: object) => {
            await tx.query.issue.limit(1);
          },
        },
      } as const,
    });

    const mockSocket = await z.socket;
    const messages: string[] = [];
    mockSocket.onUpstream(msg => {
      messages.push(msg);
    });

    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);

    const zql = createBuilder(schema);

    const q = asCustomQuery(zql.issue.limit(1), 'a', undefined);
    const view = z.materialize(q);
    const create = z.mutate.issue.create({
      id: '1',
      title: 'foo',
      closed: false,
      description: '',
      ownerId: '',
      createdAt: 1743018138477,
    });
    await create.client;

    view.destroy();

    z.queryDelegate.flushQueryChanges();

    // query is not removed, only put.
    expect(filter(messages)).toMatchInlineSnapshot(`
      [
        "["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"put","hash":"37augjshwgayh","name":"a","args":[],"ttl":300000}]}]",
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

    // confirm the mutation
    await z.triggerPokeStart({
      pokeID: '1',
      baseCookie: null,
      schemaVersions: {minSupportedVersion: 1, maxSupportedVersion: 1},
    });
    await z.triggerPokePart({
      pokeID: '1',
      lastMutationIDChanges: {[z.clientID]: 1},
    });
    await z.triggerPokeEnd({pokeID: '1', cookie: '1'});

    z.queryDelegate.flushQueryChanges();

    // lmid advancement is not in a RAF callback
    // so tick a few times

    // mutation is no longer outstanding, query is removed.
    await vi.waitFor(() => {
      expect(filter(messages)).toEqual([
        `["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"del","hash":"37augjshwgayh"}]}]`,
      ]);
    });

    messages.length = 0;

    // check the error case
    const q2 = asCustomQuery(zql.issue, 'b', undefined);
    const view2 = z.materialize(q2);
    const close = z.mutate.issue.close({});
    await close.client;
    view2.destroy();

    z.queryDelegate.flushQueryChanges();

    expect(filter(messages)).toMatchInlineSnapshot(`
      [
        "["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"put","hash":"1pmg07l6czqjy","name":"b","args":[],"ttl":300000}]}]",
      ]
    `);
    messages.length = 0;

    await z.triggerPushResponse({
      mutations: [
        {
          id: {clientID: z.clientID, id: 2},
          result: {
            error: 'app',
            message: 'womp womp',
            details: {
              issue: 'not found',
            },
          },
        },
      ],
    });

    await z.triggerPokeStart({
      pokeID: '2',
      baseCookie: '1',
      schemaVersions: {minSupportedVersion: 1, maxSupportedVersion: 1},
    });
    await z.triggerPokePart({
      pokeID: '2',
      lastMutationIDChanges: {[z.clientID]: 2},
    });
    await z.triggerPokeEnd({pokeID: '2', cookie: '2'});

    z.queryDelegate.flushQueryChanges();

    const closeServerResult = await close.server;
    assert(closeServerResult.type === 'error');
    expect(closeServerResult.error.type).toBe('app');
    expect(closeServerResult.error.message).toBe('womp womp');
    assert(closeServerResult.error.type === 'app');
    expect(closeServerResult.error.details).toEqual({
      issue: 'not found',
    });

    await vi.waitFor(() => {
      expect(filter(messages)).toEqual([
        `["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"del","hash":"1pmg07l6czqjy"}]}]`,
      ]);
    });

    messages.length = 0;

    await z.close();
  });

  test('after the server promise resolves (via poke), reads from the store return the data from the server', async () => {
    const z = zeroForTest({
      schema,
      mutators: {
        issue: {
          create: async (
            _tx: MutatorTx,
            _args: InsertValue<typeof schema.tables.issue>,
          ) => {},
        },
      } as const,
    });

    const mockSocket = await z.socket;
    const messages: string[] = [];
    mockSocket.onUpstream(msg => {
      messages.push(msg);
    });

    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);

    const create = z.mutate.issue.create({
      id: '1',
      title: 'foo',
      closed: false,
      description: '',
      ownerId: '',
      createdAt: 1743018138477,
    });
    await create.client;

    const zql = createBuilder(schema);

    let foundIssue: Row<typeof schema.tables.issue> | undefined;
    void create.server.then(async () => {
      foundIssue = await z.run(zql.issue.where('id', '1').one());
    });

    // confirm the mutation
    await z.triggerPokeStart({
      pokeID: '1',
      baseCookie: null,
      schemaVersions: {minSupportedVersion: 1, maxSupportedVersion: 1},
    });
    await z.triggerPokePart({
      pokeID: '1',
      lastMutationIDChanges: {[z.clientID]: 1},
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {
            id: '1',
            title: 'server-foo',
            closed: false,
            description: 'server-desc',
            ownerId: '',
            createdAt: 1743018138477,
          },
        },
      ],
      mutationsPatch: [
        {
          op: 'put',
          mutation: {
            id: {clientID: z.clientID, id: 1},
            result: {},
          },
        },
      ],
    });
    await z.triggerPokeEnd({pokeID: '1', cookie: '1'});
    z.queryDelegate.flushQueryChanges();

    await vi.waitFor(() => {
      expect(foundIssue).toEqual({
        id: '1',
        title: 'server-foo',
        closed: false,
        description: 'server-desc',
        ownerId: '',
        createdAt: 1743018138477,
        [refCountSymbol]: 1,
      });
    });

    await z.close();
  });
});

test('crud mutators are not available if `enableLegacyMutators` is set to false', async () => {
  const z = zeroForTest({
    schema: {
      ...schema,
      enableLegacyMutators: false,
    },
  });

  expect('issue' in z.mutate).toBe(false);

  await z.close();
});

test('crud mutators work if `enableLegacyMutators` is set to true (or not set)', async () => {
  const z = zeroForTest({
    schema: {...schema, enableLegacyMutators: true},
  });

  await z.mutate.issue.insert({
    id: '1',
    title: 'foo',
    closed: false,
    description: '',
    ownerId: '',
    createdAt: 1743018138477,
  });

  const zql = createBuilder(schema);

  // read a row
  await expect(z.run(zql.issue.where('id', '1').one())).resolves.toEqual({
    id: '1',
    title: 'foo',
    closed: false,
    description: '',
    ownerId: '',
    createdAt: 1743018138477,
    [refCountSymbol]: 1,
  });

  await z.close();
});

describe('enableLegacyQueries', () => {
  beforeEach(() => {
    vi.stubGlobal('WebSocket', MockSocket as unknown as typeof WebSocket);

    return () => {
      vi.unstubAllGlobals();
    };
  });

  test('unnamed queries do not get registered with the query manager if `enableLegacyQueries` is set to false', async () => {
    const zql = createBuilder(schema);
    const z = zeroForTest({
      schema: {
        ...schema,
        enableLegacyQueries: false,
      },
    });

    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);

    const mockSocket = await z.socket;

    await z.run(zql.issue.where('id', '1').one());
    z.queryDelegate.flushQueryChanges();

    // No changeDesiredQueries message should be sent
    const changeDesiredQueriesMessages = mockSocket.jsonMessages.filter(
      m => Array.isArray(m) && m[0] === 'changeDesiredQueries',
    );
    expect(changeDesiredQueriesMessages).toHaveLength(0);
    await z.close();
  });

  test('unnamed queries do get registered with the query manager if `enableLegacyQueries` is set to true', async () => {
    const zql = createBuilder(schema);
    const z = zeroForTest({
      schema: {...schema, enableLegacyQueries: true},
    });

    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);

    const mockSocket = await z.socket;

    await z.run(zql.issue.where('id', '1').one());
    z.queryDelegate.flushQueryChanges();

    // A changeDesiredQueries message should be sent
    const changeDesiredQueriesMessages = mockSocket.jsonMessages.filter(
      m => Array.isArray(m) && m[0] === 'changeDesiredQueries',
    );
    expect(changeDesiredQueriesMessages.length).toBeGreaterThan(0);
    await z.close();
  });
});
