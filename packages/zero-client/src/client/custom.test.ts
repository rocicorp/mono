import {
  afterEach,
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
import {refCountSymbol} from '../../../zql/src/ivm/view-apply-change.ts';
import type {InsertValue, Transaction} from '../../../zql/src/mutate/custom.ts';
import type {Row} from '../../../zql/src/query/query.ts';
import {schema} from '../../../zql/src/query/test/test-schemas.ts';
import * as ConnectionState from './connection-state-enum.ts';
import {
  TransactionImpl,
  type MakeCustomMutatorInterfaces,
  type MutatorResult,
} from './custom.ts';
import {IVMSourceBranch} from './ivm-branch.ts';
import type {WriteTransaction} from './replicache-types.ts';
import {MockSocket, zeroForTest} from './test-utils.ts';
import {createDb} from './test/create-db.ts';
import {getInternalReplicacheImplForTesting} from './zero.ts';
import {QueryManager} from './query-manager.ts';

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

  type MutatorsInterface = MakeCustomMutatorInterfaces<Schema, typeof mutators>;

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

  const issues = await z.query.issue;
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

  await z.markQueryAsGot(z.query.issue);
  let issues = await z.query.issue;
  expect(issues[0].title).toEqual('foo');

  await z.mutate.issue.setTitle({id: '1', title: 'bar'}).client;
  issues = await z.query.issue;
  expect(issues[0].title).toEqual('bar');

  await z.mutate.customNamespace.clown('1').client;
  issues = await z.query.issue;
  expect(issues[0].title).toEqual('🤡');

  await z.mutate.issue.create({
    id: '2',
    title: 'foo',
    closed: false,
    ownerId: '',
    description: '',
    createdAt: 1743018138477,
  }).client;
  issues = await z.query.issue;
  expect(issues.length).toEqual(2);

  await z.mutate.issue.deleteTwoIssues({id1: issues[0].id, id2: issues[1].id})
    .client;
  issues = await z.query.issue;
  expect(issues.length).toEqual(0);
});

test('custom mutators can query the local store during an optimistic mutation', async () => {
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
          const issues = await tx.query.issue;
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

  const q = z.query.issue.where('closed', false);
  await z.markQueryAsGot(q);
  let issues = await q;
  expect(issues.length).toEqual(10);

  await z.mutate.issue.closeAll().client;

  issues = await q;
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
              await tx.query.issue.where('id', args.id).one(),
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

    const q = z.query.issue.where('id', '1').one();
    const issue = await q.run({type: 'unknown'});
    expect(issue?.title).toEqual('foo updated');
    expect(issue?.description).toEqual('updated');
    const p = q.run({type: 'complete'});
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
      expect(await rep.query(tx => tx.has(`g/${q.hash()}`))).toEqual(true);
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

      const result = await z.query.issue.where('id', String(i)).one();
      expect(result?.title).toEqual('foo ' + i);
      expect(result?.id).toEqual(String(i));
    }
  });

  test('mutations on main do not change main until they are committed', async () => {
    let mutationRun = false;
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
            expect(await z.query.issue).length(0);
            // but it is in this tx
            expect(await tx.query.issue).length(1);

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
    await z.waitForConnectionState(ConnectionState.Connected);

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

    expect(await create.server).toEqual({data: {shortID: '1'}});

    const close = z.mutate.issue.close({});
    await close.client;

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
    await z.waitForConnectionState(ConnectionState.Connected);

    const q = z.query.issue.limit(1).materialize();
    const create = z.mutate.issue.create({
      id: '1',
      title: 'foo',
      closed: false,
      description: '',
      ownerId: '',
      createdAt: 1743018138477,
    });
    await create.client;

    q.destroy();

    z.queryDelegate.flushQueryChanges();

    // query is not removed, only put.
    expect(filter(messages)).toMatchInlineSnapshot(`
      [
        "["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"put","hash":"1vsd9vcx6ynd4","ast":{"table":"issues","limit":1,"orderBy":[["id","asc"]]},"ttl":300000}]}]",
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
        `["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"del","hash":"1vsd9vcx6ynd4"}]}]`,
      ]);
    });

    messages.length = 0;

    // check the error case
    const q2 = z.query.issue.materialize();
    const close = z.mutate.issue.close({});
    await close;
    q2.destroy();

    z.queryDelegate.flushQueryChanges();

    expect(filter(messages)).toMatchInlineSnapshot(`
      [
        "["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"put","hash":"12hwg3ihkijhm","ast":{"table":"issues","orderBy":[["id","asc"]]},"ttl":300000}]}]",
      ]
    `);
    messages.length = 0;

    await z.triggerPushResponse({
      mutations: [
        {
          id: {clientID: z.clientID, id: 2},
          result: {
            error: 'app',
            details: 'womp womp',
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

    await expect(close.server).rejects.toEqual({
      error: 'app',
      details: 'womp womp',
    });

    await vi.waitFor(() => {
      expect(filter(messages)).toEqual([
        `["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"del","hash":"12hwg3ihkijhm"}]}]`,
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
    await z.waitForConnectionState(ConnectionState.Connected);

    const create = z.mutate.issue.create({
      id: '1',
      title: 'foo',
      closed: false,
      description: '',
      ownerId: '',
      createdAt: 1743018138477,
    });
    await create.client;

    let foundIssue: Row<typeof schema.tables.issue> | undefined;
    void create.server.then(async () => {
      foundIssue = await z.query.issue.where('id', '1').one();
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

test('run waiting for complete results throws in custom mutations', async () => {
  let err;
  const z = zeroForTest({
    schema,
    mutators: {
      issue: {
        create: async (tx: MutatorTx) => {
          try {
            await tx.query.issue.run({type: 'complete'});
          } catch (e) {
            err = e;
          }
        },
      },
    } as const,
  });

  await z.triggerConnected();
  await z.waitForConnectionState(ConnectionState.Connected);

  await z.mutate.issue.create().client;

  expect(err).toMatchInlineSnapshot(
    `[Error: Cannot wait for complete results in custom mutations]`,
  );

  await z.close();
});

test('warns when awaiting the promise directly', async () => {
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
  await z.waitForConnectionState(ConnectionState.Connected);

  await z.mutate.issue.create();

  expect(z.testLogSink.messages[0][2]).toEqual([
    'Awaiting the mutator result directly is being deprecated. Please use `await z.mutate[mutatorName].client` or `await result.mutate[mutatorName].server`',
  ]);

  await z.close();
});

test('trying to use crud mutators throws if `enableLegacyMutators` is set to false', async () => {
  const z = zeroForTest({
    schema,
    enableLegacyMutators: false,
  });

  await expect(
    z.mutate.issue.insert({
      id: '1',
      title: 'foo',
      closed: false,
      description: '',
      ownerId: '',
      createdAt: 1743018138477,
    }),
  ).rejects.toThrowErrorMatchingInlineSnapshot(
    `[Error: Zero CRUD mutators are not enabled.]`,
  );

  await z.close();
});

test('crud mutators work if `enableLegacyMutators` is set to true (or not set)', async () => {
  const z = zeroForTest({
    schema,
  });

  await z.mutate.issue.insert({
    id: '1',
    title: 'foo',
    closed: false,
    description: '',
    ownerId: '',
    createdAt: 1743018138477,
  });
  // read a row
  await expect(z.query.issue.where('id', '1').one()).resolves.toEqual({
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

test('unnamed queries do not get registered with the query manager if `enableLegacyQueries` is set to false', async () => {
  const addLegacySpy = vi.spyOn(QueryManager.prototype, 'addLegacy');
  const z = zeroForTest({
    schema,
    enableLegacyQueries: false,
  });

  // mock the QueryManager class
  // and spy on addLegacy

  await z.triggerConnected();
  await z.waitForConnectionState(ConnectionState.Connected);

  await z.query.issue.where('id', '1').one();

  expect(addLegacySpy).not.toHaveBeenCalled();
  await z.close();
});

test('unnamed queries do get registered with the query manager if `enableLegacyQueries` is set to true', async () => {
  const addLegacySpy = vi.spyOn(QueryManager.prototype, 'addLegacy');

  const z = zeroForTest({
    schema,
  });

  await z.triggerConnected();
  await z.waitForConnectionState(ConnectionState.Connected);

  await z.query.issue.where('id', '1').one();

  expect(addLegacySpy).toHaveBeenCalled();
  await z.close();
});
