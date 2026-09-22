import {expect, test, vi} from 'vitest';
import {promiseVoid} from '../../shared/src/resolved-promises.ts';
import type {VersionNotSupportedResponse} from './error-responses.ts';
import type {
  EphemeralID,
  ZeroOption,
  ZeroTxData,
} from './replicache-options.ts';
import {
  addData,
  disableAllBackgroundProcesses,
  initReplicacheTesting,
  makePullResponseV1,
  replicacheForTesting,
} from './test-util.ts';
import {
  type WriteTransaction,
  type WriteTransactionImpl,
  zeroData,
} from './transactions.ts';
import type {Poke, UpdateNeededReason} from './types.ts';

initReplicacheTesting();

test('poke', async () => {
  // TODO(MP) test:
  // - when we queue a poke and it matches, we update the snapshot
  // - rebase still works
  // - when the cookie doesn't match, it doesn't apply, but later when the cookie matches it does
  // - per-client timing
  const rep = await replicacheForTesting('poke', {
    auth: '1',
    mutators: {
      setTodo: async <A extends {id: number}>(
        tx: WriteTransaction,
        args: A,
      ) => {
        await tx.set(`/todo/${args.id}`, args);
      },
    },
  });
  const {clientID} = rep;

  const {setTodo} = rep.mutate;

  const id = 1;
  const key = `/todo/${id}`;
  const text = 'yo';

  await setTodo({id, text});
  expect(await rep.query(tx => tx.has(key))).toBe(true);

  // cookie *does* apply
  const poke: Poke = {
    baseCookie: null,
    pullResponse: makePullResponseV1(clientID, 1, [{op: 'del', key}], 'c1'),
  };

  await rep.poke(poke);
  expect(await rep.query(tx => tx.has(key))).toBe(false);

  // cookie does not apply
  await setTodo({id, text});
  let error = null;
  try {
    const poke: Poke = {
      baseCookie: null,
      pullResponse: makePullResponseV1(clientID, 1, [{op: 'del', key}], 'c1'),
    };
    await rep.poke(poke);
  } catch (e) {
    error = String(e);
  }
  expect(error).contains('unexpected base cookie for poke');
  expect(await rep.query(tx => tx.has(key))).toBe(true);

  // cookie applies, but lmid goes backward - should be an error.
  await setTodo({id, text});
  error = null;
  try {
    // blech could not figure out how to use chai-as-promised.
    const poke: Poke = {
      baseCookie: 'c1',
      pullResponse: makePullResponseV1(clientID, 0, [{op: 'del', key}], 'c2'),
    };
    await rep.poke(poke);
  } catch (e: unknown) {
    error = String(e);
  }
  expect(error).matches(
    /Received ([0-9a-v]* )?lastMutationID 0 is < than last snapshot ([0-9a-v]* )?lastMutationID 1; ignoring client view/,
  );
});

test('overlapped pokes not supported', async () => {
  const rep = await replicacheForTesting(
    'multiple-pokes',
    {
      mutators: {
        addData,
      },
    },
    {
      ...disableAllBackgroundProcesses,
      enablePullAndPushInOpen: false,
    },
  );

  const {clientID} = rep;
  const poke: Poke = {
    baseCookie: null,
    pullResponse: makePullResponseV1(
      clientID,
      1,
      [
        {
          op: 'put',
          key: 'a',
          value: 1,
        },
      ],
      'c2',
    ),
  };

  const p1 = rep.poke(poke);

  const poke2: Poke = {
    baseCookie: 'c2',
    pullResponse: makePullResponseV1(
      clientID,
      2,
      [
        {
          op: 'put',
          key: 'a',
          value: 2,
        },
      ],
      'c3',
    ),
  };

  const p2 = rep.poke(poke2);

  await p1;

  let error = null;
  try {
    await p2;
  } catch (e) {
    error = String(e);
  }
  expect(error).contains('unexpected base cookie for poke');

  expect(await rep.query(tx => tx.get('a'))).toBe(1);
});

test('Client group unknown on server', async () => {
  const onClientStateNotFound = vi.fn();
  const rep = await replicacheForTesting('client-group-unknown', {
    onClientStateNotFound,
  });

  expect(rep.isClientGroupDisabled).toBe(false);

  const poke: Poke = {
    baseCookie: 123,
    pullResponse: {
      error: 'ClientStateNotFound',
    },
  };
  let err;
  try {
    await rep.poke(poke);
  } catch (e) {
    err = e;
  }

  expect(err).toBeUndefined();
  expect(onClientStateNotFound).toHaveBeenCalledOnce();
  expect(rep.isClientGroupDisabled).toBe(true);
});

test('Version not supported on server', async () => {
  const t = async (
    response: VersionNotSupportedResponse,
    reason: UpdateNeededReason,
  ) => {
    const rep = await replicacheForTesting(
      'version-not-supported-poke',
      undefined,
      disableAllBackgroundProcesses,
    );

    const onUpdateNeededStub = (rep.onUpdateNeeded = vi.fn());

    const poke: Poke = {
      baseCookie: 123,
      pullResponse: response,
    };

    await rep.poke(poke);

    expect(onUpdateNeededStub).toHaveBeenCalledOnce();
    expect(onUpdateNeededStub.mock.calls[0]).toEqual([reason]);
  };

  await t({error: 'VersionNotSupported'}, {type: 'VersionNotSupported'});
  await t(
    {error: 'VersionNotSupported', versionType: 'pull'},
    {type: 'VersionNotSupported', versionType: 'pull'},
  );
  await t(
    {error: 'VersionNotSupported', versionType: 'schema'},
    {type: 'VersionNotSupported', versionType: 'schema'},
  );
});

// The replay loop in replicache-impl hands each mutation the zero tx data the
// previous one returned. That handoff is all this layer does with it;
// `rebaseMutation`'s own behavior is covered in db/rebase.test.ts.
test('each replayed mutation is handed what the previous one returned', async () => {
  const makeTxData = (rows: ReadonlySet<string>): ZeroTxData => ({
    ivmSources: new Set(rows),
    token: undefined,
    context: undefined,
    fork(): ZeroTxData {
      return makeTxData(this.ivmSources as Set<string>);
    },
  });

  const zero = {
    auth: '',
    init: () => promiseVoid,
    getTxData: () => Promise.resolve(makeTxData(new Set())),
    advance: () => undefined,
    trackMutation: () => ({
      ephemeralID: 0 as EphemeralID,
      serverPromise: Promise.resolve(),
    }),
    mutationIDAssigned: () => undefined,
    rejectMutation: () => undefined,
  } as unknown as ZeroOption;

  const rowsOf = (tx: WriteTransaction) =>
    (tx as WriteTransactionImpl)[zeroData]?.ivmSources as Set<string>;
  const seen: string[][] = [];

  const rep = await replicacheForTesting(
    'poke-zero-tx-data-handoff',
    {
      mutators: {
        first: async (tx: WriteTransaction) => {
          if (tx.reason === 'rebase') {
            seen.push([...rowsOf(tx)]);
          }
          rowsOf(tx)?.add('first');
          await tx.set('/first', true);
        },
        second: async (tx: WriteTransaction) => {
          if (tx.reason === 'rebase') {
            seen.push([...rowsOf(tx)]);
          }
          rowsOf(tx)?.add('second');
          await tx.set('/second', true);
        },
      },
      ...disableAllBackgroundProcesses,
    },
    {zero},
  );
  const {clientID} = rep;

  await rep.mutate.first();
  await rep.mutate.second();

  // Acks neither mutation, so both are replayed.
  await rep.poke({
    baseCookie: null,
    pullResponse: makePullResponseV1(
      clientID,
      0,
      [{op: 'put', key: '/server', value: true}],
      'c1',
    ),
  } as Poke);

  // The second replayed mutation sees what the first wrote. Without the
  // handoff both entries are empty.
  expect(seen).toEqual([[], ['first']]);
});
