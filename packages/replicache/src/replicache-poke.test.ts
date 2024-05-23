import {expect} from 'chai';
import * as sinon from 'sinon';
import type {VersionNotSupportedResponse} from './error-responses.js';
import {
  addData,
  disableAllBackgroundProcesses,
  initReplicacheTesting,
  makePullResponseV1,
  replicacheForTesting,
} from './test-util.js';
import type {WriteTransaction} from './transactions.js';
import type {Poke, UpdateNeededReason} from './types.js';

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
  expect(await rep.query(tx => tx.has(key))).true;

  // cookie *does* apply
  const poke: Poke = {
    baseCookie: null,
    pullResponse: makePullResponseV1(clientID, 1, [{op: 'del', key}], 'c1'),
  };

  await rep.poke(poke);
  expect(await rep.query(tx => tx.has(key))).false;

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
  expect(await rep.query(tx => tx.has(key))).true;

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
    /Received ([0-9a-f-]* )?lastMutationID 0 is < than last snapshot ([0-9a-f-]* )?lastMutationID 1; ignoring client view/,
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

  expect(await rep.query(tx => tx.get('a'))).equal(1);
});

test('Client group unknown on server', async () => {
  const onClientStateNotFound = sinon.stub();
  const rep = await replicacheForTesting('client-group-unknown', {
    onClientStateNotFound,
  });

  expect(rep.isClientGroupDisabled).false;

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

  expect(err).undefined;
  expect(onClientStateNotFound.callCount).equal(1);
  expect(rep.isClientGroupDisabled).true;
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

    const onUpdateNeededStub = (rep.onUpdateNeeded = sinon.stub());

    const poke: Poke = {
      baseCookie: 123,
      pullResponse: response,
    };

    await rep.poke(poke);

    expect(onUpdateNeededStub.callCount).to.equal(1);
    expect(onUpdateNeededStub.lastCall.args).deep.equal([reason]);
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
