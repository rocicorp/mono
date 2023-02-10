import {expect} from '@esm-bundle/chai';
import type {PullResponse} from './puller';
import type {Poke} from './replicache';
import {
  addData,
  initReplicacheTesting,
  makePullResponse,
  replicacheForTesting,
} from './test-util';
import type {WriteTransaction} from './transactions';

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
        await tx.put(`/todo/${args.id}`, args);
      },
    },
  });
  const clientID = await rep.clientID;

  const {setTodo} = rep.mutate;

  const id = 1;
  const key = `/todo/${id}`;
  const text = 'yo';

  await setTodo({id, text});
  expect(await rep.query(tx => tx.has(key))).true;

  // cookie *does* apply
  const poke: Poke = {
    baseCookie: null,
    pullResponse: makePullResponse(
      clientID,
      1,
      [{op: 'del', key}],
      'c1',
    ) as PullResponse,
  };

  await rep.poke(poke);
  expect(await rep.query(tx => tx.has(key))).false;

  // cookie does not apply
  await setTodo({id, text});
  let error = null;
  try {
    const poke: Poke = {
      baseCookie: null,
      pullResponse: makePullResponse(
        clientID,
        1,
        [{op: 'del', key}],
        'c1',
      ) as PullResponse,
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
      pullResponse: makePullResponse(
        clientID,
        0,
        [{op: 'del', key}],
        'c2',
      ) as PullResponse,
    };
    await rep.poke(poke);
  } catch (e: unknown) {
    error = String(e);
  }
  expect(error).contains(
    'Received lastMutationID 0 is < than last snapshot lastMutationID 1; ignoring client view',
  );
});

test('overlapped pokes not supported', async () => {
  const rep = await replicacheForTesting('multiple-pokes', {
    mutators: {
      addData,
    },
  });

  const clientID = await rep.clientID;
  const poke: Poke = {
    baseCookie: null,
    pullResponse: makePullResponse(
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
    ) as PullResponse,
  };

  const p1 = rep.poke(poke);

  const poke2: Poke = {
    baseCookie: 'c2',
    pullResponse: makePullResponse(
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
    ) as PullResponse,
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
