import {expect} from '@esm-bundle/chai';
import {
  addData,
  initReplicacheTesting,
  replicacheForTesting,
} from './test-util.js';
import type {WriteTransaction} from './transactions.js';

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

  const {setTodo} = rep.mutate;

  const id = 1;
  const key = `/todo/${id}`;
  const text = 'yo';

  await setTodo({id, text});
  expect(await rep.query(tx => tx.has(key))).true;

  // cookie *does* apply
  await rep.poke({
    baseCookie: null,
    pullResponse: {
      cookie: 'c1',
      lastMutationID: 1,
      patch: [{op: 'del', key}],
    },
  });
  expect(await rep.query(tx => tx.has(key))).false;

  // cookie does not apply
  await setTodo({id, text});
  let error = null;
  try {
    await rep.poke({
      baseCookie: null,
      pullResponse: {
        cookie: 'c1',
        lastMutationID: 1,
        patch: [{op: 'del', key}],
      },
    });
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
    await rep.poke({
      baseCookie: 'c1',
      pullResponse: {
        cookie: 'c2',
        lastMutationID: 0,
        patch: [{op: 'del', key}],
      },
    });
  } catch (e: unknown) {
    error = String(e);
  }
  expect(error).contains(
    'Received lastMutationID 0 is < than last snapshot lastMutationID 1; ignoring client view',
  );
});

test('multiple pokes', async () => {
  const rep = await replicacheForTesting('multiple-pokes', {
    mutators: {
      addData,
    },
  });

  const p1 = rep.poke({
    baseCookie: null,
    pullResponse: {
      lastMutationID: 1,
      patch: [
        {
          op: 'put',
          key: 'a',
          value: 1,
        },
      ],
    },
  });

  const p2 = rep.persist();

  const p3 = rep.poke({
    baseCookie: null,
    pullResponse: {
      lastMutationID: 2,
      patch: [
        {
          op: 'put',
          key: 'a',
          value: 2,
        },
      ],
    },
  });

  await p1;
  await p2;
  await p3;

  expect(await rep.query(tx => tx.get('a'))).equal(2);
});
