import {expect, test, vi} from 'vitest';
import type {VersionNotSupportedResponse} from './error-responses.ts';
import {getDefaultPusher} from './get-default-pusher.ts';
import type {Pusher} from './pusher.ts';
import {
  disableAllBackgroundProcesses,
  fetchMocker,
  initReplicacheTesting,
  replicacheForTesting,
  tickAFewTimes,
} from './test-util.ts';
import type {WriteTransaction} from './transactions.ts';
import type {UpdateNeededReason} from './types.ts';

initReplicacheTesting();

test('push', async () => {
  const pushURL = 'https://push.com';

  const rep = await replicacheForTesting('push', {
    auth: '1',
    pushURL,
    pushDelay: 10,
    mutators: {
      createTodo: async <A extends {id: number}>(
        tx: WriteTransaction,
        args: A,
      ) => {
        await tx.set(`/todo/${args.id}`, args);
      },
      deleteTodo: async <A extends {id: number}>(
        tx: WriteTransaction,
        args: A,
      ) => {
        await tx.del(`/todo/${args.id}`);
      },
    },
  });

  const {createTodo, deleteTodo} = rep.mutate;

  const id1 = 14323534;
  const id2 = 22354345;

  await deleteTodo({id: id1});
  await deleteTodo({id: id2});

  fetchMocker.postOnce(pushURL, {
    mutationInfos: [
      {id: 1, error: 'deleteTodo: todo not found'},
      {id: 2, error: 'deleteTodo: todo not found'},
    ],
  });
  await tickAFewTimes(vi);
  const {mutations} = fetchMocker.lastBody() as {
    mutations: unknown[];
  };
  const {clientID} = rep;
  expect(mutations).toEqual([
    {
      clientID,
      id: 1,
      name: 'deleteTodo',
      args: {id: id1},
      timestamp: 100,
    },
    {
      clientID,
      id: 2,
      name: 'deleteTodo',
      args: {id: id2},
      timestamp: 100,
    },
  ]);

  await createTodo({
    id: id1,
    text: 'Test',
  });
  expect(
    (
      (await rep.query(tx => tx.get(`/todo/${id1}`))) as {
        text: string;
      }
    ).text,
  ).toBe('Test');

  fetchMocker.postOnce(pushURL, {
    mutationInfos: [{id: 3, error: 'mutation has already been processed'}],
  });
  await tickAFewTimes(vi);
  {
    const {mutations} = fetchMocker.lastBody() as {
      mutations: unknown[];
    };
    expect(mutations).toEqual([
      {
        clientID,
        id: 1,
        name: 'deleteTodo',
        args: {id: id1},
        timestamp: 100,
      },
      {
        clientID,
        id: 2,
        name: 'deleteTodo',
        args: {id: id2},
        timestamp: 100,
      },
      {
        clientID,
        id: 3,
        name: 'createTodo',
        args: {id: id1, text: 'Test'},
        timestamp: 200,
      },
    ]);
  }

  await createTodo({
    id: id2,
    text: 'Test 2',
  });
  expect(
    ((await rep.query(tx => tx.get(`/todo/${id2}`))) as {text: string}).text,
  ).toBe('Test 2');

  // Clean up
  await deleteTodo({id: id1});
  await deleteTodo({id: id2});

  fetchMocker.postOnce(pushURL, {
    mutationInfos: [],
  });
  await tickAFewTimes(vi);
  {
    const {mutations} = fetchMocker.lastBody() as {
      mutations: unknown[];
    };
    expect(mutations).toEqual([
      {
        clientID,
        id: 1,
        name: 'deleteTodo',
        args: {id: id1},
        timestamp: 100,
      },
      {
        clientID,
        id: 2,
        name: 'deleteTodo',
        args: {id: id2},
        timestamp: 100,
      },
      {
        clientID,
        id: 3,
        name: 'createTodo',
        args: {id: id1, text: 'Test'},
        timestamp: 200,
      },
      {
        clientID,
        id: 4,
        name: 'createTodo',
        args: {id: id2, text: 'Test 2'},
        timestamp: 300,
      },
      {
        clientID,
        id: 5,
        name: 'deleteTodo',
        args: {id: id1},
        timestamp: 300,
      },
      {
        clientID,
        id: 6,
        name: 'deleteTodo',
        args: {id: id2},
        timestamp: 300,
      },
    ]);
  }
});

test('push request is only sent when pushURL or non-default pusher are set', async () => {
  const rep = await replicacheForTesting(
    'no push requests',
    {
      auth: '1',
      pullURL: 'https://diff.com/pull',
      pushDelay: 1,
      mutators: {
        createTodo: async <A extends {id: number}>(
          tx: WriteTransaction,
          args: A,
        ) => {
          await tx.set(`/todo/${args.id}`, args);
        },
      },
    },
    undefined,
    {useDefaultURLs: false},
  );

  const {createTodo} = rep.mutate;

  await tickAFewTimes(vi);
  fetchMocker.reset();
  fetchMocker.post(undefined, {});

  await createTodo({id: 'id1'});
  await tickAFewTimes(vi);

  expect(fetchMocker.spy).not.toHaveBeenCalled();

  await tickAFewTimes(vi);
  fetchMocker.reset();
  fetchMocker.post(undefined, {});

  rep.pushURL = 'https://diff.com/push';

  await createTodo({id: 'id2'});
  await tickAFewTimes(vi);
  expect(fetchMocker.spy).toHaveBeenCalledOnce();

  await tickAFewTimes(vi);
  fetchMocker.reset();
  fetchMocker.post(undefined, {});

  rep.pushURL = '';

  await createTodo({id: 'id3'});
  await tickAFewTimes(vi);
  expect(fetchMocker.spy).not.toHaveBeenCalled();

  await tickAFewTimes(vi);
  fetchMocker.reset();
  fetchMocker.post(undefined, {});
  let pusherCallCount = 0;

  // oxlint-disable-next-line require-await
  rep.pusher = async () => {
    pusherCallCount++;
    return {
      httpRequestInfo: {
        httpStatusCode: 200,
        errorMessage: '',
      },
    };
  };

  await createTodo({id: 'id4'});
  await tickAFewTimes(vi);

  expect(fetchMocker.spy).not.toHaveBeenCalled();
  expect(pusherCallCount).toBe(1);

  await tickAFewTimes(vi);
  fetchMocker.reset();
  fetchMocker.post(undefined, {});
  pusherCallCount = 0;

  rep.pusher = getDefaultPusher(rep);

  await createTodo({id: 'id5'});
  await tickAFewTimes(vi);

  expect(fetchMocker.spy).not.toHaveBeenCalled();
  expect(pusherCallCount).toBe(0);
});

test('Version not supported on server', async () => {
  const t = async (
    response: VersionNotSupportedResponse,
    reason: UpdateNeededReason,
  ) => {
    const rep = await replicacheForTesting(
      'version-not-supported-push',
      {
        mutators: {
          noop: () => undefined,
        },
      },
      disableAllBackgroundProcesses,
    );

    const onUpdateNeededStub = (rep.onUpdateNeeded = vi.fn());

    // oxlint-disable-next-line require-await
    const pusher: Pusher = async () => ({
      response,
      httpRequestInfo: {
        httpStatusCode: 200,
        errorMessage: '',
      },
    });

    rep.pusher = pusher as Pusher;

    await rep.mutate.noop();
    await rep.push({now: true});

    expect(onUpdateNeededStub).toHaveBeenCalledTimes(1);
    expect(onUpdateNeededStub.mock.lastCall).toEqual([reason]);
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

test('ClientStateNotFound on server', async () => {
  const onClientStateNotFound = vi.fn();
  const rep = await replicacheForTesting(
    'client-state-not-found-push',
    {
      mutators: {
        noop: () => undefined,
      },
      onClientStateNotFound,
    },
    disableAllBackgroundProcesses,
  );

  const onUpdateNeededStub = (rep.onUpdateNeeded = vi.fn());

  // oxlint-disable-next-line require-await
  const pusher: Pusher = async () => ({
    response: {error: 'ClientStateNotFound'},
    httpRequestInfo: {
      httpStatusCode: 200,
      errorMessage: '',
    },
  });

  rep.pusher = pusher as Pusher;

  await rep.mutate.noop();
  await rep.push({now: true});

  expect(onUpdateNeededStub).toHaveBeenCalledTimes(0);
  expect(onClientStateNotFound).toHaveBeenCalledTimes(1);
  expect(rep.isClientGroupDisabled).toBe(true);
});
