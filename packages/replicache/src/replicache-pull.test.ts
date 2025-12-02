import {resolver} from '@rocicorp/resolver';
import {expect, test, vi} from 'vitest';
import type {VersionNotSupportedResponse} from './error-responses.ts';
import {getDefaultPuller} from './get-default-puller.ts';
import {type Hash, emptyHash} from './hash.ts';
import {httpStatusUnauthorized} from './http-status-unauthorized.ts';
import type {Puller} from './puller.ts';
import {
  disableAllBackgroundProcesses,
  expectConsoleLogContextStub,
  fetchMocker,
  initReplicacheTesting,
  makePullResponseV1,
  replicacheForTesting,
  requestIDLogContextRegex,
  tickAFewTimes,
  waitForSync,
} from './test-util.ts';
import type {WriteTransaction} from './transactions.ts';
import type {UpdateNeededReason} from './types.ts';

initReplicacheTesting();

test('pull', async () => {
  const pullURL = 'https://diff.com/pull';

  const rep = await replicacheForTesting(
    'pull',
    {
      auth: '1',
      pullURL,
      mutators: {
        createTodo: async <A extends {id: number}>(
          tx: WriteTransaction,
          args: A,
        ) => {
          createCount++;
          await tx.set(`/todo/${args.id}`, args);
        },
        deleteTodo: async <A extends {id: number}>(
          tx: WriteTransaction,
          args: A,
        ) => {
          deleteCount++;
          await tx.del(`/todo/${args.id}`);
        },
      },
    },
    {
      ...disableAllBackgroundProcesses,
      enablePullAndPushInOpen: false,
    },
  );

  let createCount = 0;
  let deleteCount = 0;
  let syncHead: Hash;
  let beginPullResult: {
    requestID: string;
    syncHead: Hash;
    ok: boolean;
  };

  const {createTodo, deleteTodo} = rep.mutate;

  const id1 = 14323534;
  const id2 = 22354345;

  await deleteTodo({id: id1});
  await deleteTodo({id: id2});

  let cookie = 1;
  expect(deleteCount).toBe(2);
  const {clientID} = rep;
  fetchMocker.postOnce(
    pullURL,
    makePullResponseV1(
      clientID,
      2,
      [
        {op: 'del', key: ''},
        {
          op: 'put',
          key: '/list/1',
          value: {id: 1, ownerUserID: 1},
        },
      ],
      cookie,
    ),
  );
  rep.pullIgnorePromise();
  await tickAFewTimes(vi);
  expect(deleteCount).toBe(2);

  fetchMocker.postOnce(
    pullURL,
    makePullResponseV1(clientID, 2, undefined, cookie),
  );
  beginPullResult = await rep.beginPull();
  ({syncHead} = beginPullResult);
  expect(syncHead).toBe(emptyHash);
  expect(deleteCount).toBe(2);

  await createTodo({
    id: id1,
    text: 'Test',
  });
  expect(createCount).toBe(1);
  expect(
    ((await rep.query(tx => tx.get(`/todo/${id1}`))) as {text: string}).text,
  ).toBe('Test');

  fetchMocker.postOnce(
    pullURL,
    makePullResponseV1(
      clientID,
      3,
      [
        {
          op: 'put',
          key: '/todo/14323534',
          value: {id: 14323534, text: 'Test'},
        },
      ],
      ++cookie,
    ),
  );
  beginPullResult = await rep.beginPull();
  ({syncHead} = beginPullResult);
  expect(syncHead).not.toBeUndefined();
  expect(syncHead).not.toBe(emptyHash);

  expect(rep.lastMutationID).toBe(3);

  await createTodo({
    id: id2,
    text: 'Test 2',
  });

  expect(rep.lastMutationID).toBe(4);

  expect(createCount).toBe(2);
  expect(
    ((await rep.query(tx => tx.get(`/todo/${id2}`))) as {text: string}).text,
  ).toBe('Test 2');

  fetchMocker.postOnce(
    pullURL,
    makePullResponseV1(clientID, 3, undefined, ++cookie),
  );
  await rep.maybeEndPull(syncHead, beginPullResult.requestID);

  expect(createCount).toBe(3);

  expect(rep.lastMutationID).toBe(4);

  // Clean up
  await deleteTodo({id: id1});
  await deleteTodo({id: id2});

  expect(deleteCount).toBe(4);
  expect(createCount).toBe(3);

  fetchMocker.postOnce(
    pullURL,
    makePullResponseV1(
      clientID,
      6,
      [{op: 'del', key: '/todo/14323534'}],
      ++cookie,
    ),
  );
  rep.pullIgnorePromise();
  await tickAFewTimes(vi);

  expect(rep.lastMutationID).toBe(6);

  expect(deleteCount).toBe(4);
  expect(createCount).toBe(3);
});

test('reauth pull', async () => {
  const pullURL = 'https://diff.com/pull';

  const rep = await replicacheForTesting(
    'reauth',
    {
      pullURL,
      auth: 'wrong',
    },
    {
      ...disableAllBackgroundProcesses,
      enablePullAndPushInOpen: false,
    },
  );

  fetchMocker.post(pullURL, {body: 'xxx', status: httpStatusUnauthorized});

  const consoleErrorStub = vi.spyOn(console, 'error');

  const {promise, resolve} = resolver();
  const getAuthFake = vi.fn().mockReturnValue(null);
  rep.getAuth = () => {
    resolve();
    return getAuthFake();
  };

  await rep.beginPull();

  await promise;

  expect(getAuthFake).toHaveBeenCalledTimes(1);
  expect(consoleErrorStub).toHaveBeenCalledTimes(1);
  expectConsoleLogContextStub(
    rep.name,
    consoleErrorStub.mock.calls[0],
    `Got a non 200 response doing pull: 401: xxx`,
    ['pull', requestIDLogContextRegex],
  );
  {
    const consoleInfoStub = vi.spyOn(console, 'info');
    const getAuthFake = vi.fn(() => 'boo');
    rep.getAuth = getAuthFake;

    expect((await rep.beginPull()).syncHead).toBe(emptyHash);

    expect(getAuthFake).toHaveBeenCalledTimes(8);
    expect(consoleErrorStub).toHaveBeenCalledTimes(9);
    expectConsoleLogContextStub(
      rep.name,
      consoleInfoStub.mock.calls[0],
      'Tried to reauthenticate too many times',
      ['pull'],
    );
  }
});

test('pull request is only sent when pullURL or non-default puller are set', async () => {
  const rep = await replicacheForTesting(
    'no push requests',
    {
      auth: '1',
      pushURL: 'https://diff.com/push',
    },
    undefined,
    {useDefaultURLs: false},
  );

  await tickAFewTimes(vi);
  fetchMocker.reset();
  fetchMocker.post(undefined, {});

  rep.pullIgnorePromise();
  await tickAFewTimes(vi);

  expect(fetchMocker.spy).not.toHaveBeenCalled();

  await tickAFewTimes(vi);
  fetchMocker.reset();

  rep.pullURL = 'https://diff.com/pull';
  fetchMocker.post(rep.pullURL, {lastMutationID: 0, patch: []});

  rep.pullIgnorePromise();
  await tickAFewTimes(vi);
  expect(fetchMocker.spy.mock.calls.length).toBeGreaterThan(0);

  await tickAFewTimes(vi);
  fetchMocker.reset();
  fetchMocker.post(undefined, {});

  rep.pullURL = '';

  rep.pullIgnorePromise();
  await tickAFewTimes(vi);
  expect(fetchMocker.spy).not.toHaveBeenCalled();

  await tickAFewTimes(vi);
  fetchMocker.reset();
  fetchMocker.post(undefined, {});

  let pullerCallCount = 0;

  const consoleErrorStub = vi.spyOn(console, 'error');

  rep.puller = () => {
    pullerCallCount++;
    return Promise.resolve({
      httpRequestInfo: {
        httpStatusCode: 500,
        errorMessage: 'Test failure',
      },
    });
  };

  rep.pullIgnorePromise();
  await tickAFewTimes(vi);

  expect(fetchMocker.spy).not.toHaveBeenCalled();
  expect(pullerCallCount).toBeGreaterThan(0);

  expectConsoleLogContextStub(
    rep.name,
    consoleErrorStub.mock.calls[0],
    'Got a non 200 response doing pull: 500: Test failure',
    ['pull', requestIDLogContextRegex],
  );
  consoleErrorStub.mockRestore();

  await tickAFewTimes(vi);
  fetchMocker.reset();
  fetchMocker.post(undefined, {});
  pullerCallCount = 0;

  rep.puller = getDefaultPuller(rep);

  rep.pullIgnorePromise();
  await tickAFewTimes(vi);

  expect(fetchMocker.spy).not.toHaveBeenCalled();
  expect(pullerCallCount).toBe(0);
});

test('Client Group not found on server', async () => {
  const onClientStateNotFound = vi.fn();

  const rep = await replicacheForTesting(
    'client-group-not-found-pull',
    {
      onClientStateNotFound,
    },
    {
      ...disableAllBackgroundProcesses,
      enablePullAndPushInOpen: false,
    },
  );

  // oxlint-disable-next-line require-await
  const puller: Puller = async () => ({
    response: {error: 'ClientStateNotFound'},
    httpRequestInfo: {
      httpStatusCode: 200,
      errorMessage: '',
    },
  });

  expect(rep.isClientGroupDisabled).toBe(false);

  rep.puller = puller;
  rep.pullIgnorePromise();

  await waitForSync(rep);

  expect(rep.isClientGroupDisabled).toBe(true);
  expect(onClientStateNotFound).toHaveBeenCalledTimes(1);
});

test('Version not supported on server', async () => {
  const t = async (
    response: VersionNotSupportedResponse,
    reason: UpdateNeededReason,
  ) => {
    const rep = await replicacheForTesting(
      'version-not-supported-pull',
      undefined,
      disableAllBackgroundProcesses,
    );

    const {resolve, promise} = resolver();
    const onUpdateNeededStub = (rep.onUpdateNeeded = vi.fn(() => {
      resolve();
    }));

    // oxlint-disable-next-line require-await
    const puller: Puller = async () => ({
      response,
      httpRequestInfo: {
        httpStatusCode: 200,
        errorMessage: '',
      },
    });

    rep.puller = puller;
    rep.pullIgnorePromise();

    await promise;

    expect(onUpdateNeededStub).toHaveBeenCalledTimes(1);
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
