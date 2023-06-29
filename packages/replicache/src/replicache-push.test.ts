import {expect} from 'chai';
import * as sinon from 'sinon';
import type {VersionNotSupportedResponse, WriteTransaction} from './mod.js';
import type {Pusher} from './pusher.js';
import {
  TestLogSink,
  disableAllBackgroundProcesses,
  initReplicacheTesting,
  replicacheForTesting,
  tickAFewTimes,
} from './test-util.js';

// fetch-mock has invalid d.ts file so we removed that on npm install.
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-expect-error
import fetchMock from 'fetch-mock/esm/client';
import {assert} from 'shared/src/asserts.js';
import {getDefaultPusher} from './get-default-pusher.js';
import type {UpdateNeededReason} from './replicache.js';

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
        await tx.put(`/todo/${args.id}`, args);
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

  fetchMock.postOnce(pushURL, {
    mutationInfos: [
      {id: 1, error: 'deleteTodo: todo not found'},
      {id: 2, error: 'deleteTodo: todo not found'},
    ],
  });
  await tickAFewTimes();
  const {mutations} = await fetchMock.lastCall().request.json();
  const clientID = await rep.clientID;
  expect(mutations).to.deep.equal([
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
  ).to.equal('Test');

  fetchMock.postOnce(pushURL, {
    mutationInfos: [{id: 3, error: 'mutation has already been processed'}],
  });
  await tickAFewTimes();
  {
    const {mutations} = await fetchMock.lastCall().request.json();
    expect(mutations).to.deep.equal([
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
  ).to.equal('Test 2');

  // Clean up
  await deleteTodo({id: id1});
  await deleteTodo({id: id2});

  fetchMock.postOnce(pushURL, {
    mutationInfos: [],
  });
  await tickAFewTimes();
  {
    const {mutations} = await fetchMock.lastCall().request.json();
    expect(mutations).to.deep.equal([
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
          await tx.put(`/todo/${args.id}`, args);
        },
      },
    },
    {useDefaultURLs: false},
  );

  const {createTodo} = rep.mutate;

  await tickAFewTimes();
  fetchMock.reset();
  fetchMock.postAny({});

  await createTodo({id: 'id1'});
  await tickAFewTimes();

  expect(fetchMock.calls()).to.have.length(0);

  await tickAFewTimes();
  fetchMock.reset();
  fetchMock.postAny({});

  rep.pushURL = 'https://diff.com/push';

  await createTodo({id: 'id2'});
  await tickAFewTimes();
  expect(fetchMock.calls()).to.have.length(1);

  await tickAFewTimes();
  fetchMock.reset();
  fetchMock.postAny({});

  rep.pushURL = '';

  await createTodo({id: 'id3'});
  await tickAFewTimes();
  expect(fetchMock.calls()).to.have.length(0);

  await tickAFewTimes();
  fetchMock.reset();
  fetchMock.postAny({});
  let pusherCallCount = 0;

  // eslint-disable-next-line require-await
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
  await tickAFewTimes();

  expect(fetchMock.calls()).to.have.length(0);
  expect(pusherCallCount).to.equal(1);

  await tickAFewTimes();
  fetchMock.reset();
  fetchMock.postAny({});
  pusherCallCount = 0;

  rep.pusher = getDefaultPusher(rep);

  await createTodo({id: 'id5'});
  await tickAFewTimes();

  expect(fetchMock.calls()).to.have.length(0);
  expect(pusherCallCount).to.equal(0);
});

test('Version not supported on server', async () => {
  const t = async (
    response: VersionNotSupportedResponse,
    reason: UpdateNeededReason,
  ) => {
    const rep = await replicacheForTesting('version-not-supported-push', {
      mutators: {
        noop: () => undefined,
      },
      ...disableAllBackgroundProcesses,
    });

    const onUpdateNeededStub = (rep.onUpdateNeeded = sinon.stub());

    // eslint-disable-next-line require-await
    const pusher: Pusher = async () => ({
      response,
      httpRequestInfo: {
        httpStatusCode: 200,
        errorMessage: '',
      },
    });

    rep.pusher = pusher as Pusher;

    await rep.mutate.noop();
    await rep.invokePush();

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

test('ClientStateNotFound on server', async () => {
  const testLogSink = new TestLogSink();
  const rep = await replicacheForTesting('client-state-not-found-push', {
    mutators: {
      noop: () => undefined,
    },
    logSinks: [testLogSink],
    logLevel: 'error',
    ...disableAllBackgroundProcesses,
  });

  const onUpdateNeededStub = (rep.onUpdateNeeded = sinon.stub());

  // eslint-disable-next-line require-await
  const pusher: Pusher = async () => ({
    response: {error: 'ClientStateNotFound'},
    httpRequestInfo: {
      httpStatusCode: 200,
      errorMessage: '',
    },
  });

  rep.pusher = pusher as Pusher;

  await rep.mutate.noop();
  await rep.invokePush();

  expect(onUpdateNeededStub.callCount).equal(0);

  expect(rep.isClientGroupDisabled).true;

  expect(testLogSink.messages.length).equal(1);
  expect(testLogSink.messages[0][2][0]).instanceOf(Error);
  assert(testLogSink.messages[0][2][0] instanceof Error);
  expect(testLogSink.messages[0][2][0].message).match(
    /Client group \S+ is unknown on server/,
  );
});
