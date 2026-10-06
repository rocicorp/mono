import {resolver} from '@rocicorp/resolver';
import {afterEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../../zero-protocol/src/error-reason.ts';
import type {PushFailedBody} from '../../../../zero-protocol/src/error.ts';
import type {Mutation} from '../../../../zero-protocol/src/mutation.ts';
import type {PushBody} from '../../../../zero-protocol/src/push.ts';
import {ProtocolErrorWithLevel} from '../../types/error-with-level.ts';
import {
  ConnectionContextManagerImpl,
  type ConnectionSelector,
} from '../view-syncer/connection-context-manager.ts';
import {PusherService} from './pusher.ts';

const lc = createSilentLogContext();
const config = {
  app: {id: 'zero', publications: []},
  shard: {id: 'zero', num: 0},
};

function newPusher() {
  const connContextManager = new ConnectionContextManagerImpl(
    lc,
    undefined,
    undefined,
    {
      url: undefined,
      apiKey: undefined,
      allowedClientHeaders: undefined,
      allowedRequestHeaders: undefined,
      forwardCookies: false,
    },
    {
      url: ['http://example.com'],
      apiKey: undefined,
      allowedClientHeaders: undefined,
      allowedRequestHeaders: undefined,
      forwardCookies: false,
    },
  );
  const pusher = new PusherService(config, lc, 'cgid', connContextManager);

  function openConnection(clientID: string, wsID: string) {
    const selector: ConnectionSelector = {clientID, wsID};
    connContextManager.registerConnection(selector, {
      protocolVersion: 0,
      clientID,
      clientGroupID: 'cgid',
      profileID: null,
      baseCookie: null,
      timestamp: Date.now(),
      lmID: 0,
      wsID,
      debugPerf: false,
      auth: undefined,
      userID: undefined,
      initConnectionMsg: undefined,
      httpCookie: undefined,
      origin: undefined,
      requestHeaders: undefined,
    });
    connContextManager.initConnection(selector, {desiredQueriesPatch: []});
    return {selector, stream: pusher.initConnection(selector)};
  }

  return {pusher, openConnection};
}

let timestamp = 0;

function makePush(clientID: string, ...ids: number[]): PushBody {
  return {
    clientGroupID: 'cgid',
    mutations: ids.map(
      id =>
        ({
          type: 'custom',
          args: [],
          clientID,
          id,
          name: 'n',
          timestamp: ++timestamp,
        }) satisfies Mutation,
    ),
    pushVersion: 1,
    requestID: 'rid',
    schemaVersion: 1,
    timestamp: ++timestamp,
  };
}

function pushFailed(clientID: string, ...ids: number[]): PushFailedBody {
  return {
    kind: ErrorKind.PushFailed,
    origin: ErrorOrigin.Server,
    reason: ErrorReason.Database,
    message: 'timeout exceeded when trying to connect',
    mutationIDs: ids.map(id => ({clientID, id})),
  };
}

function okResponse(clientID: string, ...ids: number[]) {
  return {
    kind: 'MutateResponse',
    userID: null,
    mutations: ids.map(id => ({id: {clientID, id}, result: {}})),
  };
}

/**
 * A fetch whose responses are released one at a time, so a test can enqueue
 * pushes while an earlier request is still in flight.
 */
function controlledFetch() {
  const pending: {
    body: PushBody;
    respond: (json: unknown) => void;
  }[] = [];
  const arrived: (() => void)[] = [];
  const fetch = vi.fn(async (_url: string, init: RequestInit) => {
    const {promise, resolve} = resolver<unknown>();
    pending.push({body: JSON.parse(init.body as string), respond: resolve});
    arrived.shift()?.();
    const json = await promise;
    return {ok: true, json: () => Promise.resolve(json)};
  });
  global.fetch = fetch as unknown as typeof global.fetch;

  return {
    fetch,
    /** Resolves once the nth request (1-based) has been made. */
    async request(n: number) {
      while (pending.length < n) {
        const {promise, resolve} = resolver<void>();
        arrived.push(resolve);
        await promise;
      }
      return pending[n - 1];
    },
  };
}

function sentMutationIDs(fetch: ReturnType<typeof controlledFetch>['fetch']) {
  return fetch.mock.calls.map(([, init]) =>
    (JSON.parse(init.body as string) as PushBody).mutations.map(
      m => `${m.clientID}:${m.id}`,
    ),
  );
}

function flush() {
  return new Promise(resolve => setTimeout(resolve, 0));
}

describe('pushes queued behind a failed push', () => {
  const originalFetch = global.fetch;
  afterEach(() => {
    global.fetch = originalFetch;
  });

  test('are not sent once the failed push has failed the connection', async () => {
    const api = controlledFetch();
    const {pusher, openConnection} = newPusher();
    void pusher.run();

    const {selector, stream} = openConnection('c1', 'ws1');
    const failure = stream[Symbol.asyncIterator]().next();

    pusher.enqueuePush(selector, makePush('c1', 1));
    const first = await api.request(1);

    // Mutation 2 arrives while mutation 1 is still at the API server, so it is
    // queued behind it rather than combined with it.
    pusher.enqueuePush(selector, makePush('c1', 2));
    first.respond(pushFailed('c1', 1));

    await expect(failure).rejects.toBeInstanceOf(ProtocolErrorWithLevel);
    await expect(failure).rejects.toMatchObject({
      errorBody: {reason: ErrorReason.Database, mutationIDs: [{id: 1}]},
    });

    await flush();
    // Sending mutation 2 alone would leave a gap at mutation 1, which the API
    // server can only reject as out of order.
    expect(sentMutationIDs(api.fetch)).toEqual([['c1:1']]);

    await pusher.stop();
  });

  test('are sent again once the client reconnects on a new socket', async () => {
    const api = controlledFetch();
    const {pusher, openConnection} = newPusher();
    void pusher.run();

    const conn1 = openConnection('c1', 'ws1');
    const failure = conn1.stream[Symbol.asyncIterator]().next();
    pusher.enqueuePush(conn1.selector, makePush('c1', 1));
    const first = await api.request(1);
    pusher.enqueuePush(conn1.selector, makePush('c1', 2));
    first.respond(pushFailed('c1', 1));
    await expect(failure).rejects.toBeInstanceOf(ProtocolErrorWithLevel);

    // The client reconnects and re-sends everything that is still pending.
    const conn2 = openConnection('c1', 'ws2');
    pusher.enqueuePush(conn2.selector, makePush('c1', 1));
    pusher.enqueuePush(conn2.selector, makePush('c1', 2));
    const resend = await api.request(2);
    resend.respond(okResponse('c1', 1, 2));
    await flush();

    expect(sentMutationIDs(api.fetch)).toEqual([['c1:1'], ['c1:1', 'c1:2']]);

    await pusher.stop();
  });

  test("do not hold back another client's pushes", async () => {
    const api = controlledFetch();
    const {pusher, openConnection} = newPusher();
    void pusher.run();

    const c1 = openConnection('c1', 'ws1');
    const c2 = openConnection('c2', 'ws2');
    const c1Failure = c1.stream[Symbol.asyncIterator]().next();

    pusher.enqueuePush(c1.selector, makePush('c1', 1));
    const first = await api.request(1);
    pusher.enqueuePush(c1.selector, makePush('c1', 2));
    pusher.enqueuePush(c2.selector, makePush('c2', 7));
    first.respond(pushFailed('c1', 1));
    await expect(c1Failure).rejects.toBeInstanceOf(ProtocolErrorWithLevel);

    const second = await api.request(2);
    second.respond(okResponse('c2', 7));
    await flush();

    expect(sentMutationIDs(api.fetch)).toEqual([['c1:1'], ['c2:7']]);

    await pusher.stop();
  });

  test('are still sent after a push that succeeded', async () => {
    const api = controlledFetch();
    const {pusher, openConnection} = newPusher();
    void pusher.run();

    const {selector} = openConnection('c1', 'ws1');
    pusher.enqueuePush(selector, makePush('c1', 1));
    const first = await api.request(1);
    pusher.enqueuePush(selector, makePush('c1', 2));
    first.respond(okResponse('c1', 1));

    const second = await api.request(2);
    second.respond(okResponse('c1', 2));
    await flush();

    expect(sentMutationIDs(api.fetch)).toEqual([['c1:1'], ['c1:2']]);

    await pusher.stop();
  });
});
