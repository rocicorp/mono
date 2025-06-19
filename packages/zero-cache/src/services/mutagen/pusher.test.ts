import * as ErrorKind from '../../../../zero-protocol/src/error-kind-enum.ts';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {combinePushes, PusherService} from './pusher.ts';
import type {
  Mutation,
  PushBody,
  PushResponse,
} from '../../../../zero-protocol/src/push.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {resolver} from '@rocicorp/resolver';
import {ErrorForClient} from '../../types/error-for-client.ts';

const config = {
  app: {
    id: 'zero',
    publications: [],
  },
  shard: {
    id: 'zero',
    num: 0,
  },
};

const clientID = 'test-cid';
const wsID = 'test-wsid';

describe('combine pushes', () => {
  test('empty array', () => {
    const [pushes, terminate] = combinePushes([]);
    expect(pushes).toEqual([]);
    expect(terminate).toBe(false);
  });

  test('stop', () => {
    const [pushes, terminate] = combinePushes([undefined]);
    expect(pushes).toEqual([]);
    expect(terminate).toBe(true);
  });

  test('stop after pushes', () => {
    const [pushes, terminate] = combinePushes([
      {
        push: makePush(1),
        jwt: 'a',
        httpCookie: undefined,

        clientID,
      },
      {
        push: makePush(1),
        jwt: 'a',
        httpCookie: undefined,

        clientID,
      },
      undefined,
    ]);
    expect(pushes).toHaveLength(1);
    expect(terminate).toBe(true);
  });

  test('stop in the middle', () => {
    const [pushes, terminate] = combinePushes([
      {
        push: makePush(1),
        jwt: 'a',
        httpCookie: undefined,

        clientID,
      },
      undefined,
      {
        push: makePush(1),
        jwt: 'a',
        httpCookie: undefined,

        clientID,
      },
    ]);
    expect(pushes).toHaveLength(1);
    expect(pushes[0].push.mutations).toHaveLength(1);
    expect(pushes[0].push.mutations[0].id).toBe(1);
    expect(terminate).toBe(true);
  });

  test('combines pushes for same clientID', () => {
    const [pushes, terminate] = combinePushes([
      {
        push: makePush(1, 'client1'),
        jwt: 'a',
        httpCookie: undefined,
        clientID: 'client1',
      },
      {
        push: makePush(2, 'client1'),
        jwt: 'a',
        httpCookie: undefined,
        clientID: 'client1',
      },
      {
        push: makePush(1, 'client2'),
        jwt: 'b',
        httpCookie: undefined,
        clientID: 'client2',
      },
    ]);

    expect(pushes).toHaveLength(2);
    expect(terminate).toBe(false);

    // Verify client1's pushes are combined
    const client1Push = pushes.find(p => p.clientID === 'client1');
    expect(client1Push).toBeDefined();
    expect(client1Push?.push.mutations).toHaveLength(3); // 1 + 2 mutations

    // Verify client2's push is separate
    const client2Push = pushes.find(p => p.clientID === 'client2');
    expect(client2Push).toBeDefined();
    expect(client2Push?.push.mutations).toHaveLength(1);
  });

  test('throws on jwt mismatch for same client', () => {
    expect(() =>
      combinePushes([
        {
          push: makePush(1, 'client1'),
          jwt: 'a',
          httpCookie: undefined,
          clientID: 'client1',
        },
        {
          push: makePush(2, 'client1'),
          jwt: 'b',
          httpCookie: undefined, // Different JWT
          clientID: 'client1',
        },
      ]),
    ).toThrow('jwt must be the same for all pushes with the same clientID');
  });

  test('throws on schema version mismatch for same client', () => {
    expect(() =>
      combinePushes([
        {
          push: {
            ...makePush(1, 'client1'),
            schemaVersion: 1,
          },
          jwt: 'a',
          httpCookie: undefined,
          clientID: 'client1',
        },
        {
          push: {
            ...makePush(2, 'client1'),
            schemaVersion: 2, // Different schema version
          },
          jwt: 'a',
          httpCookie: undefined,
          clientID: 'client1',
        },
      ]),
    ).toThrow(
      'schemaVersion must be the same for all pushes with the same clientID',
    );
  });

  test('throws on push version mismatch for same client', () => {
    expect(() =>
      combinePushes([
        {
          push: {
            ...makePush(1, 'client1'),
            pushVersion: 1,
          },
          jwt: 'a',
          httpCookie: undefined,
          clientID: 'client1',
        },
        {
          push: {
            ...makePush(2, 'client1'),
            pushVersion: 2, // Different push version
          },
          jwt: 'a',
          httpCookie: undefined,
          clientID: 'client1',
        },
      ]),
    ).toThrow(
      'pushVersion must be the same for all pushes with the same clientID',
    );
  });

  test('combines compatible pushes with same schema version and push version', () => {
    const [pushes, terminate] = combinePushes([
      {
        push: {
          ...makePush(1, 'client1'),
          schemaVersion: 1,
          pushVersion: 1,
        },
        jwt: 'a',
        httpCookie: undefined,
        clientID: 'client1',
      },
      {
        push: {
          ...makePush(2, 'client1'),
          schemaVersion: 1,
          pushVersion: 1,
        },
        jwt: 'a',
        httpCookie: undefined,
        clientID: 'client1',
      },
    ]);

    expect(pushes).toHaveLength(1);
    expect(terminate).toBe(false);
    expect(pushes[0].push.mutations).toHaveLength(3);
  });

  test('handles multiple clients with multiple pushes', () => {
    const [pushes, terminate] = combinePushes([
      {
        push: makePush(1, 'client1'),
        jwt: 'a',
        httpCookie: undefined,
        clientID: 'client1',
      },
      {
        push: makePush(2, 'client2'),
        jwt: 'b',
        httpCookie: undefined,
        clientID: 'client2',
      },
      {
        push: makePush(1, 'client1'),
        jwt: 'a',
        httpCookie: undefined,
        clientID: 'client1',
      },
      {
        push: makePush(3, 'client2'),
        jwt: 'b',
        httpCookie: undefined,
        clientID: 'client2',
      },
    ]);

    expect(pushes).toHaveLength(2);
    expect(terminate).toBe(false);

    // Verify client1's pushes are combined
    const client1Push = pushes.find(p => p.clientID === 'client1');
    expect(client1Push?.push.mutations).toHaveLength(2);

    // Verify client2's pushes are combined
    const client2Push = pushes.find(p => p.clientID === 'client2');
    expect(client2Push?.push.mutations).toHaveLength(5);
  });

  test('preserves mutation order within client', () => {
    const [pushes] = combinePushes([
      {
        push: makePush(1, 'client1'),
        jwt: 'a',
        httpCookie: undefined,
        clientID: 'client1',
      },
      {
        push: makePush(1, 'client2'),
        jwt: 'b',
        httpCookie: undefined,
        clientID: 'client2',
      },
      {
        push: makePush(1, 'client1'),
        jwt: 'a',
        httpCookie: undefined,
        clientID: 'client1',
      },
    ]);

    const client1Push = pushes.find(p => p.clientID === 'client1');
    expect(client1Push?.push.mutations[0].id).toBeLessThan(
      client1Push?.push.mutations[1].id || 0,
    );
  });
});

const lc = createSilentLogContext();
describe('pusher service', () => {
  test('the service can be stopped', async () => {
    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );
    let shutDown = false;
    void pusher.run().then(() => {
      shutDown = true;
    });
    await pusher.stop();
    expect(shutDown).toBe(true);
  });

  test('the service sets authorization headers', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
    });

    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );
    void pusher.run();
    pusher.initConnection(clientID, wsID, undefined);

    pusher.enqueuePush(clientID, makePush(1), 'jwt', undefined);

    await pusher.stop();

    expect(fetch.mock.calls[0][1]?.headers).toEqual({
      'Content-Type': 'application/json',
      'X-Api-Key': 'api-key',
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'Authorization': 'Bearer jwt',
    });

    fetch.mockReset();
  });

  test('the service sends the app id and schema over the query params', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
    });

    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );
    void pusher.run();
    pusher.initConnection(clientID, wsID, undefined);

    pusher.enqueuePush(clientID, makePush(1), 'jwt', undefined);

    await pusher.stop();

    expect(fetch.mock.calls[0][0]).toMatchInlineSnapshot(
      `"http://example.com/?schema=zero_0&appID=zero"`,
    );

    fetch.mockReset();
  });

  test('the service correctly batches pushes when the API server is delayed', async () => {
    const fetch = (global.fetch = vi.fn());
    const apiServerReturn = resolver();
    fetch.mockImplementation(async (_url: string, _options: RequestInit) => {
      await apiServerReturn.promise;
    });

    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );

    void pusher.run();
    pusher.initConnection(clientID, wsID, undefined);
    pusher.enqueuePush(clientID, makePush(1), 'jwt', undefined);
    // release control of the loop so the push can be sent
    await Promise.resolve();

    // We should have sent the first push
    expect(fetch.mock.calls).toHaveLength(1);
    expect(JSON.parse(fetch.mock.calls[0][1].body).mutations).toHaveLength(1);

    // We have not resolved the API server yet so these should stack up
    pusher.enqueuePush(clientID, makePush(1), 'jwt', undefined);
    await Promise.resolve();
    pusher.enqueuePush(clientID, makePush(1), 'jwt', undefined);
    await Promise.resolve();
    pusher.enqueuePush(clientID, makePush(1), 'jwt', undefined);
    await Promise.resolve();

    // no new pushes sent yet since we are still waiting on the user's API server
    expect(fetch.mock.calls).toHaveLength(1);

    // let the API server go
    apiServerReturn.resolve();
    // wait for the pusher to finish
    await new Promise(resolve => {
      setTimeout(resolve, 0);
    });

    // We sent all the pushes in one batch
    expect(JSON.parse(fetch.mock.calls[1][1].body).mutations).toHaveLength(3);
    expect(fetch.mock.calls).toHaveLength(2);
  });

  test('the service does not forward cookies if forwardCookies is false', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
    });

    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );
    void pusher.run();
    pusher.initConnection(clientID, wsID, undefined);

    pusher.enqueuePush(clientID, makePush(1), 'jwt', 'my-cookie');

    await pusher.stop();

    expect(fetch.mock.calls[0][1]?.headers).not.toHaveProperty('Cookie');
  });

  test('the service forwards cookies if forwardCookies is true', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
    });

    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: true,
      },
      lc,
      'cgid',
    );
    void pusher.run();
    pusher.initConnection(clientID, wsID, undefined);

    pusher.enqueuePush(clientID, makePush(1), 'jwt', 'my-cookie');

    await pusher.stop();

    expect(fetch.mock.calls[0][1]?.headers).toHaveProperty(
      'Cookie',
      'my-cookie',
    );
  });
});

describe('initConnection', () => {
  test('initConnection returns a stream', () => {
    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );
    void pusher.run();

    // const result = pusher.initConnection(clientID, wsID, undefined);
    // expect(result.type).toBe('stream');
  });

  test('initConnection throws if it was already called for the same clientID and wsID', () => {
    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );
    void pusher.run();
    pusher.initConnection('c1', 'ws1', undefined);
    expect(() => pusher.initConnection('c1', 'ws1', undefined)).toThrow(
      'Connection was already initialized',
    );
  });

  test('initConnection destroys prior stream for same client when wsID changes', async () => {
    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );
    void pusher.run();
    const stream1 = pusher.initConnection('c1', 'ws1', undefined);
    pusher.initConnection('c1', 'ws2', undefined);
    const iterator = stream1[Symbol.asyncIterator]();
    await expect(iterator.next()).resolves.toEqual({
      done: true,
      value: undefined,
    });
  });

  test('initConnection passes userPushParams to fetch', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({mutations: []}),
    });

    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );
    void pusher.run();

    const userPushParams = {
      queryParams: {workspace: '1', user: '2', foo: 'bar'},
    };

    pusher.initConnection(clientID, wsID, userPushParams);
    pusher.enqueuePush(clientID, makePush(1), 'jwt', undefined);

    // Wait for the push to be processed
    await new Promise(resolve => setTimeout(resolve, 0));

    // Verify the custom URL was used
    expect(fetch.mock.calls[0][0]).toMatchInlineSnapshot(
      `"http://example.com/?workspace=1&user=2&foo=bar&schema=zero_0&appID=zero"`,
    );

    // Verify the headers were passed through
    expect(fetch.mock.calls[0][1]?.headers).toEqual({
      'Content-Type': 'application/json',
      'X-Api-Key': 'api-key',
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'Authorization': 'Bearer jwt',
    });

    await pusher.stop();
  });
});

describe('pusher streaming', () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  test('fails if we receive a push before initializing the connection', () => {});

  test('returns ok for subsequent pushes from same client', () => {
    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );
    void pusher.run();

    pusher.initConnection(clientID, wsID, undefined);
    pusher.enqueuePush(clientID, makePush(1), 'jwt', undefined);
    const result = pusher.enqueuePush(clientID, makePush(1), 'jwt', undefined);
    expect(result.type).toBe('ok');
  });

  test('streams successful push response to correct client', async () => {
    const fetch = (global.fetch = vi.fn());
    const successResponse1: PushResponse = {
      mutations: [
        {
          id: {clientID: 'client1', id: 1},
          result: {},
        },
      ],
    };
    const successResponse2: PushResponse = {
      mutations: [
        {
          id: {clientID: 'client2', id: 1},
          result: {},
        },
      ],
    };

    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );
    void pusher.run();
    const stream1 = pusher.initConnection('client1', 'ws1', undefined);
    const stream2 = pusher.initConnection('client2', 'ws2', undefined);

    fetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve(successResponse1),
    });
    pusher.enqueuePush('client1', makePush(1, 'client1'), 'jwt', undefined);
    await new Promise(resolve => setTimeout(resolve, 0));

    fetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve(successResponse2),
    });
    pusher.enqueuePush('client2', makePush(2, 'client2'), 'jwt', undefined);

    const s1Messages: unknown[] = [];
    const s2Messages: unknown[] = [];
    for await (const response of stream1) {
      s1Messages.push(response);
      break;
    }
    for await (const response of stream2) {
      s2Messages.push(response);
      break;
    }

    expect(s1Messages).toMatchInlineSnapshot(`
        [
          [
            "pushResponse",
            {
              "mutations": [
                {
                  "id": {
                    "clientID": "client1",
                    "id": 1,
                  },
                  "result": {},
                },
              ],
            },
          ],
        ]
      `);
    expect(s2Messages).toMatchInlineSnapshot(`
        [
          [
            "pushResponse",
            {
              "mutations": [
                {
                  "id": {
                    "clientID": "client2",
                    "id": 1,
                  },
                  "result": {},
                },
              ],
            },
          ],
        ]
      `);
  });

  test('streams error response to affected clients', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: false,
      status: 500,
      text: () => Promise.resolve('Internal Server Error'),
    });

    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );
    void pusher.run();
    const stream1 = pusher.initConnection('client1', 'ws1', undefined);
    const stream2 = pusher.initConnection('client2', 'ws2', undefined);

    pusher.enqueuePush('client1', makePush(1, 'client1'), 'jwt', undefined);
    pusher.enqueuePush('client2', makePush(1, 'client2'), 'jwt', undefined);

    const messages1: unknown[] = [];
    const messages2: unknown[] = [];
    // Wait for push to be processed
    await new Promise(resolve => setTimeout(resolve, 0));

    for await (const msg of stream1) {
      messages1.push(msg);
      break;
    }
    for await (const msg of stream2) {
      messages2.push(msg);
      break;
    }

    expect(messages1).toEqual([
      [
        'pushResponse',
        {
          error: 'http',
          status: 500,
          details: 'Internal Server Error',
          mutationIDs: [{clientID: 'client1', id: 1}],
        },
      ],
    ]);

    expect(messages2).toEqual([
      [
        'pushResponse',
        {
          error: 'http',
          status: 500,
          details: 'Internal Server Error',
          mutationIDs: [{clientID: 'client2', id: 2}],
        },
      ],
    ]);
  });

  test('handles network errors', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockRejectedValue(new Error('Network error'));

    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );
    void pusher.run();
    const stream = pusher.initConnection(clientID, wsID, undefined);

    pusher.enqueuePush(clientID, makePush(1, clientID), 'jwt', undefined);

    const messages: unknown[] = [];
    for await (const msg of stream) {
      messages.push(msg);
      break;
    }

    expect(messages).toEqual([
      [
        'pushResponse',
        {
          error: 'zeroPusher',
          details: 'Error: Network error',
          mutationIDs: [{clientID, id: 1}],
        },
      ],
    ]);
  });

  test('cleanup removes client subscription', () => {
    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );
    void pusher.run();

    const stream1 = pusher.initConnection(clientID, 'ws1', undefined);

    pusher.enqueuePush(clientID, makePush(1, clientID), 'jwt', undefined);

    stream1.cancel();

    // After cleanup, should get a new stream (even if same wsid)
    expect(() =>
      pusher.initConnection(clientID, 'ws1', undefined),
    ).not.toThrow();
  });

  test('new websocket for same client creates new downstream', async () => {
    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );
    void pusher.run();

    const stream1 = pusher.initConnection(clientID, 'ws1', undefined);
    pusher.initConnection(clientID, 'ws2', undefined);

    // should not be iterable anymore as it is closed by the arrival of ws2
    const iterator = stream1[Symbol.asyncIterator]();
    await expect(iterator.next()).resolves.toEqual({
      done: true,
      value: undefined,
    });
  });

  test('fails the stream on ooo mutations', async () => {
    const fetch = (global.fetch = vi.fn());
    const oooResponse: PushResponse = {
      mutations: [
        {
          id: {clientID, id: 3},
          result: {},
        },
        {
          id: {clientID, id: 1},
          result: {error: 'oooMutation'},
        },
      ],
    };

    fetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve(oooResponse),
    });

    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );
    void pusher.run();

    const stream = pusher.initConnection(clientID, 'ws1', undefined);
    pusher.enqueuePush(clientID, makePush(2, clientID), 'jwt', undefined);

    const messages: unknown[] = [];
    for await (const msg of stream) {
      messages.push(msg);
      break;
    }

    expect(messages).toMatchInlineSnapshot(`
        [
          [
            "pushResponse",
            {
              "mutations": [
                {
                  "id": {
                    "clientID": "test-cid",
                    "id": 3,
                  },
                  "result": {},
                },
              ],
            },
          ],
        ]
      `);

    // The stream should be completed after the OOO mutation
    expect(await stream[Symbol.asyncIterator]().next()).toEqual({
      done: true,
      value: undefined,
    });
  });

  test('fails the stream on unsupported schema version or push version', async () => {
    const fetch = (global.fetch = vi.fn());
    const errorResponse: PushResponse = {
      error: 'unsupportedSchemaVersion',
      mutationIDs: [{clientID, id: 1}],
    };

    fetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve(errorResponse),
    });

    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );
    void pusher.run();

    const stream = pusher.initConnection(clientID, 'ws1', undefined);
    pusher.enqueuePush(clientID, makePush(1, clientID), 'jwt', undefined);

    await expect(stream[Symbol.asyncIterator]().next()).rejects.toThrow(
      'unsupportedSchemaVersion',
    );
  });

  test('fails the stream with AuthInvalidated error on 401 response', async () => {
    const fetch = (global.fetch = vi.fn());

    fetch.mockResolvedValue({
      ok: false,
      status: 401,
      text: () => Promise.resolve('Unauthorized access'),
    });

    const pusher = new PusherService(
      config,
      {
        url: 'http://example.com',
        apiKey: 'api-key',
        forwardCookies: false,
      },
      lc,
      'cgid',
    );
    void pusher.run();

    const stream = pusher.initConnection(clientID, 'ws1', undefined);
    pusher.enqueuePush(clientID, makePush(1, clientID), 'jwt', undefined);

    await expect(stream[Symbol.asyncIterator]().next()).rejects.toThrow(
      new ErrorForClient({
        kind: ErrorKind.AuthInvalidated,
        message: 'Unauthorized access',
      }),
    );
  });
});

let timestamp = 0;
let id = 0;

beforeEach(() => {
  timestamp = 0;
  id = 0;
});

function makePush(numMutations: number, clientID?: string): PushBody {
  return {
    clientGroupID: 'cgid',
    mutations: Array.from({length: numMutations}, () => makeMutation(clientID)),
    pushVersion: 1,
    requestID: 'rid',
    schemaVersion: 1,
    timestamp: ++timestamp,
  };
}

function makeMutation(clientID?: string): Mutation {
  return {
    type: 'custom',
    args: [],
    clientID: clientID ?? 'cid',
    id: ++id,
    name: 'n',
    timestamp: ++timestamp,
  } as const;
}
