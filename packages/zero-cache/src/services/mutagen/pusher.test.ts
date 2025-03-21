import {beforeEach, describe, expect, test, vi} from 'vitest';
import {combinePushes, PusherService} from './pusher.ts';
import type {
  Mutation,
  PushBody,
  PushResponse,
} from '../../../../zero-protocol/src/push.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {resolver} from '@rocicorp/resolver';

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

const clientID = 'test';
describe('combine pushes', () => {
  test('empty array', () => {
    const [pushes, terminate] = combinePushes([]);
    expect(pushes).toEqual([]);
    expect(terminate).toBe(false);
  });

  test('same JWT for all pushes', () => {
    const [pushes, terminate] = combinePushes([
      {
        push: makePush(1),
        jwt: 'a',
      },
      {
        push: makePush(1),
        jwt: 'a',
      },
      {
        push: makePush(1),
        jwt: 'a',
      },
    ]);
    expect(pushes).toHaveLength(1);
    expect(terminate).toBe(false);
    expect(pushes[0].push.mutations).toHaveLength(3);
  });

  test('different JWT groups', () => {
    const [pushes, terminate] = combinePushes([
      {
        push: makePush(1),
        jwt: 'a',
      },
      {
        push: makePush(1),
        jwt: 'a',
      },
      {
        push: makePush(1),
        jwt: 'c',
      },
      {
        push: makePush(1),
        jwt: 'b',
      },
      {
        push: makePush(1),
        jwt: 'b',
      },
      {
        push: makePush(1),
        jwt: 'c',
      },
    ]);
    expect(pushes).toHaveLength(4);
    expect(terminate).toBe(false);
    expect(pushes[0].push.mutations).toHaveLength(2);
    expect(pushes[0].jwt).toBe('a');
    expect(pushes[1].push.mutations).toHaveLength(1);
    expect(pushes[1].jwt).toBe('c');
    expect(pushes[2].push.mutations).toHaveLength(2);
    expect(pushes[2].jwt).toBe('b');
    expect(pushes[3].push.mutations).toHaveLength(1);
    expect(pushes[3].jwt).toBe('c');
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
      },
      {
        push: makePush(1),
        jwt: 'a',
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
      },
      undefined,
      {
        push: makePush(1),
        jwt: 'a',
      },
    ]);
    expect(pushes).toHaveLength(1);
    expect(terminate).toBe(true);
  });
});

const lc = createSilentLogContext();
describe('pusher service', () => {
  test('the service can be stopped', async () => {
    const pusher = new PusherService(
      config,
      lc,
      'cgid',
      'http://exmaple.com',
      undefined,
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
      lc,
      'cgid',
      'http://exmaple.com',
      'api-key',
    );
    void pusher.run();

    pusher.enqueuePush(clientID, makePush(1), 'jwt');

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
      lc,
      'cgid',
      'http://exmaple.com',
      'api-key',
    );
    void pusher.run();

    pusher.enqueuePush(clientID, makePush(1), 'jwt');

    await pusher.stop();

    expect(fetch.mock.calls[0][0]).toMatchInlineSnapshot(
      `"http://exmaple.com?schema=zero_0&appID=zero"`,
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
      lc,
      'cgid',
      'http://exmaple.com',
      'api-key',
    );

    void pusher.run();
    pusher.enqueuePush(clientID, makePush(1), 'jwt');
    // release control of the loop so the push can be sent
    await Promise.resolve();

    // We should have sent the first push
    expect(fetch.mock.calls).toHaveLength(1);
    expect(JSON.parse(fetch.mock.calls[0][1].body).mutations).toHaveLength(1);

    // We have not resolved the API server yet so these should stack up
    pusher.enqueuePush(clientID, makePush(1), 'jwt');
    await Promise.resolve();
    pusher.enqueuePush(clientID, makePush(1), 'jwt');
    await Promise.resolve();
    pusher.enqueuePush(clientID, makePush(1), 'jwt');
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
});

describe('pusher streaming', () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  test('returns a stream for first push from a client', () => {
    const pusher = new PusherService(
      config,
      lc,
      'cgid',
      'http://example.com',
      'api-key',
    );
    void pusher.run();

    const result = pusher.enqueuePush(clientID, makePush(1), 'jwt');
    expect(result.type).toBe('stream');
  });

  test('returns ok for subsequent pushes from same client', () => {
    const pusher = new PusherService(
      config,
      lc,
      'cgid',
      'http://example.com',
      'api-key',
    );
    void pusher.run();

    pusher.enqueuePush(clientID, makePush(1), 'jwt');
    const result = pusher.enqueuePush(clientID, makePush(1), 'jwt');
    expect(result.type).toBe('ok');
  });

  test('streams successful push response to correct client', async () => {
    const fetch = (global.fetch = vi.fn());
    const successResponse: PushResponse = {
      mutations: [
        {
          id: {clientID: 'client1', id: 1},
          result: {timestamp: 123, cookie: 'abc'},
        },
      ],
    };
    fetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve(successResponse),
    });

    const pusher = new PusherService(
      config,
      lc,
      'cgid',
      'http://example.com',
      'api-key',
    );
    void pusher.run();

    const result1 = pusher.enqueuePush('client1', makePush(1), 'jwt');
    const result2 = pusher.enqueuePush('client2', makePush(1), 'jwt');

    expect(result1.type).toBe('stream');
    expect(result2.type).toBe('stream');

    if (result1.type === 'stream') {
      const messages: unknown[] = [];
      result1.stream.subscribe(msg => messages.push(msg));

      // Wait for push to be processed
      await new Promise(resolve => setTimeout(resolve, 0));

      expect(messages).toEqual([
        [
          'push-response',
          {
            mutations: successResponse.mutations,
          },
        ],
      ]);
    }
  });

  test('streams error response to affected clients', async () => {
    const fetch = (global.fetch = vi.fn());
    const errorResponse: PushResponse = {
      error: 'http',
      status: 500,
      details: 'Internal Server Error',
      mutationIDs: [
        {clientID: 'client1', id: 1},
        {clientID: 'client2', id: 1},
      ],
    };
    fetch.mockResolvedValue({
      ok: false,
      status: 500,
      text: () => Promise.resolve('Internal Server Error'),
    });

    const pusher = new PusherService(
      config,
      lc,
      'cgid',
      'http://example.com',
      'api-key',
    );
    void pusher.run();

    const result1 = pusher.enqueuePush('client1', makePush(1), 'jwt');
    const result2 = pusher.enqueuePush('client2', makePush(1), 'jwt');

    expect(result1.type).toBe('stream');
    expect(result2.type).toBe('stream');

    if (result1.type === 'stream' && result2.type === 'stream') {
      const messages1: unknown[] = [];
      const messages2: unknown[] = [];
      result1.stream.subscribe(msg => messages1.push(msg));
      result2.stream.subscribe(msg => messages2.push(msg));

      // Wait for push to be processed
      await new Promise(resolve => setTimeout(resolve, 0));

      expect(messages1).toEqual([
        [
          'push-response',
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
          'push-response',
          {
            error: 'http',
            status: 500,
            details: 'Internal Server Error',
            mutationIDs: [{clientID: 'client2', id: 1}],
          },
        ],
      ]);
    }
  });

  test('handles network errors', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockRejectedValue(new Error('Network error'));

    const pusher = new PusherService(
      config,
      lc,
      'cgid',
      'http://example.com',
      'api-key',
    );
    void pusher.run();

    const result = pusher.enqueuePush(clientID, makePush(1), 'jwt');
    expect(result.type).toBe('stream');

    if (result.type === 'stream') {
      const messages: unknown[] = [];
      result.stream.subscribe(msg => messages.push(msg));

      // Wait for push to be processed
      await new Promise(resolve => setTimeout(resolve, 0));

      expect(messages).toEqual([
        [
          'push-response',
          {
            error: 'zero-pusher',
            details: 'Error: Network error',
            mutationIDs: [{clientID, id: 1}],
          },
        ],
      ]);
    }
  });

  test('cleanup removes client subscription', () => {
    const pusher = new PusherService(
      config,
      lc,
      'cgid',
      'http://example.com',
      'api-key',
    );
    void pusher.run();

    const result1 = pusher.enqueuePush(clientID, makePush(1), 'jwt');
    expect(result1.type).toBe('stream');

    if (result1.type === 'stream') {
      result1.stream.cleanup();

      // After cleanup, should get a new stream
      const result2 = pusher.enqueuePush(clientID, makePush(1), 'jwt');
      expect(result2.type).toBe('stream');
    }
  });
});

let timestamp = 0;
let id = 0;
function makePush(numMutations: number): PushBody {
  return {
    clientGroupID: 'cgid',
    mutations: Array.from({length: numMutations}, makeMutation),
    pushVersion: 1,
    requestID: 'rid',
    schemaVersion: 1,
    timestamp: ++timestamp,
  };
}

function makeMutation(): Mutation {
  return {
    type: 'custom',
    args: [],
    clientID: 'cid',
    id: ++id,
    name: 'n',
    timestamp: ++timestamp,
  } as const;
}
