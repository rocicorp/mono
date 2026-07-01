import {beforeEach, describe, expect, test, vi} from 'vitest';
import type {ConnectionState, Schema, ZeroOptions} from './zero.ts';
import type * as ZeroClientModule from './zero.ts';

function createMockZeroInstance(clientID = 'test-client') {
  const unsubscribe = vi.fn();
  const subscribe = vi.fn(
    (_cb: (state: ConnectionState) => void) => unsubscribe,
  );

  return {
    instance: {
      clientID,
      close: vi.fn().mockResolvedValue(undefined),
      online: true,
      connection: {
        connect: vi.fn().mockResolvedValue(undefined),
        state: {
          current: {name: 'connected' as const} as ConnectionState,
          subscribe,
        },
      },
      query: {},
      mutate: {},
      mutateBatch: vi.fn(),
      userID: 'user-1',
      context: undefined,
      preload: vi.fn(),
      run: vi.fn(),
      materialize: vi.fn(),
    },
    unsubscribe,
    subscribe,
  };
}

const {ZeroMock} = vi.hoisted(() => ({
  ZeroMock: vi.fn(),
}));

vi.mock('./zero.ts', async importOriginal => {
  const orig = await importOriginal<typeof ZeroClientModule>();
  return {
    ...orig,
    Zero: ZeroMock,
  };
});

import {Z} from './zero.svelte.ts';

beforeEach(() => {
  ZeroMock.mockReset();
});

describe('Z class', () => {
  test('constructs Zero and exposes clientID', () => {
    const mock = createMockZeroInstance('z-client');
    ZeroMock.mockImplementation(function () {
      return mock.instance;
    });

    const z = new Z({schema: {} as Schema} as ZeroOptions<Schema>);

    expect(ZeroMock).toHaveBeenCalledTimes(1);
    expect(z.clientID).toBe('z-client');
  });

  test('exposes userID', () => {
    const mock = createMockZeroInstance();
    ZeroMock.mockImplementation(function () {
      return mock.instance;
    });

    const z = new Z({schema: {} as Schema} as ZeroOptions<Schema>);
    expect(z.userID).toBe('user-1');
  });

  test('exposes connection', () => {
    const mock = createMockZeroInstance();
    ZeroMock.mockImplementation(function () {
      return mock.instance;
    });

    const z = new Z({schema: {} as Schema} as ZeroOptions<Schema>);
    expect(z.connection).toBe(mock.instance.connection);
  });

  test('connectionState subscribes and reflects initial state', () => {
    const mock = createMockZeroInstance();
    ZeroMock.mockImplementation(function () {
      return mock.instance;
    });

    const z = new Z({schema: {} as Schema} as ZeroOptions<Schema>);

    expect(z.connectionState).toEqual({name: 'connected'});
    expect(mock.subscribe).toHaveBeenCalledTimes(1);
  });

  test('connectionState updates when subscription fires', () => {
    const mock = createMockZeroInstance();
    ZeroMock.mockImplementation(function () {
      return mock.instance;
    });

    const z = new Z({schema: {} as Schema} as ZeroOptions<Schema>);

    const subscribeCb = mock.subscribe.mock.calls[0][0];
    subscribeCb({name: 'disconnected', reason: 'network'});

    expect(z.connectionState).toEqual({
      name: 'disconnected',
      reason: 'network',
    });
  });

  test('online reflects connected state', () => {
    const mock = createMockZeroInstance();
    ZeroMock.mockImplementation(function () {
      return mock.instance;
    });

    const z = new Z({schema: {} as Schema} as ZeroOptions<Schema>);
    expect(z.online).toBe(true);

    const subscribeCb = mock.subscribe.mock.calls[0][0];
    subscribeCb({name: 'disconnected', reason: 'test'});
    expect(z.online).toBe(false);
  });

  test('close calls Zero.close and unsubscribes', () => {
    const mock = createMockZeroInstance();
    ZeroMock.mockImplementation(function () {
      return mock.instance;
    });

    const z = new Z({schema: {} as Schema} as ZeroOptions<Schema>);
    z.close();

    expect(mock.unsubscribe).toHaveBeenCalledTimes(1);
    expect(mock.instance.close).toHaveBeenCalledTimes(1);
  });

  test('build closes old Zero and creates new one', () => {
    const mock1 = createMockZeroInstance('client-1');
    const mock2 = createMockZeroInstance('client-2');

    ZeroMock.mockImplementationOnce(function () {
      return mock1.instance;
    }).mockImplementationOnce(function () {
      return mock2.instance;
    });

    const opts = {schema: {} as Schema} as ZeroOptions<Schema>;
    const z = new Z(opts);

    expect(z.clientID).toBe('client-1');

    z.build(opts);

    expect(mock1.unsubscribe).toHaveBeenCalledTimes(1);
    expect(mock1.instance.close).toHaveBeenCalledTimes(1);
    expect(z.clientID).toBe('client-2');
    expect(mock2.subscribe).toHaveBeenCalledTimes(1);
  });

  test('viewStore is accessible', () => {
    const mock = createMockZeroInstance();
    ZeroMock.mockImplementation(function () {
      return mock.instance;
    });

    const z = new Z({schema: {} as Schema} as ZeroOptions<Schema>);
    expect(z.viewStore).toBeDefined();
  });
});
