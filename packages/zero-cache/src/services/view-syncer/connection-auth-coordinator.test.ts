import {describe, expect, test} from 'vitest';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import {ProtocolErrorWithLevel} from '../../types/error-with-level.ts';
import {ConnectionAuthCoordinator} from './connection-auth-coordinator.ts';

function register(
  coordinator: ConnectionAuthCoordinator,
  clientID: string,
  wsID: string,
) {
  return coordinator.registerConnection(
    clientID,
    wsID,
    `user-${clientID}`,
    {type: 'opaque', raw: `token-${wsID}`},
    `cookie-${wsID}`,
    `origin-${wsID}`,
    undefined,
    undefined,
  );
}

function expectProtocolErrorKind(fn: () => unknown, kind: ErrorKind) {
  const error = getThrownError(fn);
  expect(error).toBeInstanceOf(ProtocolErrorWithLevel);
  expect((error as ProtocolErrorWithLevel).errorBody.kind).toBe(kind);
}

function getThrownError(fn: () => unknown): unknown {
  try {
    fn();
  } catch (error) {
    return error;
  }
  throw new Error('expected protocol error');
}

describe('ConnectionAuthCoordinator', () => {
  test('registers provisional connections and applies init metadata', () => {
    const coordinator = new ConnectionAuthCoordinator();

    expect(register(coordinator, 'c1', 'ws1')).toMatchObject({
      clientID: 'c1',
      wsID: 'ws1',
      state: 'provisional',
      userQueryURL: undefined,
    });

    expect(
      coordinator.applyInitConnection(
        'c1',
        'ws1',
        'cookie-2',
        'origin-2',
        'https://api.example/query',
        {foo: 'bar'},
      ),
    ).toEqual({
      connection: expect.objectContaining({
        httpCookie: 'cookie-2',
        origin: 'origin-2',
        userQueryURL: 'https://api.example/query',
        userQueryHeaders: {foo: 'bar'},
      }),
    });
  });

  test('binds and validates the first principal', () => {
    const coordinator = new ConnectionAuthCoordinator();
    register(coordinator, 'c1', 'ws1');

    expect(
      coordinator.validateConnection('c1', 'ws1', 'principal-1', 'query', 10),
    ).toEqual({
      connection: expect.objectContaining({
        state: 'validated',
        principalID: 'principal-1',
        principalSource: 'query',
        revalidateAt: 10,
      }),
      group: {
        principalID: 'principal-1',
        principalSource: 'query',
        selectedConnection: {clientID: 'c1', wsID: 'ws1'},
        nextRetransformAt: undefined,
      },
    });
  });

  test('rejects mismatched principal binding without mutating the connection', () => {
    const coordinator = new ConnectionAuthCoordinator();
    register(coordinator, 'c1', 'ws1');
    register(coordinator, 'c2', 'ws2');
    coordinator.validateConnection('c1', 'ws1', 'principal-1', 'query');

    expectProtocolErrorKind(
      () => coordinator.validateConnection('c2', 'ws2', 'principal-2', 'query'),
      ErrorKind.Unauthorized,
    );
    expect(coordinator.getConnection('c2')).toMatchObject({
      state: 'provisional',
      principalID: undefined,
    });
  });

  test('upgrades principal source from compatibility fallback to query metadata', () => {
    const coordinator = new ConnectionAuthCoordinator();
    register(coordinator, 'c1', 'ws1');

    coordinator.validateConnection('c1', 'ws1', 'principal-1', 'userID');
    expect(coordinator.getGroupState().principalSource).toBe('userID');

    coordinator.validateConnection('c1', 'ws1', 'principal-1', 'query');
    expect(coordinator.getGroupState().principalSource).toBe('query');
  });

  test('keeps selection sticky and promotes the newest validated connection', () => {
    const coordinator = new ConnectionAuthCoordinator();
    register(coordinator, 'c1', 'ws1');
    register(coordinator, 'c2', 'ws2');
    register(coordinator, 'c3', 'ws3');

    coordinator.validateConnection('c1', 'ws1', 'principal-1', 'query');
    coordinator.validateConnection('c2', 'ws2', 'principal-1', 'query');
    coordinator.validateConnection('c3', 'ws3', 'principal-1', 'query');

    expect(coordinator.getSelectedConnection()).toMatchObject({
      clientID: 'c1',
      wsID: 'ws1',
    });

    expect(coordinator.closeConnection({clientID: 'c1', wsID: 'ws1'})).toEqual({
      status: 'closed',
      connection: expect.objectContaining({clientID: 'c1', wsID: 'ws1'}),
    });
    expect(coordinator.getSelectedConnection()).toMatchObject({
      clientID: 'c3',
      wsID: 'ws3',
    });
  });

  test('demotes validated connections when auth changes', () => {
    const coordinator = new ConnectionAuthCoordinator();
    register(coordinator, 'c1', 'ws1');
    coordinator.validateConnection('c1', 'ws1', 'principal-1', 'query', 15);

    expect(
      coordinator.updateConnectionAuth('c1', 'ws1', {
        type: 'opaque',
        raw: 'token-ws1-new',
      }),
    ).toEqual({
      connection: expect.objectContaining({
        state: 'provisional',
        principalID: undefined,
        revalidateAt: undefined,
      }),
    });
    expect(coordinator.getSelectedConnection()).toBeUndefined();
  });

  test('plans revalidation and retransform deadlines', () => {
    let now = 12_000;
    const coordinator = new ConnectionAuthCoordinator(undefined, 3, () => now);
    register(coordinator, 'c1', 'ws1');
    register(coordinator, 'c2', 'ws2');
    coordinator.validateConnection('c1', 'ws1', 'principal-1', 'query', 20_000);
    coordinator.validateConnection('c2', 'ws2', 'principal-1', 'query', 10_000);

    expect(coordinator.planMaintenance()).toEqual({
      dueRevalidations: [
        expect.objectContaining({clientID: 'c2', wsID: 'ws2'}),
      ],
      dueRetransform: false,
      nextWakeAt: 10_000,
    });
    now = 16_000;
    expect(coordinator.planMaintenance()).toEqual({
      dueRevalidations: [
        expect.objectContaining({clientID: 'c2', wsID: 'ws2'}),
      ],
      dueRetransform: true,
      nextWakeAt: 10_000,
    });
  });

  test('defaults revalidateAt from coordinator policy', () => {
    let now = 1_000;
    const coordinator = new ConnectionAuthCoordinator(5, undefined, () => now);
    register(coordinator, 'c1', 'ws1');

    expect(
      coordinator.validateConnection('c1', 'ws1', 'principal-1', 'query'),
    ).toEqual({
      connection: expect.objectContaining({revalidateAt: 6_000}),
      group: expect.any(Object),
    });
  });

  test('syncs and resets background retransform deadlines', () => {
    let now = 1_000;
    const coordinator = new ConnectionAuthCoordinator(undefined, 5, () => now);
    register(coordinator, 'c1', 'ws1');

    expect(coordinator.getGroupState().nextRetransformAt).toBeUndefined();

    coordinator.validateConnection('c1', 'ws1', 'principal-1', 'query');
    expect(coordinator.getGroupState().nextRetransformAt).toBe(6_000);

    now = 2_000;
    coordinator.validateConnection('c1', 'ws1', 'principal-1', 'query');
    expect(coordinator.getGroupState().nextRetransformAt).toBe(6_000);

    now = 3_000;
    coordinator.markBackgroundRetransformSuccess();
    expect(coordinator.getGroupState().nextRetransformAt).toBe(8_000);
  });

  test('clears background retransform deadline when no selected connection remains', () => {
    let now = 1_000;
    const coordinator = new ConnectionAuthCoordinator(undefined, 5, () => now);
    register(coordinator, 'c1', 'ws1');
    coordinator.validateConnection('c1', 'ws1', 'principal-1', 'query');
    expect(coordinator.getGroupState().nextRetransformAt).toBe(6_000);

    coordinator.closeConnection({clientID: 'c1', wsID: 'ws1'});

    expect(coordinator.getGroupState().nextRetransformAt).toBeUndefined();
  });

  test('failing the selected connection promotes a replacement without resetting cadence', () => {
    let now = 1_000;
    const coordinator = new ConnectionAuthCoordinator(undefined, 5, () => now);
    register(coordinator, 'c1', 'ws1');
    register(coordinator, 'c2', 'ws2');
    coordinator.validateConnection('c1', 'ws1', 'principal-1', 'query');
    coordinator.validateConnection('c2', 'ws2', 'principal-1', 'query');

    expect(coordinator.getSelectedConnection()).toMatchObject({
      clientID: 'c1',
      wsID: 'ws1',
    });
    expect(coordinator.getGroupState().nextRetransformAt).toBe(6_000);

    coordinator.failConnection({clientID: 'c1', wsID: 'ws1'});

    expect(coordinator.getSelectedConnection()).toMatchObject({
      clientID: 'c2',
      wsID: 'ws2',
    });
    expect(coordinator.getGroupState().nextRetransformAt).toBe(6_000);
  });

  test('failing the last validated connection clears background deadline', () => {
    const coordinator = new ConnectionAuthCoordinator(
      undefined,
      5,
      () => 1_000,
    );
    register(coordinator, 'c1', 'ws1');
    coordinator.validateConnection('c1', 'ws1', 'principal-1', 'query');

    expect(coordinator.getGroupState().nextRetransformAt).toBe(6_000);

    coordinator.failConnection({clientID: 'c1', wsID: 'ws1'});

    expect(coordinator.getSelectedConnection()).toBeUndefined();
    expect(coordinator.getGroupState().nextRetransformAt).toBeUndefined();
  });

  test('replacing a selected validated connection unschedules background work until revalidated', () => {
    const coordinator = new ConnectionAuthCoordinator(
      undefined,
      5,
      () => 1_000,
    );
    register(coordinator, 'c1', 'ws1');
    coordinator.validateConnection('c1', 'ws1', 'principal-1', 'query');

    expect(coordinator.getSelectedConnection()).toMatchObject({
      clientID: 'c1',
      wsID: 'ws1',
    });
    expect(coordinator.getGroupState().nextRetransformAt).toBe(6_000);

    register(coordinator, 'c1', 'ws2');

    expect(coordinator.getConnection('c1')).toMatchObject({
      wsID: 'ws2',
      state: 'provisional',
    });
    expect(coordinator.getSelectedConnection()).toBeUndefined();
    expect(coordinator.getGroupState().nextRetransformAt).toBeUndefined();
  });

  test('auth demotion clears background deadline when it removes the selected connection', () => {
    const coordinator = new ConnectionAuthCoordinator(
      undefined,
      5,
      () => 1_000,
    );
    register(coordinator, 'c1', 'ws1');
    coordinator.validateConnection('c1', 'ws1', 'principal-1', 'query');

    expect(coordinator.getGroupState().nextRetransformAt).toBe(6_000);

    coordinator.updateConnectionAuth('c1', 'ws1', {
      type: 'opaque',
      raw: 'token-ws1-new',
    });

    expect(coordinator.getConnection('c1')).toMatchObject({
      state: 'provisional',
      principalID: undefined,
    });
    expect(coordinator.getSelectedConnection()).toBeUndefined();
    expect(coordinator.getGroupState().nextRetransformAt).toBeUndefined();
  });

  test('treats stale validation races as no-ops', () => {
    const coordinator = new ConnectionAuthCoordinator();
    register(coordinator, 'c1', 'ws1');

    expect(
      coordinator.validateConnection('c1', 'stale-ws', 'principal-1', 'query'),
    ).toBeUndefined();
  });

  test('throws invalid connection requests for stale mutation calls', () => {
    const coordinator = new ConnectionAuthCoordinator();
    register(coordinator, 'c1', 'ws1');

    expectProtocolErrorKind(
      () => coordinator.updateConnectionAuth('c1', 'stale-ws', undefined),
      ErrorKind.InvalidConnectionRequest,
    );
  });

  test('requires a live websocket-specific connection snapshot', () => {
    const coordinator = new ConnectionAuthCoordinator();
    register(coordinator, 'c1', 'ws1');

    expect(coordinator.requireConnection('c1', 'ws1')).toMatchObject({
      clientID: 'c1',
      wsID: 'ws1',
    });
    expectProtocolErrorKind(
      () => coordinator.requireConnection('c1', 'stale-ws'),
      ErrorKind.InvalidConnectionRequest,
    );
  });

  test('ignores stale cleanup for replaced sockets', () => {
    const coordinator = new ConnectionAuthCoordinator();
    register(coordinator, 'c1', 'ws1');
    register(coordinator, 'c1', 'ws2');

    expect(
      coordinator.closeConnection({clientID: 'c1', wsID: 'ws1'}),
    ).toBeUndefined();
    expect(coordinator.getConnection('c1')).toMatchObject({wsID: 'ws2'});
  });
});
