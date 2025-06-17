import {
  beforeEach,
  describe,
  expect,
  test,
  vi,
  type MockInstance,
} from 'vitest';
import {nanoid} from '../util/nanoid.ts';
import {AliveClientsManager} from './alive-clients-manager.ts';

describe('AliveClientManager with mocked locks', () => {
  let requestSpy: MockInstance<typeof navigator.locks.request>;
  let querySpy: MockInstance<typeof navigator.locks.query>;

  beforeEach(() => {
    requestSpy = vi.spyOn(navigator.locks, 'request');
    querySpy = vi.spyOn(navigator.locks, 'query');

    return () => {
      vi.restoreAllMocks();
    };
  });

  test('should call lockManager.request in the constructor', () => {
    const clientManager = new AliveClientsManager('group1', 'client1');

    expect(requestSpy).toHaveBeenCalledWith(
      'zero-alive/group1/client1',
      expect.any(Function),
    );

    clientManager.close();
  });

  test('should return alive clients from held and pending locks', async () => {
    querySpy.mockResolvedValue({
      held: [{name: 'zero-alive/group1/client1'}],
      pending: [{name: 'zero-alive/group1/client2'}],
    });

    const clientManager = new AliveClientsManager('group1', 'client1');
    const aliveClients = await clientManager.getAliveClients();

    expect(aliveClients).toEqual(new Set(['client1', 'client2']));

    clientManager.close();
  });

  test('should ignore invalid lock keys', async () => {
    querySpy.mockResolvedValue({
      held: [{name: 'invalid-lock-key'}],
      pending: [{name: 'zero-alive/group1/client3'}],
    });

    const clientManager = new AliveClientsManager('group1', 'client1');
    const aliveClients = await clientManager.getAliveClients();

    expect(aliveClients).toEqual(new Set(['client1', 'client3']));

    clientManager.close();
  });

  test('should abort the lock when close is called', () => {
    const clientManager = new AliveClientsManager('group1', 'client1');

    expect(clientManager.closed).toBe(false);

    clientManager.close();

    expect(clientManager.closed).toBe(true);
  });

  test('should handle multiple calls to close gracefully', () => {
    const clientManager = new AliveClientsManager('group1', 'client1');

    clientManager.close();
    expect(clientManager.closed).toBe(true);

    clientManager.close();
    expect(clientManager.closed).toBe(true);
  });
});

describe('AliveClientManager without navigator', () => {
  beforeEach(() => {
    vi.stubGlobal('navigator', undefined);
    return () => {
      vi.restoreAllMocks();
    };
  });

  test('should return set with self if navigator is undefined', async () => {
    vi.stubGlobal('navigator', undefined);

    const clientManager = new AliveClientsManager('group1', 'client1');
    const aliveClients = await clientManager.getAliveClients();

    expect(aliveClients).toEqual(new Set(['client1']));

    clientManager.close();
  });
});

describe('AliveClientManager with undefined locks', () => {
  beforeEach(() => {
    vi.stubGlobal('navigator', {locks: undefined});
    return () => {
      vi.restoreAllMocks();
    };
  });

  test('should return set with self if navigator.locks is undefined', async () => {
    vi.stubGlobal('navigator', {locks: undefined});

    const clientManager = new AliveClientsManager('group1', 'client1');
    const aliveClients = await clientManager.getAliveClients();

    expect(aliveClients).toEqual(new Set(['client1']));

    clientManager.close();
  });
});

describe('AliveClientManager with real lock', () => {
  // Use nanoid for client groups so that tests do not interfere with each other.

  test('One manager', async () => {
    const clientGroupID = nanoid();
    const clientManager = new AliveClientsManager(clientGroupID, 'client1');
    const aliveClients = await clientManager.getAliveClients();

    expect(aliveClients).toEqual(new Set(['client1']));

    clientManager.close();
  });

  test('Two managers in the same group', async () => {
    const clientGroupID = nanoid();

    const clientManager1 = new AliveClientsManager(clientGroupID, 'client1');
    const clientManager2 = new AliveClientsManager(clientGroupID, 'client2');

    const aliveClients1 = await clientManager1.getAliveClients();
    const aliveClients2 = await clientManager2.getAliveClients();

    expect(aliveClients1).toEqual(new Set(['client1', 'client2']));
    expect(aliveClients2).toEqual(new Set(['client1', 'client2']));

    clientManager1.close();
    clientManager2.close();
  });

  test('4 managers in 2 different groups', async () => {
    const clientGroupID1 = nanoid();
    const clientGroupID2 = nanoid();

    const clientManager1 = new AliveClientsManager(clientGroupID1, 'client1');
    const clientManager2 = new AliveClientsManager(clientGroupID1, 'client2');
    const clientManager3 = new AliveClientsManager(clientGroupID2, 'client3');
    const clientManager4 = new AliveClientsManager(clientGroupID2, 'client4');

    const aliveClients1 = await clientManager1.getAliveClients();
    const aliveClients2 = await clientManager2.getAliveClients();
    const aliveClients3 = await clientManager3.getAliveClients();
    const aliveClients4 = await clientManager4.getAliveClients();
    expect(aliveClients1).toEqual(new Set(['client1', 'client2']));
    expect(aliveClients2).toEqual(new Set(['client1', 'client2']));
    expect(aliveClients3).toEqual(new Set(['client3', 'client4']));
    expect(aliveClients4).toEqual(new Set(['client3', 'client4']));

    clientManager1.close();
    clientManager2.close();
  });

  test('2 clients in 2 different groups', async () => {
    const clientGroupID1 = nanoid();
    const clientGroupID2 = nanoid();

    const clientManager1 = new AliveClientsManager(clientGroupID1, 'client1');
    const clientManager2 = new AliveClientsManager(clientGroupID2, 'client2');

    const aliveClients1 = await clientManager1.getAliveClients();
    const aliveClients2 = await clientManager2.getAliveClients();

    expect(aliveClients1).toEqual(new Set(['client1']));
    expect(aliveClients2).toEqual(new Set(['client2']));

    clientManager1.close();
    clientManager2.close();
  });

  test('3 clients in 1 group. Close one and check others', async () => {
    const clientGroupID = nanoid();

    const clientManager1 = new AliveClientsManager(clientGroupID, 'client1');
    const clientManager2 = new AliveClientsManager(clientGroupID, 'client2');
    const clientManager3 = new AliveClientsManager(clientGroupID, 'client3');

    const aliveClients1 = await clientManager1.getAliveClients();
    const aliveClients2 = await clientManager2.getAliveClients();
    const aliveClients3 = await clientManager3.getAliveClients();

    expect(aliveClients1).toEqual(new Set(['client1', 'client2', 'client3']));
    expect(aliveClients2).toEqual(new Set(['client1', 'client2', 'client3']));
    expect(aliveClients3).toEqual(new Set(['client1', 'client2', 'client3']));

    clientManager1.close();
    await 0;

    const aliveClientsAfterClose1 = await clientManager2.getAliveClients();
    expect(aliveClientsAfterClose1).toEqual(new Set(['client2', 'client3']));

    const aliveClientsAfterClose2 = await clientManager3.getAliveClients();
    expect(aliveClientsAfterClose2).toEqual(new Set(['client2', 'client3']));

    clientManager2.close();
    clientManager3.close();
  });
});
