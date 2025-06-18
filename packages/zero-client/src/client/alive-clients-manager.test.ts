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
    const ac = new AbortController();
    new AliveClientsManager('group1', 'client1', ac.signal);

    expect(requestSpy).toHaveBeenCalledWith(
      'zero-alive/group1/client1',
      {signal: ac.signal},
      expect.any(Function),
    );
    ac.abort();
  });

  test('should return alive clients from held and pending locks', async () => {
    const ac = new AbortController();

    querySpy.mockResolvedValue({
      held: [{name: 'zero-alive/group1/client1'}],
      pending: [{name: 'zero-alive/group1/client2'}],
    });

    const clientManager = new AliveClientsManager(
      'group1',
      'client1',
      ac.signal,
    );
    const aliveClients = await clientManager.getAliveClients();

    expect(aliveClients).toEqual(new Set(['client1', 'client2']));
    ac.abort();
  });

  test('should ignore invalid lock keys', async () => {
    const ac = new AbortController();

    const clientManager = new AliveClientsManager(
      'group1',
      'client1',
      ac.signal,
    );

    querySpy.mockResolvedValue({
      held: [{name: 'invalid-lock-key'}],
      pending: [
        {name: 'zero-alive/group1/client1'},
        {name: 'zero-alive/group1/client3'},
      ],
    });

    const aliveClients = await clientManager.getAliveClients();

    expect(aliveClients).toEqual(new Set(['client1', 'client3']));
    ac.abort();
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
    const ac = new AbortController();
    const clientManager = new AliveClientsManager(
      'group1',
      'client1',
      ac.signal,
    );
    const aliveClients = await clientManager.getAliveClients();

    expect(aliveClients).toEqual(new Set(['client1']));
    ac.abort();
  });

  test('multiple clients are managed in memory', async () => {
    const ac1 = new AbortController();
    const ac2 = new AbortController();

    const clientManager1 = new AliveClientsManager(
      'group1',
      'client1',
      ac1.signal,
    );
    const clientManager2 = new AliveClientsManager(
      'group1',
      'client2',
      ac2.signal,
    );

    expect(await clientManager1.getAliveClients()).toEqual(
      new Set(['client1', 'client2']),
    );
    expect(await clientManager2.getAliveClients()).toEqual(
      new Set(['client1', 'client2']),
    );

    ac1.abort();

    expect(await clientManager2.getAliveClients()).toEqual(
      new Set(['client2']),
    );

    ac2.abort();
  });
});

describe('AliveClientManager with undefined locks', () => {
  let signal: AbortSignal;
  beforeEach(() => {
    const ac = new AbortController();
    signal = ac.signal;
    vi.stubGlobal('navigator', {locks: undefined});
    return () => {
      ac.abort();
      vi.restoreAllMocks();
    };
  });

  test('should return set with self if navigator.locks is undefined', async () => {
    vi.stubGlobal('navigator', {locks: undefined});

    const clientManager = new AliveClientsManager('group1', 'client1', signal);
    const aliveClients = await clientManager.getAliveClients();

    expect(aliveClients).toEqual(new Set(['client1']));
  });
});

describe('AliveClientManager with real lock', () => {
  // Use nanoid for client groups so that tests do not interfere with each other.

  test('One manager', async () => {
    const ac = new AbortController();
    const clientGroupID = nanoid();
    const clientManager = new AliveClientsManager(
      clientGroupID,
      'client1',
      ac.signal,
    );
    const aliveClients = await clientManager.getAliveClients();

    expect(aliveClients).toEqual(new Set(['client1']));

    ac.abort();
  });

  test('Two managers in the same group', async () => {
    const clientGroupID = nanoid();
    const ac1 = new AbortController();
    const ac2 = new AbortController();

    const clientManager1 = new AliveClientsManager(
      clientGroupID,
      'client1',
      ac1.signal,
    );
    const clientManager2 = new AliveClientsManager(
      clientGroupID,
      'client2',
      ac2.signal,
    );

    const aliveClients1 = await clientManager1.getAliveClients();
    const aliveClients2 = await clientManager2.getAliveClients();

    expect(aliveClients1).toEqual(new Set(['client1', 'client2']));
    expect(aliveClients2).toEqual(new Set(['client1', 'client2']));

    ac1.abort();
    ac2.abort();
  });

  test('4 managers in 2 different groups', async () => {
    const clientGroupID1 = nanoid();
    const clientGroupID2 = nanoid();
    const ac1 = new AbortController();
    const ac2 = new AbortController();
    const ac3 = new AbortController();
    const ac4 = new AbortController();

    const clientManager1 = new AliveClientsManager(
      clientGroupID1,
      'client1',
      ac1.signal,
    );
    const clientManager2 = new AliveClientsManager(
      clientGroupID1,
      'client2',
      ac1.signal,
    );
    const clientManager3 = new AliveClientsManager(
      clientGroupID2,
      'client3',
      ac3.signal,
    );
    const clientManager4 = new AliveClientsManager(
      clientGroupID2,
      'client4',
      ac4.signal,
    );

    const aliveClients1 = await clientManager1.getAliveClients();
    const aliveClients2 = await clientManager2.getAliveClients();
    const aliveClients3 = await clientManager3.getAliveClients();
    const aliveClients4 = await clientManager4.getAliveClients();
    expect(aliveClients1).toEqual(new Set(['client1', 'client2']));
    expect(aliveClients2).toEqual(new Set(['client1', 'client2']));
    expect(aliveClients3).toEqual(new Set(['client3', 'client4']));
    expect(aliveClients4).toEqual(new Set(['client3', 'client4']));

    ac1.abort();
    ac2.abort();
    ac3.abort();
    ac4.abort();
  });

  test('2 clients in 2 different groups', async () => {
    const clientGroupID1 = nanoid();
    const clientGroupID2 = nanoid();
    const ac1 = new AbortController();
    const ac2 = new AbortController();

    const clientManager1 = new AliveClientsManager(
      clientGroupID1,
      'client1',
      ac1.signal,
    );
    const clientManager2 = new AliveClientsManager(
      clientGroupID2,
      'client2',
      ac2.signal,
    );

    const aliveClients1 = await clientManager1.getAliveClients();
    const aliveClients2 = await clientManager2.getAliveClients();

    expect(aliveClients1).toEqual(new Set(['client1']));
    expect(aliveClients2).toEqual(new Set(['client2']));

    ac1.abort();
    ac2.abort();
  });

  test('3 clients in 1 group. Close one and check others', async () => {
    const clientGroupID = nanoid();
    const ac1 = new AbortController();
    const ac2 = new AbortController();
    const ac3 = new AbortController();

    const clientManager1 = new AliveClientsManager(
      clientGroupID,
      'client1',
      ac1.signal,
    );
    const clientManager2 = new AliveClientsManager(
      clientGroupID,
      'client2',
      ac2.signal,
    );
    const clientManager3 = new AliveClientsManager(
      clientGroupID,
      'client3',
      ac3.signal,
    );

    const aliveClients1 = await clientManager1.getAliveClients();
    const aliveClients2 = await clientManager2.getAliveClients();
    const aliveClients3 = await clientManager3.getAliveClients();

    expect(aliveClients1).toEqual(new Set(['client1', 'client2', 'client3']));
    expect(aliveClients2).toEqual(new Set(['client1', 'client2', 'client3']));
    expect(aliveClients3).toEqual(new Set(['client1', 'client2', 'client3']));

    ac1.abort();

    const aliveClientsAfterClose1 = await clientManager2.getAliveClients();
    expect(aliveClientsAfterClose1).toEqual(new Set(['client2', 'client3']));

    const aliveClientsAfterClose2 = await clientManager3.getAliveClients();
    expect(aliveClientsAfterClose2).toEqual(new Set(['client2', 'client3']));

    ac2.abort();
    ac3.abort();
  });
});
