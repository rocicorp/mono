import {
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
  vi,
  type MockInstance,
} from 'vitest';
import {Queue} from '../../../shared/src/queue.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import {nanoid} from '../util/nanoid.ts';
import {ActiveClientsManager} from './active-clients-manager.ts';

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe('ActiveClientManager with mocked locks', () => {
  let requestSpy: MockInstance<typeof navigator.locks.request>;
  let querySpy: MockInstance<typeof navigator.locks.query>;

  beforeEach(() => {
    requestSpy = vi.spyOn(navigator.locks, 'request');
    querySpy = vi.spyOn(navigator.locks, 'query');
  });

  test('should call lockManager.request in the constructor', async () => {
    const ac = new AbortController();
    await ActiveClientsManager.create('group1', 'client1', ac.signal);

    expect(requestSpy).toHaveBeenCalledWith(
      'zero-active/group1/client1',
      {mode: 'exclusive', signal: ac.signal},
      expect.any(Function),
    );
    ac.abort();
  });

  test('should return active clients from held and pending locks', async () => {
    const ac = new AbortController();

    querySpy.mockResolvedValue({
      held: [{name: 'zero-active/group1/client1', mode: 'exclusive'}],
      pending: [{name: 'zero-active/group1/client2', mode: 'exclusive'}],
    });

    const clientManager = await ActiveClientsManager.create(
      'group1',
      'client1',
      ac.signal,
    );
    const activeClients = await clientManager.getActiveClients();

    expect(activeClients).toEqual(new Set(['client1', 'client2']));
    ac.abort();
  });

  test('should ignore invalid lock keys', async () => {
    const ac = new AbortController();

    const clientManager = await ActiveClientsManager.create(
      'group1',
      'client1',
      ac.signal,
    );

    querySpy.mockResolvedValue({
      held: [{name: 'invalid-lock-key', mode: 'exclusive'}],
      pending: [
        {name: 'zero-active/group1/client1', mode: 'exclusive'},
        {name: 'zero-active/group1/client3', mode: 'exclusive'},
      ],
    });

    const activeClients = await clientManager.getActiveClients();

    expect(activeClients).toEqual(new Set(['client1', 'client3']));
    ac.abort();
  });
});

describe('ActiveClientManager without navigator', () => {
  beforeEach(() => {
    vi.stubGlobal('navigator', undefined);
  });

  test('should return set with self if navigator is undefined', async () => {
    const ac = new AbortController();
    const clientManager = await ActiveClientsManager.create(
      'group1',
      'client1',
      ac.signal,
    );
    const activeClients = await clientManager.getActiveClients();

    expect(activeClients).toEqual(new Set(['client1']));
    ac.abort();
  });

  test('multiple clients are managed in memory', async () => {
    const ac1 = new AbortController();
    const ac2 = new AbortController();

    const clientManager1 = await ActiveClientsManager.create(
      'group1',
      'client1',
      ac1.signal,
    );
    const clientManager2 = await ActiveClientsManager.create(
      'group1',
      'client2',
      ac2.signal,
    );

    expect(await clientManager1.getActiveClients()).toEqual(
      new Set(['client1', 'client2']),
    );
    expect(await clientManager2.getActiveClients()).toEqual(
      new Set(['client1', 'client2']),
    );

    ac1.abort();

    expect(await clientManager2.getActiveClients()).toEqual(
      new Set(['client2']),
    );

    ac2.abort();
  });
});

describe('ActiveClientManager with undefined locks', () => {
  let signal: AbortSignal;
  beforeEach(() => {
    const ac = new AbortController();
    signal = ac.signal;
    vi.stubGlobal('navigator', {locks: undefined});
    return () => {
      ac.abort();
    };
  });

  test('should return set with self if navigator.locks is undefined', async () => {
    vi.stubGlobal('navigator', {locks: undefined});

    const clientManager = await ActiveClientsManager.create(
      'group1',
      'client1',
      signal,
    );
    const activeClients = await clientManager.getActiveClients();

    expect(activeClients).toEqual(new Set(['client1']));
  });
});

describe('ActiveClientManager', () => {
  const cases = [
    ['real locks', () => expect(navigator.locks).toBeDefined()],
    ['no locks', () => vi.stubGlobal('navigator', {locks: undefined})],
  ] as const;

  describe('onChange', () => {
    test.each(cases)('%s', async (_desc, setup) => {
      setup();
      // This sets up three clients in the same group and listens for changes.
      const clientGroupID = nanoid();

      const ac1 = new AbortController();
      const ac2 = new AbortController();
      const ac3 = new AbortController();

      const changes = new Queue<{
        clientID: string;
        activeClients: ReadonlySet<string>;
      }>();

      const clientManager1 = await ActiveClientsManager.create(
        clientGroupID,
        'client1',
        ac1.signal,
      );
      clientManager1.onChange = activeClients => {
        changes.enqueue({clientID: 'client1', activeClients});
      };

      const clientManager2 = await ActiveClientsManager.create(
        clientGroupID,
        'client2',
        ac2.signal,
      );
      clientManager2.onChange = activeClients => {
        changes.enqueue({clientID: 'client2', activeClients});
      };

      async function expectDequeue<T>(queue: Queue<T>, expected: Array<T>) {
        // Use set to allow arbitrary order of changes
        const actual: Set<T> = new Set();
        for (const _ of expected) {
          actual.add(await queue.dequeue());
        }
        expect(queue.size(), `Queue was not drained`).toBe(0);
        const expectedSet = new Set(expected);
        expect(actual).toEqual(expectedSet);
      }

      await expectDequeue(changes, [
        {
          clientID: 'client1',
          activeClients: new Set(['client1', 'client2']),
        },
      ]);

      const clientManager3 = await ActiveClientsManager.create(
        clientGroupID,
        'client3',
        ac3.signal,
      );
      clientManager3.onChange = activeClients => {
        changes.enqueue({clientID: 'client3', activeClients});
      };

      await expectDequeue(changes, [
        {
          clientID: 'client1',
          activeClients: new Set(['client1', 'client2', 'client3']),
        },
        {
          clientID: 'client2',
          activeClients: new Set(['client1', 'client2', 'client3']),
        },
      ]);

      // Now abort one client and check for changes
      ac1.abort();

      await expectDequeue(changes, [
        {
          clientID: 'client2',
          activeClients: new Set(['client2', 'client3']),
        },
        {
          clientID: 'client3',
          activeClients: new Set(['client2', 'client3']),
        },
      ]);

      ac2.abort();

      await expectDequeue(changes, [
        {
          clientID: 'client3',
          activeClients: new Set(['client3']),
        },
      ]);

      ac3.abort();

      await expectDequeue(changes, []);
    });
  });

  describe('One manager', () => {
    test.each(cases)('%s', async (_desc, setup) => {
      setup();
      const ac = new AbortController();
      const clientGroupID = nanoid();
      const clientManager = await ActiveClientsManager.create(
        clientGroupID,
        'client1',
        ac.signal,
      );
      const activeClients = await clientManager.getActiveClients();

      expect(activeClients).toEqual(new Set(['client1']));

      ac.abort();
    });
  });

  describe('Two managers in the same group', () => {
    test.each(cases)('%s', async (_desc, setup) => {
      setup();
      const clientGroupID = nanoid();
      const ac1 = new AbortController();
      const ac2 = new AbortController();

      const clientManager1 = await ActiveClientsManager.create(
        clientGroupID,
        'client1',
        ac1.signal,
      );
      const clientManager2 = await ActiveClientsManager.create(
        clientGroupID,
        'client2',
        ac2.signal,
      );

      const activeClients1 = await clientManager1.getActiveClients();
      const activeClients2 = await clientManager2.getActiveClients();

      expect(activeClients1).toEqual(new Set(['client1', 'client2']));
      expect(activeClients2).toEqual(new Set(['client1', 'client2']));

      ac1.abort();
      ac2.abort();
    });
  });

  describe('4 managers in 2 different groups', () => {
    test.each(cases)('%s', async (_desc, setup) => {
      setup();
      const clientGroupID1 = nanoid();
      const clientGroupID2 = nanoid();
      const ac1 = new AbortController();
      const ac2 = new AbortController();
      const ac3 = new AbortController();
      const ac4 = new AbortController();

      const clientManager1 = await ActiveClientsManager.create(
        clientGroupID1,
        'client1',
        ac1.signal,
      );
      const clientManager2 = await ActiveClientsManager.create(
        clientGroupID1,
        'client2',
        ac1.signal,
      );
      const clientManager3 = await ActiveClientsManager.create(
        clientGroupID2,
        'client3',
        ac3.signal,
      );
      const clientManager4 = await ActiveClientsManager.create(
        clientGroupID2,
        'client4',
        ac4.signal,
      );

      const activeClients1 = await clientManager1.getActiveClients();
      const activeClients2 = await clientManager2.getActiveClients();
      const activeClients3 = await clientManager3.getActiveClients();
      const activeClients4 = await clientManager4.getActiveClients();
      expect(activeClients1).toEqual(new Set(['client1', 'client2']));
      expect(activeClients2).toEqual(new Set(['client1', 'client2']));
      expect(activeClients3).toEqual(new Set(['client3', 'client4']));
      expect(activeClients4).toEqual(new Set(['client3', 'client4']));

      ac1.abort();
      ac2.abort();
      ac3.abort();
      ac4.abort();
    });
  });

  describe('2 clients in 2 different groups', () => {
    test.each(cases)('%s', async (_desc, setup) => {
      setup();
      const clientGroupID1 = nanoid();
      const clientGroupID2 = nanoid();
      const ac1 = new AbortController();
      const ac2 = new AbortController();

      const clientManager1 = await ActiveClientsManager.create(
        clientGroupID1,
        'client1',
        ac1.signal,
      );
      const clientManager2 = await ActiveClientsManager.create(
        clientGroupID2,
        'client2',
        ac2.signal,
      );

      const activeClients1 = await clientManager1.getActiveClients();
      const activeClients2 = await clientManager2.getActiveClients();

      expect(activeClients1).toEqual(new Set(['client1']));
      expect(activeClients2).toEqual(new Set(['client2']));

      ac1.abort();
      ac2.abort();
    });
  });

  describe('3 clients in 1 group. Close one and check others', () => {
    test.each(cases)('%s', async (_desc, setup) => {
      setup();
      const clientGroupID = nanoid();
      const ac1 = new AbortController();
      const ac2 = new AbortController();
      const ac3 = new AbortController();

      const clientManager1 = await ActiveClientsManager.create(
        clientGroupID,
        'client1',
        ac1.signal,
      );
      const clientManager2 = await ActiveClientsManager.create(
        clientGroupID,
        'client2',
        ac2.signal,
      );
      const clientManager3 = await ActiveClientsManager.create(
        clientGroupID,
        'client3',
        ac3.signal,
      );

      const activeClients1 = await clientManager1.getActiveClients();
      const activeClients2 = await clientManager2.getActiveClients();
      const activeClients3 = await clientManager3.getActiveClients();

      expect(activeClients1).toEqual(
        new Set(['client1', 'client2', 'client3']),
      );
      expect(activeClients2).toEqual(
        new Set(['client1', 'client2', 'client3']),
      );
      expect(activeClients3).toEqual(
        new Set(['client1', 'client2', 'client3']),
      );

      ac1.abort();
      await sleep(5); // Give some time for the change to propagate

      const activeClientsAfterClose1 = await clientManager2.getActiveClients();
      expect(activeClientsAfterClose1).toEqual(new Set(['client2', 'client3']));

      const activeClientsAfterClose2 = await clientManager3.getActiveClients();
      expect(activeClientsAfterClose2).toEqual(new Set(['client2', 'client3']));

      ac2.abort();
      ac3.abort();
    });
  });
});
