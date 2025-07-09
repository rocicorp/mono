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

    expect(clientManager.activeClients).toEqual(
      new Set(['client1', 'client2']),
    );

    ac.abort();
  });

  test('should ignore invalid lock keys', async () => {
    const ac = new AbortController();

    querySpy.mockResolvedValue({
      held: [{name: 'invalid-lock-key', mode: 'exclusive'}],
      pending: [
        {name: 'zero-active/group1/client1', mode: 'exclusive'},
        {name: 'zero-active/group1/client3', mode: 'exclusive'},
      ],
    });

    const clientManager = await ActiveClientsManager.create(
      'group1',
      'client1',
      ac.signal,
    );

    expect(clientManager.activeClients).toEqual(
      new Set(['client1', 'client3']),
    );
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

    expect(clientManager.activeClients).toEqual(new Set(['client1']));
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
    await waitForPostMessage();

    expect(clientManager1.activeClients).toEqual(
      new Set(['client1', 'client2']),
    );
    expect(clientManager2.activeClients).toEqual(
      new Set(['client1', 'client2']),
    );

    ac1.abort();

    expect(clientManager2.activeClients).toEqual(new Set(['client2']));

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

    expect(clientManager.activeClients).toEqual(new Set(['client1']));
  });
});

describe('ActiveClientManager', () => {
  const cases = [
    ['real locks', () => expect(navigator.locks).toBeDefined()],
    ['no locks', () => vi.stubGlobal('navigator', {locks: undefined})],
  ] as const;

  async function expectDequeue<const T>(queue: Queue<T>, expected: Array<T>) {
    // Use set to allow arbitrary order of additions
    const actual: Set<T> = new Set();
    for (const _ of expected) {
      actual.add(await queue.dequeue());
    }
    expect(queue.size(), `Queue was not drained`).toBe(0);
    const expectedSet = new Set(expected);
    expect(actual).toEqual(expectedSet);
  }

  describe('onAdd and onDelete', () => {
    test.each(cases)('%s', async (_desc, setup) => {
      setup();
      // This sets up three clients in the same group and listens for additions and deletions.
      const clientGroupID = nanoid();

      const ac1 = new AbortController();
      const ac2 = new AbortController();
      const ac3 = new AbortController();

      const events = new Queue<{
        clientID: string;
        type: 'add' | 'delete';
        affectedClientID: string;
      }>();

      const clientManager1 = await ActiveClientsManager.create(
        clientGroupID,
        'client1',
        ac1.signal,
      );
      clientManager1.onAdd = addedClientID => {
        events.enqueue({
          clientID: 'client1',
          type: 'add',
          affectedClientID: addedClientID,
        });
      };
      clientManager1.onDelete = deletedClientID => {
        events.enqueue({
          clientID: 'client1',
          type: 'delete',
          affectedClientID: deletedClientID,
        });
      };

      const clientManager2 = await ActiveClientsManager.create(
        clientGroupID,
        'client2',
        ac2.signal,
      );
      clientManager2.onAdd = addedClientID => {
        events.enqueue({
          clientID: 'client2',
          type: 'add',
          affectedClientID: addedClientID,
        });
      };
      clientManager2.onDelete = deletedClientID => {
        events.enqueue({
          clientID: 'client2',
          type: 'delete',
          affectedClientID: deletedClientID,
        });
      };

      await expectDequeue(events, [
        {
          clientID: 'client1',
          type: 'add',
          affectedClientID: 'client2',
        },
      ]);

      const clientManager3 = await ActiveClientsManager.create(
        clientGroupID,
        'client3',
        ac3.signal,
      );
      clientManager3.onAdd = addedClientID => {
        events.enqueue({
          clientID: 'client3',
          type: 'add',
          affectedClientID: addedClientID,
        });
      };
      clientManager3.onDelete = deletedClientID => {
        events.enqueue({
          clientID: 'client3',
          type: 'delete',
          affectedClientID: deletedClientID,
        });
      };

      await expectDequeue(events, [
        {
          clientID: 'client1',
          type: 'add',
          affectedClientID: 'client3',
        },
        {
          clientID: 'client2',
          type: 'add',
          affectedClientID: 'client3',
        },
      ]);

      // Now abort one client and check for deletion notifications
      ac1.abort();

      await expectDequeue(events, [
        {
          clientID: 'client2',
          type: 'delete',
          affectedClientID: 'client1',
        },
        {
          clientID: 'client3',
          type: 'delete',
          affectedClientID: 'client1',
        },
      ]);

      ac2.abort();

      await expectDequeue(events, [
        {
          clientID: 'client3',
          type: 'delete',
          affectedClientID: 'client2',
        },
      ]);

      ac3.abort();

      await expectDequeue(events, []);
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

      expect(clientManager.activeClients).toEqual(new Set(['client1']));

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
      await waitForPostMessage();

      expect(clientManager1.activeClients).toEqual(
        new Set(['client1', 'client2']),
      );
      expect(clientManager2.activeClients).toEqual(
        new Set(['client1', 'client2']),
      );

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
      await waitForPostMessage();

      expect(clientManager1.activeClients).toEqual(
        new Set(['client1', 'client2']),
      );
      expect(clientManager2.activeClients).toEqual(
        new Set(['client1', 'client2']),
      );
      expect(clientManager3.activeClients).toEqual(
        new Set(['client3', 'client4']),
      );
      expect(clientManager4.activeClients).toEqual(
        new Set(['client3', 'client4']),
      );

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

      const activeClients1 = clientManager1.activeClients;
      const activeClients2 = clientManager2.activeClients;

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
      await waitForPostMessage();

      expect(clientManager1.activeClients).toEqual(
        new Set(['client1', 'client2', 'client3']),
      );
      expect(clientManager2.activeClients).toEqual(
        new Set(['client1', 'client2', 'client3']),
      );
      expect(clientManager3.activeClients).toEqual(
        new Set(['client1', 'client2', 'client3']),
      );

      ac1.abort();

      await vi.waitFor(
        () => {
          expect(clientManager2.activeClients).toEqual(
            new Set(['client2', 'client3']),
          );
          expect(clientManager2.activeClients).toEqual(
            new Set(['client2', 'client3']),
          );
          expect(clientManager3.activeClients).toEqual(
            new Set(['client2', 'client3']),
          );
        },
        {interval: 10, timeout: 100},
      );

      ac2.abort();
      ac3.abort();
    });
  });

  describe('onDelete', () => {
    test.each(cases)('%s', async (_desc, setup) => {
      setup();
      // This sets up three clients in the same group and listens for deletions.
      const clientGroupID = nanoid();

      const ac1 = new AbortController();
      const ac2 = new AbortController();
      const ac3 = new AbortController();

      const deletions = new Queue<{
        clientID: string;
        deletedClientID: string;
      }>();

      const clientManager1 = await ActiveClientsManager.create(
        clientGroupID,
        'client1',
        ac1.signal,
      );
      clientManager1.onDelete = deletedClientID => {
        deletions.enqueue({clientID: 'client1', deletedClientID});
      };

      const clientManager2 = await ActiveClientsManager.create(
        clientGroupID,
        'client2',
        ac2.signal,
      );
      clientManager2.onDelete = deletedClientID => {
        deletions.enqueue({clientID: 'client2', deletedClientID});
      };

      // Initially no deletions should occur
      expect(deletions.size()).toBe(0);

      const clientManager3 = await ActiveClientsManager.create(
        clientGroupID,
        'client3',
        ac3.signal,
      );
      clientManager3.onDelete = deletedClientID => {
        deletions.enqueue({clientID: 'client3', deletedClientID});
      };

      // Still no deletions after adding a client
      expect(deletions.size()).toBe(0);

      // Now abort one client and check for deletion notifications
      ac1.abort();

      await expectDequeue(deletions, [
        {
          clientID: 'client2',
          deletedClientID: 'client1',
        },
        {
          clientID: 'client3',
          deletedClientID: 'client1',
        },
      ]);

      // Abort another client
      ac2.abort();

      await expectDequeue(deletions, [
        {
          clientID: 'client3',
          deletedClientID: 'client2',
        },
      ]);

      // Abort the last client - no more deletion notifications since no other clients are listening
      ac3.abort();

      await expectDequeue(deletions, []);
    });

    test.each(cases)(
      '%s - client does not get notified of its own deletion',
      async (_desc, setup) => {
        setup();
        const clientGroupID = nanoid();

        const ac1 = new AbortController();
        const ac2 = new AbortController();

        const deletions = new Queue<string>();

        const clientManager1 = await ActiveClientsManager.create(
          clientGroupID,
          'client1',
          ac1.signal,
        );
        clientManager1.onDelete = deletedClientID => {
          deletions.enqueue(deletedClientID);
        };

        const clientManager2 = await ActiveClientsManager.create(
          clientGroupID,
          'client2',
          ac2.signal,
        );
        clientManager2.onDelete = deletedClientID => {
          deletions.enqueue(deletedClientID);
        };

        await waitForPostMessage();

        // Abort client1 - client1 should not get notified of its own deletion
        ac1.abort();

        await vi.waitFor(
          async () => {
            // Only client2 should be notified about client1's deletion
            expect(deletions.size()).toBe(1);
            const deletedClient = await deletions.dequeue();
            expect(deletedClient).toBe('client1');
          },
          {interval: 10, timeout: 100},
        );

        ac2.abort();
      },
    );

    test.each(cases)(
      '%s - no notification when onDelete is undefined',
      async (_desc, setup) => {
        setup();
        const clientGroupID = nanoid();

        const ac1 = new AbortController();
        const ac2 = new AbortController();

        void (await ActiveClientsManager.create(
          clientGroupID,
          'client1',
          ac1.signal,
        ));
        // Intentionally not setting onDelete callback

        const clientManager2 = await ActiveClientsManager.create(
          clientGroupID,
          'client2',
          ac2.signal,
        );
        // Intentionally not setting onDelete callback

        await waitForPostMessage();

        // This should not throw even though onDelete is undefined
        ac1.abort();
        await vi.waitFor(
          () => {
            // Verify the client was actually removed from active clients
            expect(clientManager2.activeClients).toEqual(new Set(['client2']));
          },
          {interval: 10, timeout: 100},
        );

        ac2.abort();
      },
    );
  });

  describe('onAdd', () => {
    test.each(cases)('%s', async (_desc, setup) => {
      setup();
      // This sets up clients in the same group and listens for additions.
      const clientGroupID = nanoid();

      const ac1 = new AbortController();
      const ac2 = new AbortController();
      const ac3 = new AbortController();

      const additions = new Queue<{
        clientID: string;
        addedClientID: string;
      }>();

      const clientManager1 = await ActiveClientsManager.create(
        clientGroupID,
        'client1',
        ac1.signal,
      );
      clientManager1.onAdd = addedClientID => {
        additions.enqueue({clientID: 'client1', addedClientID});
      };

      // Initially no additions should occur (client1 doesn't get notified of its own addition)
      expect(additions.size()).toBe(0);

      const clientManager2 = await ActiveClientsManager.create(
        clientGroupID,
        'client2',
        ac2.signal,
      );
      clientManager2.onAdd = addedClientID => {
        additions.enqueue({clientID: 'client2', addedClientID});
      };

      // client1 should be notified of client2's addition
      await expectDequeue(additions, [
        {
          clientID: 'client1',
          addedClientID: 'client2',
        },
      ]);

      const clientManager3 = await ActiveClientsManager.create(
        clientGroupID,
        'client3',
        ac3.signal,
      );
      clientManager3.onAdd = addedClientID => {
        additions.enqueue({clientID: 'client3', addedClientID});
      };

      // Both client1 and client2 should be notified of client3's addition
      await expectDequeue(additions, [
        {
          clientID: 'client1',
          addedClientID: 'client3',
        },
        {
          clientID: 'client2',
          addedClientID: 'client3',
        },
      ]);

      ac1.abort();
      ac2.abort();
      ac3.abort();
    });

    test.each(cases)(
      '%s - client does not get notified of its own addition',
      async (_desc, setup) => {
        setup();
        const clientGroupID = nanoid();

        const ac1 = new AbortController();
        const ac2 = new AbortController();

        const additions = new Queue<string>();

        const clientManager1 = await ActiveClientsManager.create(
          clientGroupID,
          'client1',
          ac1.signal,
        );
        clientManager1.onAdd = addedClientID => {
          additions.enqueue(addedClientID);
        };

        // client1 should not get notified of its own addition
        expect(additions.size()).toBe(0);

        const clientManager2 = await ActiveClientsManager.create(
          clientGroupID,
          'client2',
          ac2.signal,
        );
        clientManager2.onAdd = addedClientID => {
          additions.enqueue(addedClientID);
        };

        await waitForPostMessage();

        // Only client1 should be notified about client2's addition
        expect(additions.size()).toBe(1);
        const addedClient = await additions.dequeue();
        expect(addedClient).toBe('client2');

        ac1.abort();
        ac2.abort();
      },
    );

    test.each(cases)(
      '%s - no notification when onAdd is undefined',
      async (_desc, setup) => {
        setup();
        const clientGroupID = nanoid();

        const ac1 = new AbortController();
        const ac2 = new AbortController();

        const clientManager1 = await ActiveClientsManager.create(
          clientGroupID,
          'client1',
          ac1.signal,
        );
        // Intentionally not setting onAdd callback

        void (await ActiveClientsManager.create(
          clientGroupID,
          'client2',
          ac2.signal,
        ));
        // Intentionally not setting onAdd callback

        await waitForPostMessage();

        // This should not throw even though onAdd is undefined
        // Verify the client was actually added to active clients
        expect(clientManager1.activeClients).toEqual(
          new Set(['client1', 'client2']),
        );

        ac1.abort();
        ac2.abort();
      },
    );

    test.each(cases)(
      '%s - multiple clients get notified of single addition',
      async (_desc, setup) => {
        setup();
        const clientGroupID = nanoid();

        const ac1 = new AbortController();
        const ac2 = new AbortController();
        const ac3 = new AbortController();
        const ac4 = new AbortController();

        const additions = new Queue<{
          clientID: string;
          addedClientID: string;
        }>();

        // Create first three clients
        const clientManager1 = await ActiveClientsManager.create(
          clientGroupID,
          'client1',
          ac1.signal,
        );
        clientManager1.onAdd = addedClientID => {
          additions.enqueue({clientID: 'client1', addedClientID});
        };

        const clientManager2 = await ActiveClientsManager.create(
          clientGroupID,
          'client2',
          ac2.signal,
        );
        clientManager2.onAdd = addedClientID => {
          additions.enqueue({clientID: 'client2', addedClientID});
        };

        const clientManager3 = await ActiveClientsManager.create(
          clientGroupID,
          'client3',
          ac3.signal,
        );
        clientManager3.onAdd = addedClientID => {
          additions.enqueue({clientID: 'client3', addedClientID});
        };

        // Wait for all existing additions to be processed
        await waitForPostMessage();
        // Clear any existing additions from setup
        while (additions.size() > 0) {
          await additions.dequeue();
        }

        // Now add a fourth client - all three existing clients should be notified
        void (await ActiveClientsManager.create(
          clientGroupID,
          'client4',
          ac4.signal,
        ));

        await expectDequeue(additions, [
          {
            clientID: 'client1',
            addedClientID: 'client4',
          },
          {
            clientID: 'client2',
            addedClientID: 'client4',
          },
          {
            clientID: 'client3',
            addedClientID: 'client4',
          },
        ]);

        ac1.abort();
        ac2.abort();
        ac3.abort();
        ac4.abort();
      },
    );
  });
});

// postMessage uses a message queue. By adding another message to the queue,
// we can ensure that the first message is processed before the second one.
function waitForPostMessage() {
  return new Promise<void>(resolve => {
    const name = nanoid();
    const c1 = new BroadcastChannel(name);
    const c2 = new BroadcastChannel(name);
    c2.postMessage('');
    c1.onmessage = () => {
      c1.close();
      c2.close();
      resolve();
    };
  });
}
