import {beforeEach, describe, expect, test, vi} from 'vitest';
import type {ConnectionManager} from './connection-manager.ts';
import {ConnectionStatus} from './connection-status.ts';
import {ConnectionImpl} from './connection.ts';
import {ZeroLogContext} from './zero-log-context.ts';

describe('ConnectionImpl', () => {
  let manager: ConnectionManager;
  let lc: ZeroLogContext;
  let logSpy: ReturnType<typeof vi.fn>;
  // let disconnectCallback: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    logSpy = vi.fn();
    lc = new ZeroLogContext('debug', {}, {log: logSpy});
    // disconnectCallback = vi.fn().mockResolvedValue(undefined);

    // Mock connection manager with minimal required behavior
    manager = {
      state: {name: ConnectionStatus.Connecting},
      isInTerminalState: vi.fn().mockReturnValue(false),
      connecting: vi
        .fn()
        .mockReturnValue({nextStatePromise: Promise.resolve()}),
      subscribe: vi.fn().mockReturnValue(vi.fn()),
    } as unknown as ConnectionManager;
  });

  describe('state', () => {
    test('returns current manager state', () => {
      const connection = new ConnectionImpl(manager, lc);

      expect(connection.state.current).toBe(manager.state);
    });

    test('subscribe delegates to manager', () => {
      const connection = new ConnectionImpl(manager, lc);
      const listener = vi.fn();

      connection.state.subscribe(listener);

      expect(manager.subscribe).toHaveBeenCalledWith(listener);
    });
  });

  describe('connect', () => {
    test('returns early when not in terminal state', async () => {
      vi.mocked(manager.isInTerminalState).mockReturnValue(false);
      const connection = new ConnectionImpl(manager, lc);

      await connection.connect();

      expect(manager.connecting).not.toHaveBeenCalled();
      expect(logSpy).toHaveBeenCalledWith(
        'debug',
        {connect: undefined},
        'connect() called but not in a terminal state. Current state:',
        ConnectionStatus.Connecting,
      );
    });

    test('calls manager.connecting() and waits for state change', async () => {
      vi.mocked(manager.isInTerminalState).mockReturnValue(true);
      const nextStatePromise = Promise.resolve(manager.state);
      vi.mocked(manager.connecting).mockReturnValue({
        nextStatePromise,
      } as ReturnType<ConnectionManager['connecting']>);
      const connection = new ConnectionImpl(manager, lc);

      await connection.connect();

      expect(manager.connecting).toHaveBeenCalledTimes(1);
      expect(logSpy).toHaveBeenCalledWith(
        'info',
        {connect: undefined},
        'Resuming connection from state: connecting',
      );
    });
  });

  // TODO(0xcadams): reenable when disconnect is implemented
  // describe('disconnect', () => {
  //   test('calls disconnect callback and logs', async () => {
  //     const connection = new ConnectionImpl(manager, lc, disconnectCallback);

  //     await connection.disconnect();

  //     expect(disconnectCallback).toHaveBeenCalledTimes(1);
  //     expect(logSpy).toHaveBeenCalledWith(
  //       'info',
  //       {disconnect: undefined},
  //       'User requested disconnect',
  //     );
  //   });
  // });
});
