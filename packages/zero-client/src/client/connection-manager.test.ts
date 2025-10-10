import {describe, expect, test, vi} from 'vitest';
import {ConnectionManager} from './connection-manager.ts';
import {ConnectionStatus} from './connection-status.ts';

describe('ConnectionManager', () => {
  test('initial state is disconnected', () => {
    const manager = new ConnectionManager();

    expect(manager.state).toEqual({name: ConnectionStatus.Disconnected});
    expect(manager.is(ConnectionStatus.Disconnected)).toBe(true);
    expect(manager.is(ConnectionStatus.Connected)).toBe(false);
  });

  test('setStatus updates state and notifies listeners on change', () => {
    const manager = new ConnectionManager();
    const listener = vi.fn();
    manager.subscribe(listener);

    const changed = manager.setStatus(ConnectionStatus.Connecting);

    expect(changed).toBe(true);
    expect(manager.state).toEqual({name: ConnectionStatus.Connecting});
    expect(manager.is(ConnectionStatus.Connecting)).toBe(true);
    expect(listener).toHaveBeenCalledTimes(1);
    expect(listener).toHaveBeenLastCalledWith({
      name: ConnectionStatus.Connecting,
    });

    manager.setStatus(ConnectionStatus.Connected);

    expect(manager.state).toEqual({name: ConnectionStatus.Connected});
    expect(listener).toHaveBeenCalledTimes(2);
    expect(listener).toHaveBeenLastCalledWith({
      name: ConnectionStatus.Connected,
    });
  });

  test('setStatus returns false and does not notify when status is unchanged', () => {
    const manager = new ConnectionManager();
    const listener = vi.fn();
    manager.subscribe(listener);

    expect(manager.setStatus(ConnectionStatus.Disconnected)).toBe(false);
    expect(listener).not.toHaveBeenCalled();

    expect(manager.setStatus(ConnectionStatus.Connecting)).toBe(true);
    expect(listener).toHaveBeenCalledTimes(1);

    listener.mockClear();

    expect(manager.setStatus(ConnectionStatus.Connecting)).toBe(false);
    expect(listener).not.toHaveBeenCalled();
  });
});
