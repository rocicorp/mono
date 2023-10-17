import {
  afterEach,
  beforeEach,
  describe,
  expect,
  jest,
  test,
} from '@jest/globals';
import {
  ConnectionCountTrackingClientMap,
  type ClientState,
} from './client-state.js';

describe('client count tracking map', () => {
  const state1 = {} as ClientState;
  const state2 = {} as ClientState;
  const state3 = {} as ClientState;
  const tracker = {
    onConnectionCountChange: jest.fn(),
  };
  let map: ConnectionCountTrackingClientMap;

  beforeEach(() => {
    map = new ConnectionCountTrackingClientMap(tracker);
  });

  afterEach(() => {
    jest.resetAllMocks();
  });

  test('put', () => {
    expect(tracker.onConnectionCountChange).not.toBeCalled;

    map.set('foo', state1);

    expect(tracker.onConnectionCountChange).toHaveBeenCalledTimes(1);
    expect(tracker.onConnectionCountChange.mock.lastCall?.[0]).toBe(1);

    // Overwrites existing key. Final count should be the same.
    map.set('foo', state2);

    expect(tracker.onConnectionCountChange).toHaveBeenCalledTimes(2);
    expect(tracker.onConnectionCountChange.mock.lastCall?.[0]).toBe(1);

    map.set('bar', state3);

    expect(tracker.onConnectionCountChange).toHaveBeenCalledTimes(3);
    expect(tracker.onConnectionCountChange.mock.lastCall?.[0]).toBe(2);
  });

  test('delete', () => {
    expect(tracker.onConnectionCountChange).not.toBeCalled;

    map.set('foo', state1);
    map.set('bar', state2);
    map.set('baz', state3);

    expect(tracker.onConnectionCountChange).toHaveBeenCalledTimes(3);
    expect(tracker.onConnectionCountChange.mock.lastCall?.[0]).toBe(3);

    expect(map.delete('foo')).toBe(true);

    expect(tracker.onConnectionCountChange).toHaveBeenCalledTimes(4);
    expect(tracker.onConnectionCountChange.mock.lastCall?.[0]).toBe(2);

    expect(map.delete('foo')).toBe(false);

    expect(tracker.onConnectionCountChange).toHaveBeenCalledTimes(4);

    expect(map.delete('bar')).toBe(true);

    expect(tracker.onConnectionCountChange).toHaveBeenCalledTimes(5);
    expect(tracker.onConnectionCountChange.mock.lastCall?.[0]).toBe(1);

    expect(map.delete('baz')).toBe(true);

    expect(tracker.onConnectionCountChange).toHaveBeenCalledTimes(6);
    expect(tracker.onConnectionCountChange.mock.lastCall?.[0]).toBe(0);
  });

  test('clear', () => {
    expect(tracker.onConnectionCountChange).not.toBeCalled;

    map.set('foo', state1);
    map.set('bar', state2);
    map.set('baz', state3);

    expect(tracker.onConnectionCountChange).toHaveBeenCalledTimes(3);
    expect(tracker.onConnectionCountChange.mock.lastCall?.[0]).toBe(3);

    map.clear();

    expect(tracker.onConnectionCountChange).toHaveBeenCalledTimes(4);
    expect(tracker.onConnectionCountChange.mock.lastCall?.[0]).toBe(0);
  });
});
