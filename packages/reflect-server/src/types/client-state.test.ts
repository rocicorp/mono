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
  const tracker1 = {
    onConnectionCountChange: jest.fn(),
  };
  const tracker2 = {
    onConnectionCountChange: jest.fn(),
  };
  let map: ConnectionCountTrackingClientMap;

  beforeEach(() => {
    map = new ConnectionCountTrackingClientMap(tracker1, tracker2);
  });

  afterEach(() => {
    jest.resetAllMocks();
  });

  test('put', () => {
    expect(tracker1.onConnectionCountChange).not.toBeCalled;
    expect(tracker2.onConnectionCountChange).not.toBeCalled;

    map.set('foo', state1);

    expect(tracker1.onConnectionCountChange).toHaveBeenCalledTimes(1);
    expect(tracker1.onConnectionCountChange.mock.lastCall?.[0]).toBe(1);
    expect(tracker2.onConnectionCountChange).toHaveBeenCalledTimes(1);
    expect(tracker2.onConnectionCountChange.mock.lastCall?.[0]).toBe(1);

    // Overwrites existing key. Final count should be the same.
    map.set('foo', state2);

    expect(tracker1.onConnectionCountChange).toHaveBeenCalledTimes(2);
    expect(tracker1.onConnectionCountChange.mock.lastCall?.[0]).toBe(1);
    expect(tracker2.onConnectionCountChange).toHaveBeenCalledTimes(2);
    expect(tracker2.onConnectionCountChange.mock.lastCall?.[0]).toBe(1);

    map.set('bar', state3);

    expect(tracker1.onConnectionCountChange).toHaveBeenCalledTimes(3);
    expect(tracker1.onConnectionCountChange.mock.lastCall?.[0]).toBe(2);
    expect(tracker2.onConnectionCountChange).toHaveBeenCalledTimes(3);
    expect(tracker2.onConnectionCountChange.mock.lastCall?.[0]).toBe(2);
  });

  test('delete', () => {
    expect(tracker1.onConnectionCountChange).not.toBeCalled;
    expect(tracker2.onConnectionCountChange).not.toBeCalled;

    map.set('foo', state1);
    map.set('bar', state2);
    map.set('baz', state3);

    expect(tracker1.onConnectionCountChange).toHaveBeenCalledTimes(3);
    expect(tracker1.onConnectionCountChange.mock.lastCall?.[0]).toBe(3);
    expect(tracker2.onConnectionCountChange).toHaveBeenCalledTimes(3);
    expect(tracker2.onConnectionCountChange.mock.lastCall?.[0]).toBe(3);

    expect(map.delete('foo')).toBe(true);

    expect(tracker1.onConnectionCountChange).toHaveBeenCalledTimes(4);
    expect(tracker1.onConnectionCountChange.mock.lastCall?.[0]).toBe(2);
    expect(tracker2.onConnectionCountChange).toHaveBeenCalledTimes(4);
    expect(tracker2.onConnectionCountChange.mock.lastCall?.[0]).toBe(2);

    expect(map.delete('foo')).toBe(false);

    expect(tracker1.onConnectionCountChange).toHaveBeenCalledTimes(4);
    expect(tracker2.onConnectionCountChange).toHaveBeenCalledTimes(4);

    expect(map.delete('bar')).toBe(true);

    expect(tracker1.onConnectionCountChange).toHaveBeenCalledTimes(5);
    expect(tracker1.onConnectionCountChange.mock.lastCall?.[0]).toBe(1);
    expect(tracker2.onConnectionCountChange).toHaveBeenCalledTimes(5);
    expect(tracker2.onConnectionCountChange.mock.lastCall?.[0]).toBe(1);

    expect(map.delete('baz')).toBe(true);

    expect(tracker1.onConnectionCountChange).toHaveBeenCalledTimes(6);
    expect(tracker1.onConnectionCountChange.mock.lastCall?.[0]).toBe(0);
    expect(tracker2.onConnectionCountChange).toHaveBeenCalledTimes(6);
    expect(tracker2.onConnectionCountChange.mock.lastCall?.[0]).toBe(0);
  });

  test('clear', () => {
    expect(tracker1.onConnectionCountChange).not.toBeCalled;
    expect(tracker2.onConnectionCountChange).not.toBeCalled;

    map.set('foo', state1);
    map.set('bar', state2);
    map.set('baz', state3);

    expect(tracker1.onConnectionCountChange).toHaveBeenCalledTimes(3);
    expect(tracker1.onConnectionCountChange.mock.lastCall?.[0]).toBe(3);
    expect(tracker2.onConnectionCountChange).toHaveBeenCalledTimes(3);
    expect(tracker2.onConnectionCountChange.mock.lastCall?.[0]).toBe(3);

    map.clear();

    expect(tracker1.onConnectionCountChange).toHaveBeenCalledTimes(4);
    expect(tracker1.onConnectionCountChange.mock.lastCall?.[0]).toBe(0);
    expect(tracker2.onConnectionCountChange).toHaveBeenCalledTimes(4);
    expect(tracker2.onConnectionCountChange.mock.lastCall?.[0]).toBe(0);
  });
});
