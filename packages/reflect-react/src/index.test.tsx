import {expect, test} from '@jest/globals';
import {
  PresenceSubscribable,
  SubscribeToPresenceCallback,
  usePresence,
} from './index.js';
import React from 'react';
import {create, act, ReactTestRenderer} from 'react-test-renderer';

test('undefined/null PresenceSubscribable', async () => {
  function A({presenceSubscribable}: {presenceSubscribable: null | undefined}) {
    const presentClientIDs = usePresence(presenceSubscribable);
    return JSON.stringify([...presentClientIDs]);
  }

  const t = async (presenceSubscribable: null | undefined) => {
    let root: ReactTestRenderer | undefined;
    await act(() => {
      root = create(<A presenceSubscribable={presenceSubscribable}></A>);
    });

    expect(root?.toJSON()).toEqual('[]');
  };

  await t(undefined);
  await t(null);
});

test('updating', async () => {
  function A({
    presenceSubscribable,
  }: {
    presenceSubscribable: PresenceSubscribable;
  }) {
    const presentClientIDs = usePresence(presenceSubscribable);
    return JSON.stringify([...presentClientIDs]);
  }

  const callbacks: SubscribeToPresenceCallback[] = [];
  const presenceSubscribable = {
    subscribeToPresence: (callback: SubscribeToPresenceCallback) => {
      callbacks.push(callback);
      return () => {};
    },
  };

  let root: ReactTestRenderer | undefined;
  await act(() => {
    root = create(<A presenceSubscribable={presenceSubscribable}></A>);
  });

  expect(root?.toJSON()).toEqual('[]');

  expect(callbacks.length).toEqual(1);
  await act(() => {
    callbacks[0](['client1', 'client2']);
  });

  expect(root?.toJSON()).toEqual('["client1","client2"]');

  expect(callbacks.length).toEqual(1);
  await act(() => {
    callbacks[0](['client3']);
  });

  expect(root?.toJSON()).toEqual('["client3"]');
});

test('cleanup', async () => {
  function A({
    presenceSubscribable,
  }: {
    presenceSubscribable: PresenceSubscribable;
  }) {
    const presentClientIDs = usePresence(presenceSubscribable);
    return JSON.stringify([...presentClientIDs]);
  }

  const callbacks: SubscribeToPresenceCallback[] = [];
  const removeFnCalls: number[] = [];
  const presenceSubscribable = {
    subscribeToPresence: (callback: SubscribeToPresenceCallback) => {
      callbacks.push(callback);
      removeFnCalls.push(0);
      const removeFnCallsIndex = removeFnCalls.length - 1;
      return () => {
        removeFnCalls[removeFnCallsIndex]++;
      };
    },
  };

  let root: ReactTestRenderer | undefined;
  await act(() => {
    root = create(<A presenceSubscribable={presenceSubscribable}></A>);
  });

  expect(root?.toJSON()).toEqual('[]');

  expect(callbacks.length).toEqual(1);
  expect(removeFnCalls).toEqual([0]);
  await act(() => {
    callbacks[0](['client1', 'client2']);
  });

  expect(root?.toJSON()).toEqual('["client1","client2"]');

  await act(() => {
    root?.unmount();
  });

  expect(removeFnCalls).toEqual([1]);
});
