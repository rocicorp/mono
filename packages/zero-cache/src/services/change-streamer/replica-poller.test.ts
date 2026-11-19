import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {ReplicaPoller} from './replica-poller.ts';

test('a second start() throws', () => {
  const poller = new ReplicaPoller(createSilentLogContext(), ':memory:');
  const watermarks = poller.start();
  try {
    expect(() => poller.start()).toThrow('Already called start()');
  } finally {
    watermarks.cancel();
  }
});
