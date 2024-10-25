import {expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.js';
import type {Terminator} from './life-cycle.js';
import {TaskStateWatcher} from './task-state-watcher.js';

test.each([
  [['PROVISIONING', 'PENDING', 'ACTIVATING', 'RUNNING'], 'DEACTIVATING'],
  [['PENDING', 'ACTIVATING'], 'STOPPING'],
  [['ACTIVATING', 'RUNNING'], 'DEACTIVATING'],
  [['ACTIVATING', 'RUNNING'], 'STOPPING'],
  [['ACTIVATING', 'RUNNING'], 'DEPROVISIONING'],
  [['ACTIVATING', 'RUNNING'], 'STOPPED'],
])('task state changes %s => %s', (pre, post) => {
  const terminator = {
    startDrain: vi.fn(),
  };

  const watcher = new TaskStateWatcher(
    createSilentLogContext(),
    terminator as unknown as Terminator,
  );

  for (const state of pre) {
    watcher.updateDesiredStatus({['DesiredStatus']: state});
  }
  expect(terminator.startDrain).not.toBeCalled();

  watcher.updateDesiredStatus({['DesiredStatus']: post});
  expect(terminator.startDrain).toHaveBeenCalledOnce();
});
