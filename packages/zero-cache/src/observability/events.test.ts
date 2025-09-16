/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {LogContext} from '@rocicorp/logger';
import {expect, test} from 'vitest';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../shared/src/logging-test-utils.ts';
import {initEventSink, publishEvent} from './events.ts';

test('initEventSink', () => {
  process.env.MY_CLOUD_EVENT_SINK = 'http://localhost:9999';
  process.env.MY_CLOUD_EVENT_OVERRIDES = JSON.stringify({
    extensions: {
      foo: 'bar',
      baz: 123,
    },
  });

  const logSink = new TestLogSink();
  const lc = new LogContext('debug', {}, logSink);

  initEventSink(createSilentLogContext(), {
    taskID: 'my-task-id',
    cloudEvent: {
      sinkEnv: 'MY_CLOUD_EVENT_SINK',
      extensionOverridesEnv: 'MY_CLOUD_EVENT_OVERRIDES',
    },
  });

  publishEvent(lc, {
    type: 'my-type',
    time: new Date(Date.UTC(2024, 7, 14, 3, 2, 1)).toISOString(),
  });

  expect(logSink.messages[0][2]).toMatchObject([
    'Publishing CloudEvent: my-type',
    {
      type: 'my-type',
      time: '2024-08-14T03:02:01.000Z',
      source: 'my-task-id',
      specversion: '1.0',
      data: {
        time: '2024-08-14T03:02:01.000Z',
        type: 'my-type',
      },
      foo: 'bar',
      baz: 123,
    },
  ]);
});
