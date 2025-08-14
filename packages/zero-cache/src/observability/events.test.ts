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
