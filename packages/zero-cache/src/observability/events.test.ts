import {LogContext} from '@rocicorp/logger';
import {getLocal, type Mockttp} from 'mockttp';
import {beforeEach, expect, test} from 'vitest';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../shared/src/logging-test-utils.ts';
import {initEventSink, publishCriticalEvent} from './events.ts';

let sink: Mockttp;

beforeEach(async () => {
  sink = getLocal();
  await sink.start();

  return () => sink.stop();
});

test('publish with backoff', async () => {
  const failure = await sink.forPost().times(2).thenCloseConnection();
  const success = await sink.forPost().thenReply(200);

  process.env.MY_CLOUD_EVENT_SINK = sink.url;
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

  await publishCriticalEvent(lc, {
    type: 'my-type',
    time: new Date(Date.UTC(2024, 7, 14, 3, 2, 1)).toISOString(),
  });

  const failedRequests = await failure.getSeenRequests();
  expect(failedRequests).toHaveLength(2);
  for (const r of failedRequests) {
    expect(r.headers).toMatchObject({
      'ce-baz': '123',
      'ce-foo': 'bar',
      'ce-id': expect.any(String),
      'ce-source': 'my-task-id',
      'ce-specversion': '1.0',
      'ce-time': '2024-08-14T03:02:01.000Z',
      'ce-type': 'my-type',
      'connection': 'keep-alive',
      'content-type': 'application/json; charset=utf-8',
      'host': 'localhost:8000',
      'transfer-encoding': 'chunked',
    });
  }

  const [r] = await success.getSeenRequests();
  expect(r.headers).toMatchObject({
    'ce-baz': '123',
    'ce-foo': 'bar',
    'ce-id': expect.any(String),
    'ce-source': 'my-task-id',
    'ce-specversion': '1.0',
    'ce-time': '2024-08-14T03:02:01.000Z',
    'ce-type': 'my-type',
    'connection': 'keep-alive',
    'content-type': 'application/json; charset=utf-8',
    'host': 'localhost:8000',
    'transfer-encoding': 'chunked',
  });

  expect(logSink.messages[0][2]).toMatchObject([
    'Publishing CloudEvent: my-type',
  ]);

  expect(logSink.messages[1][2]).toMatchObject([
    'Error publishing my-type (attempt 1)',
    expect.any(Error),
  ]);

  expect(logSink.messages[2][2]).toMatchObject([
    'Error publishing my-type (attempt 2)',
    expect.any(Error),
  ]);

  expect(logSink.messages[3][2]).toMatchObject([
    'Published CloudEvent: my-type',
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
