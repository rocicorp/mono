import {jest, afterEach, beforeEach, test, expect} from '@jest/globals';
import type {ReadonlyJSONObject} from 'replicache';
import {
  DatadogLogSink,
  FORCE_FLUSH_THRESHOLD,
  MAX_ENTRY_CHARS,
  MAX_LOG_ENTRIES_PER_FLUSH,
} from './datadog-log-sink.js';
import {resolver} from '@rocicorp/resolver';
import realFetch from 'cross-fetch';

const fetch = jest.fn(realFetch);
globalThis.fetch = fetch;

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
  fetch.mockReturnValue(Promise.resolve(new Response('{}')));
});

afterEach(() => {
  jest.restoreAllMocks();
});

function stringifyMany(...messages: ReadonlyJSONObject[]): string {
  return messages.map(m => JSON.stringify(m)).join('\n');
}

test('calling error also calls flush', () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
  });
  const flushSpy = jest
    .spyOn(l, 'flush')
    .mockImplementation(() => Promise.resolve(undefined));
  l.log('error', {usr: {name: 'bob'}}, 'aaa');
  expect(flushSpy).toHaveBeenCalledTimes(1);
});

test('reaching flush threshold also calls flush', () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
  });
  const flushSpy = jest
    .spyOn(l, 'flush')
    .mockImplementation(() => Promise.resolve(undefined));
  for (let i = 0; i < FORCE_FLUSH_THRESHOLD - 1; i++) {
    l.log('info', {usr: {name: 'bob'}}, 'aaa');
  }
  expect(flushSpy).not.toHaveBeenCalled();
  // The next log should force a flush.
  l.log('info', {usr: {name: 'bob'}}, 'aaa');
  expect(flushSpy).toHaveBeenCalledTimes(1);

  // Subsequent logs don't repeatedly flush.
  for (let i = 0; i < 5; i++) {
    l.log('info', {usr: {name: 'bob'}}, 'aaa');
  }
  expect(flushSpy).toHaveBeenCalledTimes(1);
});

test('does not flush more than max entries', async () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
    interval: 10,
  });

  let fetchCount = 0;
  const fetchLatches = [resolver<void>(), resolver<void>(), resolver<void>()];
  const {promise: canFinishFetch, resolve: finishFetch} = resolver<Response>();

  fetch.mockImplementation(() => {
    fetchLatches[fetchCount++].resolve();
    return canFinishFetch;
  });
  // Trigger the first force flush.
  for (let i = 0; i < FORCE_FLUSH_THRESHOLD; i++) {
    l.log('info', {usr: {name: 'bob'}}, 'aaa');
  }

  const numLogEntriesInRequest = (n: number) => {
    const body = fetch.mock.calls[n][1]?.body;
    return String(body).split('\n').length;
  };

  // Wait for the resulting flush() to call fetch.
  await fetchLatches[0].promise;
  expect(numLogEntriesInRequest(0)).toBe(FORCE_FLUSH_THRESHOLD);

  // While fetch() is blocked, add 123 + MAX_LOG_ENTRIES more log statements.
  for (let i = 0; i < 123 + MAX_LOG_ENTRIES_PER_FLUSH; i++) {
    l.log('info', {usr: {name: 'bob'}}, 'aaa');
  }

  // Let the first fetch complete.
  finishFetch({ok: true} as unknown as Response);

  // Check the second fetch.
  await fetchLatches[1].promise;
  expect(numLogEntriesInRequest(1)).toBe(MAX_LOG_ENTRIES_PER_FLUSH);

  // Check the third fetch (not flushed till after flush interval
  // since message count is below FORCE_FLUSH_THRESHOLD).
  jest.advanceTimersByTime(10);
  await fetchLatches[2].promise;
  expect(numLogEntriesInRequest(2)).toBe(123);
});

test('flushes MAX_LOG_ENTRIES_PER_FLUSH at a time until size is below FORCE_FLUSH_THRESHOLD', async () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
    interval: 10,
  });

  let fetchCount = 0;
  const fetchLatches = [
    resolver<void>(),
    resolver<void>(),
    resolver<void>(),
    resolver<void>(),
    resolver<void>(),
  ];
  const fetchResponseResolvers = [
    resolver<Response>(),
    resolver<Response>(),
    resolver<Response>(),
    resolver<Response>(),
    resolver<Response>(),
  ];

  fetch.mockImplementation(() => {
    const i = fetchCount++;
    fetchLatches[i].resolve();
    return fetchResponseResolvers[i].promise;
  });
  // Trigger the first force flush.
  for (let i = 0; i < FORCE_FLUSH_THRESHOLD; i++) {
    l.log('info', {usr: {name: 'bob'}}, 'aaa');
  }

  const numLogEntriesInRequest = (n: number) => {
    const body = fetch.mock.calls[n][1]?.body;
    return String(body).split('\n').length;
  };

  // Wait for the resulting flush() to call fetch.
  await fetchLatches[0].promise;
  expect(numLogEntriesInRequest(0)).toBe(FORCE_FLUSH_THRESHOLD);

  // While fetch() is blocked, add 123 + FORCE_FLUSH_THRESHOLD + MAX_LOG_ENTRIES * 2 more log statements.
  for (
    let i = 0;
    i < 123 + FORCE_FLUSH_THRESHOLD + MAX_LOG_ENTRIES_PER_FLUSH * 2;
    i++
  ) {
    l.log('info', {usr: {name: 'bob'}}, 'aaa');
  }

  // Let the first fetch complete.
  fetchResponseResolvers[0].resolve({ok: true} as unknown as Response);

  // Check the second fetch.
  await fetchLatches[1].promise;
  fetchResponseResolvers[1].resolve({ok: true} as unknown as Response);
  expect(numLogEntriesInRequest(1)).toBe(MAX_LOG_ENTRIES_PER_FLUSH);

  // Check the third fetch.
  await fetchLatches[2].promise;
  fetchResponseResolvers[2].resolve({ok: true} as unknown as Response);
  expect(numLogEntriesInRequest(2)).toBe(MAX_LOG_ENTRIES_PER_FLUSH);

  // Check the fourth fetch
  await fetchLatches[3].promise;
  expect(numLogEntriesInRequest(3)).toBe(FORCE_FLUSH_THRESHOLD + 123);

  // While fetch() is blocked, add 123 more log statements.
  for (let i = 0; i < 123; i++) {
    l.log('info', {usr: {name: 'bob'}}, 'aaa');
  }
  fetchResponseResolvers[3].resolve({ok: true} as unknown as Response);

  // Check the final fetch (not flushed till after flush interval
  // since message count is below FORCE_FLUSH_THRESHOLD).
  jest.advanceTimersByTime(10);
  await fetchLatches[4].promise;
  fetchResponseResolvers[4].resolve({ok: true} as unknown as Response);
  expect(numLogEntriesInRequest(4)).toBe(123);
});

test('flush calls fetch', async () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
  });
  jest.setSystemTime(1);
  l.log('debug', {usr: {name: 'bob'}}, 'debug message');
  jest.setSystemTime(2);
  l.log('info', {usr: {name: 'bob'}}, 'info message');

  jest.setSystemTime(10);
  await l.flush();

  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch).toHaveBeenCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
    {
      body: stringifyMany(
        {
          usr: {name: 'bob'},
          date: 1,
          message: 'debug message',
          status: 'debug',
          flushDelayMs: 9,
        },
        {
          usr: {name: 'bob'},
          date: 2,
          message: 'info message',
          status: 'info',
          flushDelayMs: 8,
        },
      ),
      method: 'POST',
      keepalive: true,
    },
  );
});

test('flush calls fetch with specified baseURL', async () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
    baseURL: new URL('http://custom.base.com/api/path/log'),
  });
  jest.setSystemTime(1);
  l.log('debug', {usr: {name: 'bob'}}, 'debug message');
  jest.setSystemTime(2);
  l.log('info', {usr: {name: 'bob'}}, 'info message');

  jest.setSystemTime(10);
  await l.flush();

  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch).toHaveBeenCalledWith(
    'http://custom.base.com/api/path/log?dd-api-key=apiKey',
    {
      body: stringifyMany(
        {
          usr: {name: 'bob'},
          date: 1,
          message: 'debug message',
          status: 'debug',
          flushDelayMs: 9,
        },
        {
          usr: {name: 'bob'},
          date: 2,
          message: 'info message',
          status: 'info',
          flushDelayMs: 8,
        },
      ),
      method: 'POST',
      keepalive: true,
    },
  );
});

test('flush truncates large messages and batches', async () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
  });
  jest.setSystemTime(1);
  l.log('debug', {usr: {name: 'bob'}}, 'a'.repeat(MAX_ENTRY_CHARS * 2));
  l.log('debug', {usr: {name: 'bob'}}, 'b'.repeat(MAX_ENTRY_CHARS / 2));
  l.log('debug', {usr: {name: 'bob'}}, 'small message');
  l.log('debug', {usr: {name: 'bob'}}, 'c'.repeat(MAX_ENTRY_CHARS / 2));
  l.log('debug', {usr: {name: 'bob'}}, 'another small message');
  l.log('debug', {usr: {name: 'bob'}}, 'd'.repeat(MAX_ENTRY_CHARS * 2));
  l.log('debug', {usr: {name: 'bob'}}, 'last small message');

  await l.flush();

  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch).toHaveBeenCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
    {
      body: stringifyMany(
        // The first message should be truncated.
        {
          usr: {name: 'bob'},
          date: 1,
          message: '[Dropped message of length 2621518]',
          status: 'debug',
          flushDelayMs: 0,
        },
        // The second message fits within the total MAX_ENTRY_CHARS limit, but the other big message does not.
        {
          usr: {name: 'bob'},
          date: 1,
          message: 'b'.repeat(MAX_ENTRY_CHARS / 2),
          status: 'debug',
          flushDelayMs: 0,
        },
        {
          usr: {name: 'bob'},
          date: 1,
          message: 'small message',
          status: 'debug',
          flushDelayMs: 0,
        },
      ),
      method: 'POST',
      keepalive: true,
    },
  );

  await l.flush();
  expect(fetch).toHaveBeenCalledTimes(2);
  expect(fetch).toHaveBeenCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
    // The remaining messages are sent on the next flush.
    {
      body: stringifyMany(
        {
          usr: {name: 'bob'},
          date: 1,
          message: 'c'.repeat(MAX_ENTRY_CHARS / 2),
          status: 'debug',
          flushDelayMs: 0,
        },
        {
          usr: {name: 'bob'},
          date: 1,
          message: 'another small message',
          status: 'debug',
          flushDelayMs: 0,
        },
        {
          usr: {name: 'bob'},
          date: 1,
          message: '[Dropped message of length 2621518]',
          status: 'debug',
          flushDelayMs: 0,
        },
        {
          usr: {name: 'bob'},
          date: 1,
          message: 'last small message',
          status: 'debug',
          flushDelayMs: 0,
        },
      ),

      method: 'POST',
      keepalive: true,
    },
  );
});

test('reserved keys are prefixed', async () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
  });
  jest.setSystemTime(1);
  l.log(
    'debug',
    {
      usr: {name: 'bob'},
      host: 'testHost',
      source: 'testSource',
      status: 'testStatus',
      service: 'testService',
      version: 'testVersion',
      ['trace_id']: 'testTrace_id',
      message: 'testMessage',
      msg: 'testMsg',
      date: 'test-date',
      flushDelayMs: 'test-flushDelayMs',
      flushRetryCount: 'test-flushRetryCount',
    },
    'debug message',
  );

  jest.setSystemTime(10);
  await l.flush();

  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch).toHaveBeenCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
    {
      body: stringifyMany({
        usr: {name: 'bob'},
        ['@DATADOG_RESERVED_host']: 'testHost',
        ['@DATADOG_RESERVED_source']: 'testSource',
        ['@DATADOG_RESERVED_status']: 'testStatus',
        ['@DATADOG_RESERVED_service']: 'testService',
        ['@DATADOG_RESERVED_version']: 'testVersion',
        ['@DATADOG_RESERVED_trace_id']: 'testTrace_id',
        ['@DATADOG_RESERVED_message']: 'testMessage',
        ['@DATADOG_RESERVED_msg']: 'testMsg',
        ['@DATADOG_RESERVED_date']: 'test-date',
        ['@DATADOG_RESERVED_flushDelayMs']: 'test-flushDelayMs',
        ['@DATADOG_RESERVED_flushRetryCount']: 'test-flushRetryCount',
        date: 1,
        message: 'debug message',
        status: 'debug',
        flushDelayMs: 9,
      }),
      method: 'POST',
      keepalive: true,
    },
  );
});

test('Errors in multi arg messages are converted to JSON', async () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
  });

  jest.setSystemTime(1);
  l.log(
    'info',
    {usr: {name: 'bob'}},
    'Logging an error',
    new Error('Test error msg'),
    'after',
  );

  await l.flush();

  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch.mock.calls[0][0]).toEqual(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
  );
  const request = fetch.mock.calls[0][1];
  expect(request).toBeDefined();
  if (request === undefined) {
    throw new Error('Expect request to be defined');
  }
  expect(request.method).toEqual('POST');
  const {body} = request;
  expect(body).toBeDefined();
  if (!body) {
    throw new Error('Expect body to be defined and non-null');
  }
  const parsedBody = JSON.parse(body.toString());
  expect(parsedBody.date).toEqual(1);
  expect(parsedBody.status).toEqual('info');
  expect(parsedBody.message.length).toEqual(3);
  expect(parsedBody.message[0]).toEqual('Logging an error');
  expect(parsedBody.message[1].name).toEqual('Error');
  expect(parsedBody.message[1].message).toEqual('Test error msg');
  expect(parsedBody.message[1].stack).toBeDefined();
  expect(parsedBody.message[2]).toEqual('after');
});

test('Errors in single arg messages are converted to JSON', async () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
  });

  jest.setSystemTime(1);
  l.log('info', {usr: {name: 'bob'}}, new Error('Test error msg'));

  await l.flush();

  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch.mock.calls[0][0]).toEqual(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
  );
  const request = fetch.mock.calls[0][1];
  expect(request).toBeDefined();
  if (request === undefined) {
    throw new Error('Expect request to be defined');
  }
  expect(request.method).toEqual('POST');
  const {body} = request;
  expect(body).toBeDefined();
  if (!body) {
    throw new Error('Expect body to be defined and non-null');
  }
  const parsedBody = JSON.parse(body.toString());
  expect(parsedBody.date).toEqual(1);
  expect(parsedBody.status).toEqual('info');
  expect(parsedBody.message.name).toEqual('Error');
  expect(parsedBody.message.message).toEqual('Test error msg');
  expect(parsedBody.message.stack).toBeDefined();
});

test('flush calls fetch but includes logs after the error', async () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
  });
  jest.useFakeTimers();
  jest.setSystemTime(3);
  l.log('error', {usr: {name: 'bob'}}, 'error message');
  jest.setSystemTime(4);
  l.log('info', {usr: {name: 'bob'}}, 'info message');

  jest.setSystemTime(10);
  await l.flush();

  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch).toHaveBeenCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
    {
      body: stringifyMany(
        {
          usr: {name: 'bob'},
          date: 3,
          message: 'error message',
          status: 'error',
          error: {origin: 'logger'},
          flushDelayMs: 7,
        },
        {
          usr: {name: 'bob'},
          date: 4,
          message: 'info message',
          status: 'info',
          flushDelayMs: 6,
        },
      ),
      method: 'POST',
      keepalive: true,
    },
  );
});

test('flush is called 1s after a log', async () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
    interval: 1000,
  });

  jest.setSystemTime(3);
  l.log('info', {usr: {name: 'bob'}}, 'info message');
  jest.advanceTimersByTime(1000);

  await microtasksUntil(() => fetch.mock.calls.length >= 1);

  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch).toHaveBeenCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
    {
      body: stringifyMany({
        usr: {name: 'bob'},
        date: 3,
        message: 'info message',
        status: 'info',
        flushDelayMs: 1000,
      }),
      method: 'POST',
      keepalive: true,
    },
  );
});

test('flush is called again in case of failure', async () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
    interval: 1000,
  });

  fetch.mockReturnValue(Promise.reject(new Error('error')));
  jest.setSystemTime(3);
  l.log('info', {usr: {name: 'bob'}}, 'info message');
  jest.advanceTimersByTime(1000);

  await microtasksUntil(() => fetch.mock.calls.length >= 1);

  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch).toHaveBeenCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
    {
      body: stringifyMany({
        usr: {name: 'bob'},
        date: 3,
        message: 'info message',
        status: 'info',
        flushDelayMs: 1000,
      }),
      method: 'POST',
      keepalive: true,
    },
  );

  fetch.mockReturnValue(Promise.resolve(new Response()));
  l.log('info', {usr: {name: 'bob'}}, 'info message 2');
  jest.advanceTimersByTime(1000);

  await microtasksUntil(() => fetch.mock.calls.length >= 2);

  expect(fetch).toHaveBeenCalledTimes(2);
  expect(fetch).toHaveBeenLastCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
    {
      body: stringifyMany(
        {
          usr: {name: 'bob'},
          date: 3,
          message: 'info message',
          status: 'info',
          flushDelayMs: 2000,
          flushRetryCount: 1,
        },
        {
          usr: {name: 'bob'},
          date: 1003,
          message: 'info message 2',
          status: 'info',
          flushDelayMs: 1000,
        },
      ),
      method: 'POST',
      keepalive: true,
    },
  );
});

test('messages that fail to send 3 times are dropped', async () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
    interval: 1000,
  });

  fetch.mockReturnValue(Promise.reject(new Error('error')));
  jest.setSystemTime(3);
  l.log('info', {usr: {name: 'bob'}}, 'info message');
  l.log('info', {usr: {name: 'bob'}}, 'info message 2');
  jest.advanceTimersByTime(1000);

  await microtasksUntil(() => fetch.mock.calls.length >= 1);

  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch).toHaveBeenCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
    {
      body: stringifyMany(
        {
          usr: {name: 'bob'},
          date: 3,
          message: 'info message',
          status: 'info',
          flushDelayMs: 1000,
        },
        {
          usr: {name: 'bob'},
          date: 3,
          message: 'info message 2',
          status: 'info',
          flushDelayMs: 1000,
        },
      ),
      method: 'POST',
      keepalive: true,
    },
  );

  l.log('info', {usr: {name: 'bob'}}, 'info message 3');
  jest.advanceTimersByTime(1000);

  await microtasksUntil(() => fetch.mock.calls.length >= 2);

  expect(fetch).toHaveBeenCalledTimes(2);
  expect(fetch).toHaveBeenLastCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
    {
      body: stringifyMany(
        {
          usr: {name: 'bob'},
          date: 3,
          message: 'info message',
          status: 'info',
          flushDelayMs: 2000,
          flushRetryCount: 1,
        },
        {
          usr: {name: 'bob'},
          date: 3,
          message: 'info message 2',
          status: 'info',
          flushDelayMs: 2000,
          flushRetryCount: 1,
        },
        {
          usr: {name: 'bob'},
          date: 1003,
          message: 'info message 3',
          status: 'info',
          flushDelayMs: 1000,
        },
      ),
      method: 'POST',
      keepalive: true,
    },
  );

  l.log('info', {usr: {name: 'bob'}}, 'info message 4');
  jest.advanceTimersByTime(1000);

  await microtasksUntil(() => fetch.mock.calls.length >= 3);

  expect(fetch).toHaveBeenCalledTimes(3);
  expect(fetch).toHaveBeenLastCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
    {
      body: stringifyMany(
        {
          usr: {name: 'bob'},
          date: 3,
          message: 'info message',
          status: 'info',
          flushDelayMs: 3000,
          flushRetryCount: 2,
        },
        {
          usr: {name: 'bob'},
          date: 3,
          message: 'info message 2',
          status: 'info',
          flushDelayMs: 3000,
          flushRetryCount: 2,
        },
        {
          usr: {name: 'bob'},
          date: 1003,
          message: 'info message 3',
          status: 'info',
          flushDelayMs: 2000,
          flushRetryCount: 1,
        },
        {
          usr: {name: 'bob'},
          date: 2003,
          message: 'info message 4',
          status: 'info',
          flushDelayMs: 1000,
        },
      ),
      method: 'POST',
      keepalive: true,
    },
  );

  fetch.mockReturnValue(Promise.resolve(new Response()));
  l.log('info', {usr: {name: 'bob'}}, 'info message 5');
  jest.advanceTimersByTime(1000);

  await microtasksUntil(() => fetch.mock.calls.length >= 4);

  expect(fetch).toHaveBeenCalledTimes(4);
  expect(fetch).toHaveBeenLastCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
    {
      body: stringifyMany(
        {
          usr: {name: 'bob'},
          date: 1003,
          message: 'info message 3',
          status: 'info',
          flushDelayMs: 3000,
          flushRetryCount: 2,
        },
        {
          usr: {name: 'bob'},
          date: 2003,
          message: 'info message 4',
          status: 'info',
          flushDelayMs: 2000,
          flushRetryCount: 1,
        },
        {
          usr: {name: 'bob'},
          date: 3003,
          message: 'info message 5',
          status: 'info',
          flushDelayMs: 1000,
        },
      ),
      method: 'POST',
      keepalive: true,
    },
  );
});

async function microtasksUntil(p: () => boolean) {
  for (let i = 0; i < 100; i++) {
    if (p()) {
      return;
    }
    await 'microtask';
  }
}
