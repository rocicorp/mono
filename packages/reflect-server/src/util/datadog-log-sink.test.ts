import {jest, afterEach, beforeEach, test, expect} from '@jest/globals';
import type {SpyInstance} from 'jest-mock';
import type {ReadonlyJSONObject} from 'replicache';
import {DatadogLogSink} from './datadog-log-sink.js';

let fetchSpy: SpyInstance<typeof fetch>;

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
  fetchSpy = jest
    .spyOn(globalThis, 'fetch')
    .mockReturnValue(Promise.resolve(new Response('{}')));
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
  l.log('error', 'aaa');
  expect(flushSpy).toHaveBeenCalledTimes(1);
});

test('flush calls fetch', async () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
  });
  jest.setSystemTime(1);
  l.log('debug', 'debug message');
  jest.setSystemTime(2);
  l.log('info', 'info message');

  await l.flush();

  expect(fetchSpy).toHaveBeenCalledTimes(1);
  expect(fetchSpy).toHaveBeenCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?ddsource=worker',
    {
      body: stringifyMany(
        {
          date: 1,
          message: 'debug message',
          status: 'debug',
        },
        {date: 2, message: 'info message', status: 'info'},
      ),
      // eslint-disable-next-line @typescript-eslint/naming-convention
      headers: {'DD-API-KEY': 'apiKey'},
      method: 'POST',
    },
  );
});

test('Errors in multi arg messages are converted to JSON', async () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
  });

  jest.setSystemTime(1);
  l.log('info', 'Logging an error', new Error('Test error msg'), 'after');

  await l.flush();

  expect(fetchSpy).toHaveBeenCalledTimes(1);
  expect(fetchSpy.mock.calls[0][0]).toEqual(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?ddsource=worker',
  );
  const request = fetchSpy.mock.calls[0][1];
  expect(request).toBeDefined();
  if (request === undefined) {
    throw new Error('Expect request to be defined');
  }
  // eslint-disable-next-line @typescript-eslint/naming-convention
  expect(request.headers).toEqual({'DD-API-KEY': 'apiKey'});
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
  l.log('info', new Error('Test error msg'));

  await l.flush();

  expect(fetchSpy).toHaveBeenCalledTimes(1);
  expect(fetchSpy.mock.calls[0][0]).toEqual(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?ddsource=worker',
  );
  const request = fetchSpy.mock.calls[0][1];
  expect(request).toBeDefined();
  if (request === undefined) {
    throw new Error('Expect request to be defined');
  }
  // eslint-disable-next-line @typescript-eslint/naming-convention
  expect(request.headers).toEqual({'DD-API-KEY': 'apiKey'});
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

test('flush calls fetch but includes logs efter the error', async () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
  });
  jest.useFakeTimers();
  jest.setSystemTime(3);
  l.log('error', 'error message');
  jest.setSystemTime(4);
  l.log('info', 'info message');

  await l.flush();

  expect(fetchSpy).toHaveBeenCalledTimes(1);
  expect(fetchSpy).toHaveBeenCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?ddsource=worker',
    {
      body: stringifyMany(
        {
          date: 3,
          message: 'error message',
          status: 'error',
          error: {origin: 'logger'},
        },
        {date: 4, message: 'info message', status: 'info'},
      ),

      // eslint-disable-next-line @typescript-eslint/naming-convention
      headers: {'DD-API-KEY': 'apiKey'},
      method: 'POST',
    },
  );
});

test('flush is called 1s after a log', async () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
    interval: 1000,
  });

  jest.setSystemTime(3);
  l.log('info', 'info message');
  jest.advanceTimersByTime(1000);

  await microtasksUntil(() => fetchSpy.mock.calls.length >= 1);

  expect(fetchSpy).toHaveBeenCalledTimes(1);
  expect(fetchSpy).toHaveBeenCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?ddsource=worker',
    {
      body: stringifyMany({date: 3, message: 'info message', status: 'info'}),
      // eslint-disable-next-line @typescript-eslint/naming-convention
      headers: {'DD-API-KEY': 'apiKey'},
      method: 'POST',
    },
  );
});

test('flush is called again in case of failure', async () => {
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
    interval: 1000,
  });

  fetchSpy.mockReturnValue(Promise.reject(new Error('error')));
  jest.setSystemTime(3);
  l.log('info', 'info message');
  jest.advanceTimersByTime(1000);

  await microtasksUntil(() => fetchSpy.mock.calls.length >= 1);

  expect(fetchSpy).toHaveBeenCalledTimes(1);
  expect(fetchSpy).toHaveBeenCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?ddsource=worker',
    {
      body: stringifyMany({date: 3, message: 'info message', status: 'info'}),
      // eslint-disable-next-line @typescript-eslint/naming-convention
      headers: {'DD-API-KEY': 'apiKey'},
      method: 'POST',
    },
  );

  fetchSpy.mockReturnValue(Promise.resolve(new Response('{}')));
  l.log('info', 'info message 2');
  jest.advanceTimersByTime(1000);

  await microtasksUntil(() => fetchSpy.mock.calls.length >= 2);

  expect(fetchSpy).toHaveBeenCalledTimes(2);
  expect(fetchSpy).toHaveBeenLastCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?ddsource=worker',
    {
      body: stringifyMany(
        {date: 3, message: 'info message', status: 'info'},
        {date: 1003, message: 'info message 2', status: 'info'},
      ),
      // eslint-disable-next-line @typescript-eslint/naming-convention
      headers: {'DD-API-KEY': 'apiKey'},
      method: 'POST',
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

test('aborting stops timers', async () => {
  const ac = new AbortController();
  const l = new DatadogLogSink({
    apiKey: 'apiKey',
    interval: 1000,
    signal: ac.signal,
  });

  jest.setSystemTime(3);
  l.log('info', 'info message');
  ac.abort();

  jest.advanceTimersByTime(1000);

  let i = 0;
  await microtasksUntil(() => i++ > 100);

  expect(fetchSpy).toHaveBeenCalledTimes(0);
});
