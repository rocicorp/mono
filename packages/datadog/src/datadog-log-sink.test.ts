import {jest, afterEach, beforeEach, test, expect} from '@jest/globals';
import type {ReadonlyJSONObject} from 'replicache';
import {DatadogLogSink} from './datadog-log-sink.js';
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

  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch).toHaveBeenCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
    {
      body: stringifyMany(
        {
          date: 1,
          message: 'debug message',
          status: 'debug',
        },
        {date: 2, message: 'info message', status: 'info'},
      ),
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
  l.log('info', 'Logging an error', new Error('Test error msg'), 'after');

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
  l.log('info', new Error('Test error msg'));

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

  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch).toHaveBeenCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
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
  l.log('info', 'info message');
  jest.advanceTimersByTime(1000);

  await microtasksUntil(() => fetch.mock.calls.length >= 1);

  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch).toHaveBeenCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
    {
      body: stringifyMany({date: 3, message: 'info message', status: 'info'}),
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
  l.log('info', 'info message');
  jest.advanceTimersByTime(1000);

  await microtasksUntil(() => fetch.mock.calls.length >= 1);

  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch).toHaveBeenCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
    {
      body: stringifyMany({date: 3, message: 'info message', status: 'info'}),
      method: 'POST',
      keepalive: true,
    },
  );

  fetch.mockReturnValue(Promise.resolve(new Response('{}')));
  l.log('info', 'info message 2');
  jest.advanceTimersByTime(1000);

  await microtasksUntil(() => fetch.mock.calls.length >= 2);

  expect(fetch).toHaveBeenCalledTimes(2);
  expect(fetch).toHaveBeenLastCalledWith(
    'https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=apiKey',
    {
      body: stringifyMany(
        {date: 3, message: 'info message', status: 'info'},
        {date: 1003, message: 'info message 2', status: 'info'},
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
