import {afterEach, beforeEach, expect, jest, test} from '@jest/globals';
import {resolver} from '@rocicorp/resolver';
import type {TailMessage} from 'mirror-protocol/src/tail.js';
import assert from 'node:assert';
import {makeRequester} from '../requester.js';
import {createTailEventSource} from './tail-event-source.js';

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});

afterEach(() => {
  jest.restoreAllMocks();
});

test('Error in response should be handled', async () => {
  jest.spyOn(globalThis, 'fetch').mockImplementation(() =>
    Promise.resolve({
      ok: false,
      status: 555,
      statusText: 'Error in test',
    } as Response),
  );

  const src = createTailEventSource(
    'test-tail',
    'app-id',
    'api-token',
    {
      requester: makeRequester('user-id'),
    },
    'http://localhost:8080/test-tail',
  );

  let err: Error | undefined;
  try {
    for await (const _ of src) {
      console.log('for await');
    }
  } catch (e) {
    err = e as Error;
  }
  expect(err).toBeInstanceOf(Error);
  expect(err?.message).toBe('HTTP 555 Error in test');
});

test('Streaming data should emit messages', async () => {
  let controller: ReadableStreamDefaultController<string> | undefined;
  const controllerResolver = resolver<void>();

  const stringStream = new ReadableStream<string>({
    start(c) {
      controllerResolver.resolve();
      controller = c;
    },
  });
  const body = stringStream.pipeThrough(new TextEncoderStream());

  const fetchSpy = jest.spyOn(globalThis, 'fetch').mockImplementation(() =>
    Promise.resolve({
      ok: true,
      status: 200,
      body,
    } as Response),
  );

  const src = createTailEventSource(
    'test-tail',
    'app-id',
    'api-token',
    {
      requester: makeRequester('user-id'),
    },
    'http://localhost:8080/test-tail',
  );

  await controllerResolver.promise;
  assert(controller);

  function enqueue(data: TailMessage) {
    assert(controller);
    controller.enqueue(`data: ${JSON.stringify(data)}\n\n`);
  }

  const iter = src[Symbol.asyncIterator]();

  {
    const data: TailMessage = {type: 'log', level: 'info', message: ['foo']};
    enqueue(data);
    expect(await iter.next()).toEqual({done: false, value: data});
  }

  expect(fetchSpy).toHaveBeenCalledTimes(1);
  const {lastCall} = fetchSpy.mock;
  assert(lastCall);
  const {signal} = lastCall[1] as RequestInit;
  assert(signal);
  expect(signal.aborted).toBe(false);

  {
    const data: TailMessage = {
      type: 'log',
      level: 'error',
      message: [1, true, [], {}],
    };
    enqueue(data);
    expect(await iter.next()).toEqual({done: false, value: data});
  }

  expect(fetchSpy).toHaveBeenCalledTimes(1);
  expect(signal.aborted).toBe(false);

  {
    assert(iter.return);
    expect(await iter.return()).toEqual({done: true, value: undefined});
  }

  expect(signal.aborted).toBe(true);
});
