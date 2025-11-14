import {consoleLogSink, type LogSink} from '@rocicorp/logger';
import {
  afterEach,
  beforeEach,
  expect,
  suite,
  test,
  vi,
  type Mock,
  type MockInstance,
} from 'vitest';
import type {DatadogLogSinkOptions} from '../../../datadog/src/datadog-log-sink.ts';
import {TestLogSink} from '../../../shared/src/logging-test-utils.ts';
import type {HTTPString} from './http-string.ts';
import {createLogOptions} from './log-options.ts';

type NotUndefined<T> = T extends undefined ? never : T;

type LogSinkSpy = {
  log: MockInstance<LogSink['log']>;
  flush?: MockInstance<NotUndefined<LogSink['flush']>> | undefined;
};

let consoleLogSinkSpy: LogSinkSpy;
let datadogLogSinkSpy: LogSinkSpy;
let fakeCreateDatadogLogSink: Mock<(options: DatadogLogSinkOptions) => LogSink>;

function makeLogSinkSpy(logSink: LogSink): LogSinkSpy {
  const log = vi.spyOn(logSink, 'log');
  let flush;
  if (logSink.flush) {
    flush = vi.spyOn(logSink, 'flush');
  }
  return {log, flush};
}

beforeEach(() => {
  consoleLogSinkSpy = makeLogSinkSpy(consoleLogSink);
  fakeCreateDatadogLogSink = vi.fn((_options: DatadogLogSinkOptions) => {
    const testLogSink = new TestLogSink();
    datadogLogSinkSpy = makeLogSinkSpy(testLogSink);
    return testLogSink;
  });
});

afterEach(() => {
  vi.restoreAllMocks();
});

test('includes datadog sink even when custom log sink provided', () => {
  const customSink = new TestLogSink();
  const {logLevel, logSink} = createLogOptions(
    {
      consoleLogLevel: 'debug',
      logSinks: [customSink],
      server: 'https://example.com',
      enableAnalytics: true,
    },
    fakeCreateDatadogLogSink,
  );
  expect(fakeCreateDatadogLogSink).toHaveBeenCalledOnce();
  expect(logLevel).toBe('debug');

  logSink.log('info', undefined, 'hello');

  expect(customSink.messages).toEqual([['info', undefined, ['hello']]]);
  expect(datadogLogSinkSpy.log).toHaveBeenCalledTimes(1);
  expect(datadogLogSinkSpy.log.mock.calls[0]).toEqual([
    'info',
    undefined,
    'hello',
  ]);
});

test('tees provided log sinks when more than one supplied', () => {
  const sinkA = new TestLogSink();
  const sinkB = new TestLogSink();
  const {logSink} = createLogOptions(
    {
      consoleLogLevel: 'info',
      logSinks: [sinkA, sinkB],
      server: 'https://example.com',
      enableAnalytics: true,
    },
    fakeCreateDatadogLogSink,
  );
  expect(fakeCreateDatadogLogSink).toHaveBeenCalledOnce();
  expect(logSink).not.toBe(sinkA);
  expect(logSink).not.toBe(sinkB);

  logSink.log('info', undefined, 'hello');

  expect(sinkA.messages).toEqual([['info', undefined, ['hello']]]);
  expect(sinkB.messages).toEqual(sinkA.messages);
  expect(datadogLogSinkSpy.log).toHaveBeenCalledTimes(1);
  expect(datadogLogSinkSpy.log.mock.calls[0]).toEqual([
    'info',
    undefined,
    'hello',
  ]);
});

test('uses only datadog sink when custom sinks empty and analytics enabled', () => {
  const {logSink} = createLogOptions(
    {
      consoleLogLevel: 'info',
      logSinks: [],
      server: 'https://example.com',
      enableAnalytics: true,
    },
    fakeCreateDatadogLogSink,
  );
  expect(fakeCreateDatadogLogSink).toHaveBeenCalledOnce();

  logSink.log('info', undefined, 'hello');

  expect(datadogLogSinkSpy.log).toHaveBeenCalledTimes(1);
});

test('flush flushes wrapped datadog sink when analytics enabled', async () => {
  const {logSink} = createLogOptions(
    {
      consoleLogLevel: 'info',
      server: 'https://example.com',
      enableAnalytics: true,
    },
    fakeCreateDatadogLogSink,
  );
  expect(fakeCreateDatadogLogSink).toHaveBeenCalledOnce();
  expect(datadogLogSinkSpy.flush).toBeDefined();

  await logSink.flush?.();

  expect(datadogLogSinkSpy.flush).toHaveBeenCalledTimes(1);
});

function testEnableAnalyticsFalse(server: HTTPString | null) {
  test(`server ${server}, enableAnalytics false`, () => {
    const {logLevel, logSink} = createLogOptions(
      {
        consoleLogLevel: 'info',
        server,
        enableAnalytics: false,
      },
      fakeCreateDatadogLogSink,
    );
    expect(fakeCreateDatadogLogSink).not.toBeCalled();
    expect(logLevel).toBe('info');
    expect(logSink).toBe(consoleLogSink);
  });
}

function testLogLevels(
  server: HTTPString,
  expectedServiceLabel: string,
  expectedBaseURLString: string,
) {
  test('consoleLogLevel debug', () => {
    vi.spyOn(console, 'debug');
    vi.spyOn(console, 'info');
    vi.spyOn(console, 'error');

    const {logLevel, logSink} = createLogOptions(
      {
        consoleLogLevel: 'debug',
        server,
        enableAnalytics: true,
      },
      fakeCreateDatadogLogSink,
    );
    expect(fakeCreateDatadogLogSink).toHaveBeenCalledOnce();
    expect(fakeCreateDatadogLogSink.mock.calls[0][0].service).toBe(
      expectedServiceLabel,
    );
    expect(fakeCreateDatadogLogSink.mock.calls[0][0].baseURL?.toString()).toBe(
      expectedBaseURLString,
    );
    expect(logLevel).toBe('debug');

    logSink.log('debug', {foo: 'bar'}, 'hello');
    logSink.log('info', {foo: 'bar'}, 'world');
    logSink.log('error', {foo: 'bar'}, 'goodbye');

    // debug not logged
    expect(datadogLogSinkSpy.log).toBeCalledTimes(2);
    expect(datadogLogSinkSpy.log.mock.calls[0]).toEqual([
      'info',
      {foo: 'bar'},
      'world',
    ]);
    expect(datadogLogSinkSpy.log.mock.calls[1]).toEqual([
      'error',
      {foo: 'bar'},
      'goodbye',
    ]);

    expect(consoleLogSinkSpy.log).toBeCalledTimes(3);
    expect(consoleLogSinkSpy.log.mock.calls[0]).toEqual([
      'debug',
      {foo: 'bar'},
      'hello',
    ]);
    expect(consoleLogSinkSpy.log.mock.calls[1]).toEqual([
      'info',
      {foo: 'bar'},
      'world',
    ]);
    expect(consoleLogSinkSpy.log.mock.calls[2]).toEqual([
      'error',
      {foo: 'bar'},
      'goodbye',
    ]);
  });

  test('consoleLogLevel info', () => {
    vi.spyOn(console, 'debug');
    vi.spyOn(console, 'info');
    vi.spyOn(console, 'error');

    const {logLevel, logSink} = createLogOptions(
      {
        consoleLogLevel: 'info',
        server,
        enableAnalytics: true,
      },
      fakeCreateDatadogLogSink,
    );
    expect(fakeCreateDatadogLogSink).toBeCalledTimes(1);
    expect(fakeCreateDatadogLogSink.mock.calls[0][0].service).toBe(
      expectedServiceLabel,
    );
    expect(logLevel).toBe('info');

    logSink.log('debug', {foo: 'bar'}, 'hello');
    logSink.log('info', {foo: 'bar'}, 'world');
    logSink.log('error', {foo: 'bar'}, 'goodbye');

    expect(datadogLogSinkSpy.log).toBeCalledTimes(2);
    expect(datadogLogSinkSpy.log.mock.calls[0]).toEqual([
      'info',
      {foo: 'bar'},
      'world',
    ]);
    expect(datadogLogSinkSpy.log.mock.calls[1]).toEqual([
      'error',
      {foo: 'bar'},
      'goodbye',
    ]);

    expect(consoleLogSinkSpy.log).toBeCalledTimes(2);
    expect(consoleLogSinkSpy.log.mock.calls[0]).toEqual([
      'info',
      {foo: 'bar'},
      'world',
    ]);
    expect(consoleLogSinkSpy.log.mock.calls[1]).toEqual([
      'error',
      {foo: 'bar'},
      'goodbye',
    ]);
  });

  test('consoleLogLevel error', () => {
    vi.spyOn(console, 'debug');
    vi.spyOn(console, 'info');
    vi.spyOn(console, 'error');

    const {logLevel, logSink} = createLogOptions(
      {
        consoleLogLevel: 'error',
        server,
        enableAnalytics: true,
      },
      fakeCreateDatadogLogSink,
    );
    expect(fakeCreateDatadogLogSink).toBeCalledTimes(1);
    expect(fakeCreateDatadogLogSink.mock.calls[0][0].service).toBe(
      expectedServiceLabel,
    );
    expect(logLevel).toBe('info');

    logSink.log('debug', {foo: 'bar'}, 'hello');
    logSink.log('info', {foo: 'bar'}, 'world');
    logSink.log('error', {foo: 'bar'}, 'goodbye');

    // info still logged
    expect(datadogLogSinkSpy.log).toBeCalledTimes(2);
    expect(datadogLogSinkSpy.log.mock.calls[0]).toEqual([
      'info',
      {foo: 'bar'},
      'world',
    ]);
    expect(datadogLogSinkSpy.log.mock.calls[1]).toEqual([
      'error',
      {foo: 'bar'},
      'goodbye',
    ]);

    // only error logged
    expect(consoleLogSinkSpy.log).toBeCalledTimes(1);
    expect(consoleLogSinkSpy.log.mock.calls[0]).toEqual([
      'error',
      {foo: 'bar'},
      'goodbye',
    ]);
  });
}

suite('when server is subdomain of .reflect-server.net', () => {
  const server = 'https://testSubdomain.reflect-server.net';
  testLogLevels(
    server,
    'testsubdomain',
    'https://testsubdomain.reflect-server.net/logs/v0/log',
  );
  testEnableAnalyticsFalse(server);
});

suite('when server is not a subdomain of .reflect-server.net', () => {
  const server = 'https://fooBar.FuzzyWuzzy.com';
  testLogLevels(
    server,
    'foobar.fuzzywuzzy.com',
    'https://foobar.fuzzywuzzy.com/logs/v0/log',
  );
  testEnableAnalyticsFalse(server);
});

suite('when server has a path prefix', () => {
  const server = 'https://fooBar.FuzzyWuzzy.com/prefix';
  testLogLevels(
    server,
    'foobar.fuzzywuzzy.com',
    'https://foobar.fuzzywuzzy.com/prefix/logs/v0/log',
  );
  testEnableAnalyticsFalse(server);
});

suite('when server is null', () => {
  const server = null;
  test('datadog logging is disabled', () => {
    const {logLevel, logSink} = createLogOptions(
      {
        consoleLogLevel: 'info',
        server,
        enableAnalytics: true,
      },
      fakeCreateDatadogLogSink,
    );
    expect(fakeCreateDatadogLogSink).toBeCalledTimes(0);
    expect(logLevel).toBe('info');
    expect(logSink).toBe(consoleLogSink);
  });
  testEnableAnalyticsFalse(server);
});
