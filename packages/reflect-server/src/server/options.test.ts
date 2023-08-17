import {describe, test, expect} from '@jest/globals';
import {TestLogSink} from '../util/test-utils.js';
import {
  DataDogLogEnv,
  DataDogMetricsEnv,
  FilteredLogSink,
  LogFilterEnv,
  LogLevelEnv,
  datadogLogging,
  datadogMetrics,
  defaultConsoleLogSink,
  logFilter,
  logLevel,
  newOptionsBuilder,
} from './options.js';
import {LogContext, consoleLogSink} from '@rocicorp/logger';
import type {ReflectServerOptions} from './reflect.js';
import {DatadogLogSink} from 'datadog';

describe('options', () => {
  test('LogSink filtering', () => {
    const dest = new TestLogSink();
    const sink = new FilteredLogSink(
      dest,
      (level, ctx) => level === 'error' || ctx?.['vis'] === 'app',
    );

    const logger = new LogContext('debug', undefined, sink);
    logger.error?.('Errors should be logged regardless of context');
    logger.info?.('Info is not logged without context.');
    logger
      .withContext('vis', 'internal')
      .info?.('Info is not logged for vis=internal');
    logger.withContext('vis', 'app').info?.('vis=app Info should be logged');

    expect(dest.messages).toEqual([
      ['error', undefined, ['Errors should be logged regardless of context']],
      ['info', {vis: 'app'}, ['vis=app Info should be logged']],
    ]);
  });

  // eslint-disable-next-line @typescript-eslint/ban-types
  type EmptyMutators = {};
  const baseOptions: ReflectServerOptions<EmptyMutators> = {
    mutators: {},
    logLevel: 'debug',
  };

  test('defaultConsoleLogSink', () => {
    let options = newOptionsBuilder(() => baseOptions)
      .add(defaultConsoleLogSink())
      .build()({});
    expect(options.logSinks).toEqual([consoleLogSink]);
    // Original options are unchanged.
    expect(baseOptions.logSinks).toBeUndefined;

    const existingLogSink = new TestLogSink();
    options = newOptionsBuilder(() => ({
      ...baseOptions,
      logSinks: [existingLogSink],
    }))
      .add(defaultConsoleLogSink())
      .build()({});
    expect(options.logSinks).toEqual([existingLogSink]);
  });

  test('filterLogs', () => {
    const testLogSink = new TestLogSink();
    const build = newOptionsBuilder<LogFilterEnv, EmptyMutators>(() => ({
      ...baseOptions,
      logSinks: [testLogSink],
    }))
      .add(
        logFilter((level, ctx) => level === 'error' || ctx?.['vis'] === 'app'),
      )
      .build();
    let options = build({});
    expect(options.logSinks).toHaveLength(1);
    let logger = new LogContext('debug', undefined, options.logSinks?.[0]);
    logger.error?.('Errors should be logged regardless of context');
    logger.info?.('Info is not logged without context.');
    logger
      .withContext('vis', 'internal')
      .info?.('Info is not logged for vis=internal');
    logger.withContext('vis', 'app').info?.('vis=app Info should be logged');

    expect(testLogSink.messages).toEqual([
      ['error', undefined, ['Errors should be logged regardless of context']],
      ['info', {vis: 'app'}, ['vis=app Info should be logged']],
    ]);

    testLogSink.messages = [];
    // eslint-disable-next-line @typescript-eslint/naming-convention
    options = build({DISABLE_LOG_FILTERING: '1'});
    expect(options.logSinks).toHaveLength(1);
    logger = new LogContext('debug', undefined, options.logSinks?.[0]);
    logger.error?.('Errors should be logged regardless of context');
    logger.info?.('Info is no longer filtered');
    logger
      .withContext('vis', 'internal')
      .info?.('Context is no longer filtered');
    logger.withContext('vis', 'app').info?.('vis=app Info should be logged');

    expect(testLogSink.messages).toEqual([
      ['error', undefined, ['Errors should be logged regardless of context']],
      ['info', undefined, ['Info is no longer filtered']],
      ['info', {vis: 'internal'}, ['Context is no longer filtered']],
      ['info', {vis: 'app'}, ['vis=app Info should be logged']],
    ]);
  });

  test('logLevel', () => {
    const build = newOptionsBuilder<LogLevelEnv, EmptyMutators>(() => ({
      ...baseOptions,
      logLevel: 'debug',
    }))
      .add(logLevel())
      .build();

    expect(build({}).logLevel).toBe('info');
    // eslint-disable-next-line @typescript-eslint/naming-convention
    expect(build({LOG_LEVEL: 'error'}).logLevel).toBe('error');
  });

  test('datadogLogging', () => {
    const build = newOptionsBuilder<DataDogLogEnv, EmptyMutators>(
      () => baseOptions,
    )
      .add(datadogLogging('my-service'))
      .build();

    expect(build({}).logSinks).toBeUndefined;
    expect(
      // eslint-disable-next-line @typescript-eslint/naming-convention
      build({DATADOG_LOGS_API_KEY: 'foobar'}).logSinks?.[0],
    ).toBeInstanceOf(DatadogLogSink);
  });

  test('datadogMetrics', () => {
    const build = newOptionsBuilder<DataDogMetricsEnv, EmptyMutators>(
      () => baseOptions,
    )
      .add(datadogMetrics('my-service'))
      .build();

    expect(build({}).datadogMetricsOptions).toBeUndefined;
    expect(
      // eslint-disable-next-line @typescript-eslint/naming-convention
      build({DATADOG_METRICS_API_KEY: 'foobar'}).datadogMetricsOptions,
    ).toEqual({
      apiKey: 'foobar',
      service: 'my-service',
    });
  });
});
