import type {ReflectServerOptions} from './reflect.js';
import type {MutatorDefs} from 'reflect-types/src/mod.js';
import type {Context, LogLevel, LogSink} from '@rocicorp/logger';
import {consoleLogSink, createWorkerDatadogLogSink} from '../mod.js';

export type BuildableOptionsEnv = LogFilterEnv &
  LogLevelEnv &
  DataDogLogEnv &
  DataDogMetricsEnv;

export function newOptionsBuilder<Env, MD extends MutatorDefs>(
  base: OptionsMaker<Env, MD>,
) {
  return new OptionsBuilder(base);
}

export function defaultConsoleLogSink<
  Env,
  MD extends MutatorDefs,
>(): OptionsAdder<Env, MD> {
  return (options: ReflectServerOptions<MD>) =>
    options.logSinks?.length ?? 0 > 0
      ? options
      : {
          ...options,
          logSinks: [...(options.logSinks ?? []), consoleLogSink],
        };
}

export type LogFilterEnv = {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  DISABLE_LOG_FILTERING?: string;
};

export function logFilter<Env extends LogFilterEnv, MD extends MutatorDefs>(
  include: LogPredicate,
): OptionsAdder<Env, MD> {
  return (options, env) => {
    switch ((env.DISABLE_LOG_FILTERING ?? '0').toLowerCase()) {
      case 'true':
      case '1':
        return options;
    }
    const numLogSinks = options.logSinks?.length ?? 0;
    if (numLogSinks === 0) {
      return options;
    }
    const newOptions = {
      ...options,
      logSinks: options.logSinks?.map(
        sink => new FilteredLogSink(sink, include),
      ),
    };
    return newOptions;
  };
}

export type LogLevelEnv = {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  LOG_LEVEL?: string;
};

export function logLevel<Env extends LogLevelEnv, MD extends MutatorDefs>(
  defaultLogLevel: LogLevel = 'info',
): OptionsAdder<Env, MD> {
  return (options: ReflectServerOptions<MD>, env: Env) => {
    switch (env.LOG_LEVEL) {
      case 'info':
      case 'debug':
      case 'error':
        return {...options, logLevel: env.LOG_LEVEL};
      case undefined:
        break;
      default:
        throw new Error(`Invalid value for LOG_LEVEL: ${env.LOG_LEVEL}`);
    }
    return {...options, logLevel: defaultLogLevel};
  };
}

export type DataDogLogEnv = {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  DATADOG_LOGS_API_KEY?: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  DATADOG_SERVICE_LABEL?: string;
};

export function datadogLogging<
  Env extends DataDogLogEnv,
  MD extends MutatorDefs,
>(defaultServiceLabel: string): OptionsAdder<Env, MD> {
  return (options, env) => {
    if (env.DATADOG_LOGS_API_KEY === undefined) {
      console.warn(
        'Not enabling datadog logging because env.DATADOG_LOGS_API_KEY is undefined',
      );
      return options;
    }
    const logSink = createWorkerDatadogLogSink({
      apiKey: env.DATADOG_LOGS_API_KEY,
      service: env.DATADOG_SERVICE_LABEL ?? defaultServiceLabel,
    });
    return {...options, logSinks: [...(options.logSinks ?? []), logSink]};
  };
}

export type DataDogMetricsEnv = {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  DATADOG_METRICS_API_KEY?: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  DATADOG_SERVICE_LABEL?: string;
};

export function datadogMetrics<
  Env extends DataDogMetricsEnv,
  MD extends MutatorDefs,
>(defaultServiceLabel: string): OptionsAdder<Env, MD> {
  return (options, env) => {
    if (env.DATADOG_METRICS_API_KEY === undefined) {
      console.warn(
        'Not enabling datadog metrics because env.DATADOG_METRICS_API_KEY is undefined',
      );
      return options;
    }
    return {
      ...options,
      datadogMetricsOptions: {
        apiKey: env.DATADOG_METRICS_API_KEY,
        service: env.DATADOG_SERVICE_LABEL ?? defaultServiceLabel,
      },
    };
  };
}

type LogPredicate = (
  level: LogLevel,
  context: Context | undefined,
  ...args: unknown[]
) => boolean;

// TODO: Consider moving into @rocicorp/logger
export class FilteredLogSink implements LogSink {
  readonly #sink: LogSink;
  readonly #include: LogPredicate;

  constructor(sink: LogSink, include: LogPredicate) {
    this.#sink = sink;
    this.#include = include;
  }

  log(level: LogLevel, context: Context | undefined, ...args: unknown[]): void {
    if (this.#include(level, context, ...args)) {
      this.#sink.log(level, context, ...args);
    }
  }

  async flush(): Promise<void> {
    await this.#sink.flush?.();
  }
}

type OptionsMaker<Env, MD extends MutatorDefs> = (
  env: Env,
) => ReflectServerOptions<MD>;

type OptionsAdder<Env, MD extends MutatorDefs> = (
  options: ReflectServerOptions<MD>,
  env: Env,
) => ReflectServerOptions<MD>;

class OptionsBuilder<Env, MD extends MutatorDefs> {
  readonly #base: OptionsMaker<Env, MD>;
  readonly #adders: OptionsAdder<Env, MD>[] = [];

  constructor(base: OptionsMaker<Env, MD>) {
    this.#base = base;
  }

  add(adder: OptionsAdder<Env, MD>): OptionsBuilder<Env, MD> {
    this.#adders.push(adder);
    return this;
  }

  build(): OptionsMaker<Env, MD> {
    return env => {
      let options = this.#base(env);
      for (const add of this.#adders) {
        options = add(options, env);
      }
      return options;
    };
  }
}
