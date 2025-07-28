import type {
  Counter,
  Histogram,
  Meter,
  MetricOptions,
  ObservableGauge,
  UpDownCounter,
} from '@opentelemetry/api';
import {metrics} from '@opentelemetry/api';

// intentional lazy initialization so it is not started before the SDK is started.

export type Category = 'replication' | 'sync' | 'mutation' | 'server';

let meter: Meter | undefined;

type Options = MetricOptions & {description: string};
type OptionsWithUnit = MetricOptions & {description: string; unit: string};

function getMeter() {
  if (!meter) {
    meter = metrics.getMeter('zero');
  }
  return meter;
}

function cache<TRet>(): (
  name: string,
  creator: (name: string) => TRet,
) => TRet {
  const instruments = new Map<string, TRet>();
  return (name: string, creator: (name: string) => TRet) => {
    const existing = instruments.get(name);
    if (existing) {
      return existing;
    }

    const ret = creator(name);
    instruments.set(name, ret);
    return ret;
  };
}

const upDownCounters = cache<UpDownCounter>();

export function getOrCreateUpDownCounter(
  category: Category,
  name: string,
  description: string,
): UpDownCounter;
export function getOrCreateUpDownCounter(
  category: Category,
  name: string,
  opts: Options,
): UpDownCounter;
export function getOrCreateUpDownCounter(
  category: Category,
  name: string,
  opts: string | Options,
): UpDownCounter {
  return upDownCounters(name, name =>
    getMeter().createUpDownCounter(
      `zero.${category}.${name}`,
      typeof opts === 'string' ? {description: opts} : opts,
    ),
  );
}

const histograms = cache<Histogram>();

export function getOrCreateHistogram(
  category: Category,
  name: string,
  description: string,
): Histogram;
export function getOrCreateHistogram(
  category: Category,
  name: string,
  options: OptionsWithUnit,
): Histogram;
export function getOrCreateHistogram(
  category: Category,
  name: string,
  opts: string | OptionsWithUnit,
): Histogram {
  return histograms(name, name => {
    const options: {description: string; unit: string; boundaries?: number[]} =
      typeof opts === 'string'
        ? {
            description: opts,
            unit: 'milliseconds',
          }
        : opts;

    return getMeter().createHistogram(`zero.${category}.${name}`, options);
  });
}

const counters = cache<Counter>();

export function getOrCreateCounter(
  category: Category,
  name: string,
  description: string,
): Counter;
export function getOrCreateCounter(
  category: Category,
  name: string,
  opts: Options,
): Counter;
export function getOrCreateCounter(
  category: Category,
  name: string,
  opts: string | Options,
): Counter {
  return counters(name, name =>
    getMeter().createCounter(
      `zero.${category}.${name}`,
      typeof opts === 'string' ? {description: opts} : opts,
    ),
  );
}

const gauges = cache<ObservableGauge>();

export function getOrCreateGauge(
  category: Category,
  name: string,
  description: string,
): ObservableGauge;
export function getOrCreateGauge(
  category: Category,
  name: string,
  opts: Options,
): ObservableGauge;
export function getOrCreateGauge(
  category: Category,
  name: string,
  opts: string | Options,
): ObservableGauge {
  return gauges(name, name =>
    getMeter().createObservableGauge(
      `zero.${category}.${name}`,
      typeof opts === 'string' ? {description: opts} : opts,
    ),
  );
}
