import type {LogContext} from '@rocicorp/logger';
import {
  DISTRIBUTION_METRIC_TYPE,
  type Series,
} from '../types/report-metrics.js';

export type DatadogMetricsSinkOptions = {
  apiKey: string;
  service?: string | undefined;
  tags?: Record<string, string>;
};

export function createDatadogMetricsSink(options: DatadogMetricsSinkOptions) {
  return async (allSeries: Series[], lc: LogContext) => {
    const distributions = [];
    const series = [];

    for (const metric of allSeries) {
      if (
        metric.type === undefined ||
        metric.type === DISTRIBUTION_METRIC_TYPE
      ) {
        distributions.push(metric);
      } else {
        series.push(metric);
      }
    }

    await Promise.all([
      report('series', series, lc, options),
      report('distribution_points', distributions, lc, options),
    ]);
  };
}

async function report(
  resource: 'distribution_points' | 'series',
  series: Series[],
  lc: LogContext,
  options: DatadogMetricsSinkOptions,
) {
  if (series.length === 0) {
    return;
  }

  const tags = {
    ...(options.tags ?? {}),
    ...(options.service ? {service: options.service} : {}),
  };

  const body = JSON.stringify({
    series: series.map(s => ({
      ...s,
      tags: [
        ...(s.tags ?? []),
        ...Object.entries(tags).map(([key, value]) => `${key}:${value}`),
      ],
      type: s.type,
    })),
  });
  lc.debug?.(`Reporting ${resource} to Datadog`, {body});
  const resp = await fetch(`https://api.datadoghq.com/api/v1/${resource}`, {
    method: 'POST',
    headers: {
      'DD-API-KEY': options.apiKey,
    },
    body,
  });
  if (!resp.ok) {
    throw new Error(
      `Failed to report metrics to Datadog: ${resp.status} ${resp.statusText}. Dropping metrics on the floor.`,
    );
  }
}
