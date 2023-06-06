import type {LogContext} from '@rocicorp/logger';
import {Series, DISTRIBUTION_METRIC_TYPE} from '../types/report-metrics.js';
import {default as datadog} from 'datadog-metrics';

export type DatadogMetricsSinkOptions = {
  apiKey: string;
  service?: string | undefined;
};

export function createDatadogMetricsSink(options: DatadogMetricsSinkOptions) {
  return async (allSeries: Series[], lc: LogContext) => {
    const reporter = new datadog.reporters.DatadogReporter(options.apiKey);
    const series = allSeries.map(s => ({
      ...s,
      tags: [...(s.tags ?? []), `service:${options.service}`],
      type: s.type ?? DISTRIBUTION_METRIC_TYPE, // Backwards compatibility
    }));

    lc.debug?.('Reporting metrics to Datadog', {
      series: JSON.stringify(series),
    });

    await new Promise((resolve, reject) => {
      reporter.report(series, resolve, reject);
    });
  };
}
