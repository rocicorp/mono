import type {LogContext} from '@rocicorp/logger';
import type {Series} from '../types/report-metrics.js';

export type DatadogMetricsSinkOptions = {
  apiKey: string;
  service?: string | undefined;
};

export function createDatadogMetricsSink(options: DatadogMetricsSinkOptions) {
  return async (allSeries: Series[], lc: LogContext) => {
    const body = JSON.stringify({
      series: allSeries.map(s => ({
        ...s,
        tags: [...(s.tags ?? []), `service:${options.service}`],
      })),
    });
    lc.debug?.('Reporting metrics to Datadog', {body});
    const resp = await fetch(
      'https://api.datadoghq.com/api/v1/distribution_points',
      {
        method: 'POST',
        headers: {
          'DD-API-KEY': options.apiKey,
        },
        body,
      },
    );
    if (!resp.ok) {
      throw new Error(
        `Failed to report metrics to Datadog: ${resp.status} ${resp.statusText}. Dropping metrics on the floor.`,
      );
    }
  };
}
