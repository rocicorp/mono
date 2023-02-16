import type {Series} from '../types/report-metrics.js';

export type DatadogMetricsSinkOptions = {
  apiKey: string;
  // TODO: Do something with these.
  service?: string;
  host?: string;
};

export function createDatadogMetricsSink(options: DatadogMetricsSinkOptions) {
  return async (allSeries: Series[]) => {
    const body = JSON.stringify({
      series: allSeries,
    });
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
