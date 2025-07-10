export type MetricMap = {
  'query-materialization-client': [queryID: string];
};

export interface MetricsDelegate {
  addMetric<K extends keyof MetricMap>(
    metric: K,
    value: number,
    ...args: MetricMap[K]
  ): void;
}
