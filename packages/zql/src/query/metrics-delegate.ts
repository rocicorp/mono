import type {AST} from '../../../zero-protocol/src/ast.ts';

export type ClientMetricMap = {
  'query-materialization-client': [queryID: string];
  'query-materialization-end-to-end': [queryID: string, ast: AST];
  'query-update-client': [queryID: string];
};

export type ServerMetricMap = {
  'query-materialization-server': [queryID: string];
  'query-update-server': [queryID: string];
};

export type MetricMap = ClientMetricMap & ServerMetricMap;

export interface MetricsDelegate {
  addMetric<K extends keyof MetricMap>(
    metric: K,
    value: number,
    ...args: MetricMap[K]
  ): void;
}

export function isClientMetric(
  metric: keyof MetricMap,
): metric is keyof ClientMetricMap {
  return metric.endsWith('-client') || metric.endsWith('-end-to-end');
}

export function isServerMetric(
  metric: keyof MetricMap,
): metric is keyof ServerMetricMap {
  return metric.endsWith('-server');
}
