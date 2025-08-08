import type {AST} from '../../../zero-protocol/src/ast.ts';

export type MetricMap = {
  'query-materialization-client': [queryID: string];
  'query-materialization-end-to-end': [queryID: string, ast: AST];
  'query-update-client': [queryID: string];
};

export interface MetricsDelegate {
  addMetric<K extends keyof MetricMap>(
    metric: K,
    value: number,
    ...args: MetricMap[K]
  ): void;
}
