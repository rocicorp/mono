import * as v from 'shared/valita.js';

export const pointSchema = v.union(
  // count, rate, gauge
  v.tuple([v.number(), v.number()]),
  // distribution
  v.tuple([v.number(), v.array(v.number())]),
);
export type Point = v.Infer<typeof pointSchema>;

// https://docs.datadoghq.com/api/latest/metrics/#submit-metrics (v1)
export const COUNT_METRIC_TYPE = 'count';
export const RATE_METRIC_TYPE = 'rate';
export const GAUGE_METRIC_TYPE = 'gauge';
// https://docs.datadoghq.com/api/latest/metrics/#submit-distribution-points
export const DISTRIBUTION_METRIC_TYPE = 'distribution';

export const metricsTypeSchema = v.union(
  v.literal(COUNT_METRIC_TYPE),
  v.literal(RATE_METRIC_TYPE),
  v.literal(GAUGE_METRIC_TYPE),
  v.literal(DISTRIBUTION_METRIC_TYPE),
);

export const seriesSchema = v.object({
  host: v.string().optional(),
  metric: v.string(),
  points: v.array(pointSchema),
  tags: v.array(v.string()).optional(),
  type: metricsTypeSchema.optional(),
});
export type Series = v.Infer<typeof seriesSchema>;

export const reportMetricsSchema = v.object({
  series: v.array(seriesSchema),
});
export type ReportMetrics = v.Infer<typeof reportMetricsSchema>;
