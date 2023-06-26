import * as v from 'shared/valita.js';

export const pointSchema = v.tuple([v.number(), v.number()]);
export const pointsSchema = v.tuple([v.number(), v.array(v.number())]);

export type Point = v.Infer<typeof pointSchema>;
export type Points = v.Infer<typeof pointsSchema>;

const baseMetricFields = {
  host: v.string().optional(),
  metric: v.string(),
  tags: v.array(v.string()).optional(),
};

// https://docs.datadoghq.com/api/latest/metrics/#submit-metrics (v1)
export const COUNT_METRIC_TYPE = 'count';
export const RATE_METRIC_TYPE = 'rate';
export const GAUGE_METRIC_TYPE = 'gauge';
export const nonDistributionSchema = v.object({
  ...baseMetricFields,
  type: v.union(
    v.literal(COUNT_METRIC_TYPE),
    v.literal(RATE_METRIC_TYPE),
    v.literal(GAUGE_METRIC_TYPE),
  ),
  points: v.array(pointSchema),
});

// https://docs.datadoghq.com/api/latest/metrics/#submit-distribution-points
export const DISTRIBUTION_METRIC_TYPE = 'distribution';
export const distributionSchema = v.object({
  ...baseMetricFields,
  // Backwards compatible with clients that do not specify a type.
  type: v.literal(DISTRIBUTION_METRIC_TYPE).optional(),
  points: v.array(pointsSchema),
});

export const seriesSchema = v.union(distributionSchema, nonDistributionSchema);
export type Series = v.Infer<typeof seriesSchema>;

export const reportMetricsSchema = v.object({
  series: v.array(seriesSchema),
});
export type ReportMetrics = v.Infer<typeof reportMetricsSchema>;
