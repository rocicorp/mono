import * as v from 'shared/valita.js';

const datadogPointSchema = v.tuple([v.number(), v.array(v.number())]);

const datadogSeriesSchema = v.object({
  metric: v.string(),
  points: v.array(datadogPointSchema),
});

export const reportMetricsSchema = v.object({
  series: v.array(datadogSeriesSchema),
});

export type ReportMetrics = v.Infer<typeof reportMetricsSchema>;
