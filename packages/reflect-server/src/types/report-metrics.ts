import * as s from 'superstruct';

const datadogPointSchema = s.tuple([s.number(), s.array(s.number())]);

const datadogSeriesSchema = s.type({
  metric: s.string(),
  points: s.array(datadogPointSchema),
});

export const reportMetricsSchema = s.type({
  series: s.array(datadogSeriesSchema),
});

export type ReportMetrics = s.Infer<typeof reportMetricsSchema>;
