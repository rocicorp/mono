// Types for https://github.com/yunyu/parse-prometheus-text-format base on:
// https://github.com/yunyu/parse-prometheus-text-format/?tab=readme-ov-file#example
declare module 'parse-prometheus-text-format' {
  type Metric = {labels?: Record<string, string>};
  type Counter = Metric & {value: string};
  type Gauge = Metric & {value: string};
  type Untyped = Metric & {value: string};
  type Histogram = Metric & {
    buckets: Record<string, number>;
    count: string;
    sum: string;
  };
  type Summary = Metric & {
    quantiles?: Record<string, number>;
    count: string;
    sum: string;
  };

  type MetricFamilyDesc = {
    name: string;
    help: string;
  };
  type GaugeFamily = MetricFamilyDesc & {
    type: 'GAUGE';
    metrics: Gauge[];
  };
  type CounterFamily = MetricFamilyDesc & {
    type: 'COUNTER';
    metrics: Counter[];
  };
  type UntypedFamily = MetricFamilyDesc & {
    type: 'UNTYPED';
    metrics: Untyped[];
  };
  type HistogramFamily = MetricFamilyDesc & {
    type: 'HISTOGRAM';
    metrics: Histogram[];
  };
  type SummaryFamily = MetricFamilyDesc & {
    type: 'SUMMARY';
    metrics: Summary[];
  };

  type MetricFamily =
    | GaugeFamily
    | CounterFamily
    | UntypedFamily
    | HistogramFamily
    | SummaryFamily;

  export default function parsePrometheusTextFormat(
    text: string,
  ): MetricFamily[];
}
