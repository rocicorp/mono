/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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
