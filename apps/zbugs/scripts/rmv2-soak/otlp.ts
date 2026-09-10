import {createServer, type Server} from 'node:http';
import {gunzipSync} from 'node:zlib';

/**
 * A minimal OTLP/HTTP-JSON metrics receiver.
 *
 * zero-cache already exports every counter, histogram and gauge this soak
 * needs -- the route census (`sqlite_change_log.catchup_routes`), the purge
 * probe outcomes, compare results, log file bytes, backup lag -- through
 * OpenTelemetry. Pointing each task at its own path on this server is a far
 * more faithful source than scraping prose out of log lines, and it needs no
 * collector: `OTEL_EXPORTER_OTLP_PROTOCOL=http/json` posts plain JSON.
 *
 * Both temporalities are handled. Delta points are summed. Cumulative points
 * are held per series and summed across series at read time, where a series
 * is (node, metric, attributes, resource, startTimeUnixNano) -- a restarted
 * process opens a new series rather than appearing to count backwards.
 */

type AnyValue = {
  stringValue?: string;
  boolValue?: boolean;
  intValue?: string | number;
  doubleValue?: number;
};

type KeyValue = {key: string; value?: AnyValue};

type NumberDataPoint = {
  attributes?: KeyValue[];
  startTimeUnixNano?: string | number;
  timeUnixNano?: string | number;
  asInt?: string | number;
  asDouble?: number;
};

type HistogramDataPoint = NumberDataPoint & {
  count?: string | number;
  sum?: number;
  min?: number;
  max?: number;
};

type OtlpMetric = {
  name: string;
  unit?: string;
  sum?: {
    dataPoints?: NumberDataPoint[];
    aggregationTemporality?: number;
    isMonotonic?: boolean;
  };
  gauge?: {dataPoints?: NumberDataPoint[]};
  histogram?: {
    dataPoints?: HistogramDataPoint[];
    aggregationTemporality?: number;
  };
  exponentialHistogram?: {
    dataPoints?: HistogramDataPoint[];
    aggregationTemporality?: number;
  };
};

type ExportRequest = {
  resourceMetrics?: {
    resource?: {attributes?: KeyValue[]};
    scopeMetrics?: {metrics?: OtlpMetric[]}[];
  }[];
};

const AGGREGATION_TEMPORALITY_DELTA = 1;

const METRICS_PATH = /^\/v1\/metrics\/([^/?]+)/;

export type Attributes = Readonly<Record<string, string>>;

/** One (node, metric, attributes) series, aggregated over the run. */
export type MetricSample = {
  readonly node: string;
  readonly name: string;
  readonly attributes: Attributes;
  /** Sum of counter increments, or of histogram sums. */
  sum: number;
  /** Number of histogram observations; 0 for counters and gauges. */
  count: number;
  /** Last observed gauge value. */
  last: number | undefined;
  min: number | undefined;
  max: number | undefined;
  lastSeenMs: number;
};

type CumulativeSeries = {
  key: string;
  value: number;
};

function attrValue(value: AnyValue | undefined): string {
  if (!value) {
    return '';
  }
  if (value.stringValue !== undefined) {
    return value.stringValue;
  }
  if (value.boolValue !== undefined) {
    return String(value.boolValue);
  }
  if (value.intValue !== undefined) {
    return String(value.intValue);
  }
  if (value.doubleValue !== undefined) {
    return String(value.doubleValue);
  }
  return '';
}

function toAttributes(kvs: KeyValue[] | undefined): Attributes {
  const out: Record<string, string> = {};
  for (const kv of kvs ?? []) {
    out[kv.key] = attrValue(kv.value);
  }
  return out;
}

function attrKey(attrs: Attributes): string {
  return Object.keys(attrs)
    .sort()
    .map(k => `${k}=${attrs[k]}`)
    .join(',');
}

function numeric(
  point: NumberDataPoint | HistogramDataPoint,
): number | undefined {
  if (point.asInt !== undefined) {
    return Number(point.asInt);
  }
  if (point.asDouble !== undefined) {
    return point.asDouble;
  }
  if ('sum' in point && point.sum !== undefined) {
    return point.sum;
  }
  return undefined;
}

export class MetricStore {
  readonly #samples = new Map<string, MetricSample>();
  readonly #cumulative = new Map<string, CumulativeSeries>();
  readonly #listeners = new Set<(sample: MetricSample) => void>();

  onSample(listener: (sample: MetricSample) => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  ingest(node: string, body: ExportRequest): void {
    const now = Date.now();
    for (const rm of body.resourceMetrics ?? []) {
      const resourceKey = attrKey(toAttributes(rm.resource?.attributes));
      for (const sm of rm.scopeMetrics ?? []) {
        for (const metric of sm.metrics ?? []) {
          this.#ingestMetric(node, resourceKey, metric, now);
        }
      }
    }
  }

  #ingestMetric(
    node: string,
    resourceKey: string,
    metric: OtlpMetric,
    now: number,
  ): void {
    const histogram = metric.histogram ?? metric.exponentialHistogram;
    if (metric.sum) {
      const delta =
        metric.sum.aggregationTemporality === AGGREGATION_TEMPORALITY_DELTA;
      for (const point of metric.sum.dataPoints ?? []) {
        const value = numeric(point) ?? 0;
        this.#record(node, resourceKey, metric.name, point, now, sample => {
          if (delta) {
            sample.sum += value;
          } else {
            sample.sum += this.#cumulativeDelta(
              node,
              resourceKey,
              metric.name,
              point,
              value,
            );
          }
        });
      }
    }
    if (metric.gauge) {
      for (const point of metric.gauge.dataPoints ?? []) {
        const value = numeric(point);
        this.#record(node, resourceKey, metric.name, point, now, sample => {
          if (value === undefined) {
            return;
          }
          sample.last = value;
          sample.min =
            sample.min === undefined ? value : Math.min(sample.min, value);
          sample.max =
            sample.max === undefined ? value : Math.max(sample.max, value);
        });
      }
    }
    if (histogram) {
      const delta =
        histogram.aggregationTemporality === AGGREGATION_TEMPORALITY_DELTA;
      for (const point of histogram.dataPoints ?? []) {
        const count = Number(point.count ?? 0);
        const sum = point.sum ?? 0;
        this.#record(node, resourceKey, metric.name, point, now, sample => {
          if (delta) {
            sample.sum += sum;
            sample.count += count;
          } else {
            const key = this.#seriesKey(node, resourceKey, metric.name, point);
            const prev = this.#cumulative.get(`${key}#count`);
            const prevSum = this.#cumulative.get(`${key}#sum`);
            sample.count += count - (prev?.value ?? 0);
            sample.sum += sum - (prevSum?.value ?? 0);
            this.#cumulative.set(`${key}#count`, {key, value: count});
            this.#cumulative.set(`${key}#sum`, {key, value: sum});
          }
          if (point.min !== undefined) {
            sample.min =
              sample.min === undefined
                ? point.min
                : Math.min(sample.min, point.min);
          }
          if (point.max !== undefined) {
            sample.max =
              sample.max === undefined
                ? point.max
                : Math.max(sample.max, point.max);
          }
        });
      }
    }
  }

  #seriesKey(
    node: string,
    resourceKey: string,
    name: string,
    point: NumberDataPoint,
  ): string {
    return `${node}|${resourceKey}|${name}|${attrKey(
      toAttributes(point.attributes),
    )}|${point.startTimeUnixNano ?? ''}`;
  }

  #cumulativeDelta(
    node: string,
    resourceKey: string,
    name: string,
    point: NumberDataPoint,
    value: number,
  ): number {
    const key = this.#seriesKey(node, resourceKey, name, point);
    const prev = this.#cumulative.get(key);
    this.#cumulative.set(key, {key, value});
    return value - (prev?.value ?? 0);
  }

  #record(
    node: string,
    _resourceKey: string,
    name: string,
    point: NumberDataPoint,
    now: number,
    update: (sample: MetricSample) => void,
  ): void {
    const attributes = toAttributes(point.attributes);
    const key = `${node}|${name}|${attrKey(attributes)}`;
    let sample = this.#samples.get(key);
    if (!sample) {
      sample = {
        node,
        name,
        attributes,
        sum: 0,
        count: 0,
        last: undefined,
        min: undefined,
        max: undefined,
        lastSeenMs: now,
      };
      this.#samples.set(key, sample);
    }
    sample.lastSeenMs = now;
    update(sample);
    for (const listener of this.#listeners) {
      listener(sample);
    }
  }

  /** Every series whose metric name ends with `suffix`. */
  series(suffix: string, node?: string): MetricSample[] {
    return [...this.#samples.values()].filter(
      s => s.name.endsWith(suffix) && (node === undefined || s.node === node),
    );
  }

  /** The summed value of every series whose metric name ends with `suffix`. */
  total(suffix: string, node?: string): number {
    return this.series(suffix, node).reduce((acc, s) => acc + s.sum, 0);
  }

  /** Counter totals broken out by one attribute. */
  byAttribute(suffix: string, attribute: string): Record<string, number> {
    const out: Record<string, number> = {};
    for (const s of this.series(suffix)) {
      const key = s.attributes[attribute] ?? '';
      out[key] = (out[key] ?? 0) + s.sum;
    }
    return out;
  }

  /** Counter totals broken out by two attributes, joined with `/`. */
  byAttributes(
    suffix: string,
    first: string,
    second: string,
  ): Record<string, number> {
    const out: Record<string, number> = {};
    for (const s of this.series(suffix)) {
      const key = `${s.attributes[first] ?? ''}/${s.attributes[second] ?? ''}`;
      out[key] = (out[key] ?? 0) + s.sum;
    }
    return out;
  }

  snapshot(): MetricSample[] {
    return Array.from(this.#samples.values(), s => ({...s}));
  }
}

export class OtlpMetricsReceiver {
  readonly #server: Server;
  readonly store = new MetricStore();
  #errors = 0;

  constructor() {
    this.#server = createServer((req, res) => {
      const match = METRICS_PATH.exec(req.url ?? '');
      if (req.method !== 'POST' || !match) {
        res.writeHead(404).end();
        return;
      }
      const node = decodeURIComponent(match[1]);
      const chunks: Buffer[] = [];
      req.on('data', chunk => chunks.push(chunk as Buffer));
      req.on('end', () => {
        try {
          let raw = Buffer.concat(chunks);
          if (req.headers['content-encoding'] === 'gzip') {
            raw = gunzipSync(raw);
          }
          if (raw.length > 0) {
            this.store.ingest(node, JSON.parse(raw.toString('utf8')));
          }
          // The OTLP spec wants a (possibly empty) ExportMetricsServiceResponse.
          res.writeHead(200, {'content-type': 'application/json'}).end('{}');
        } catch {
          this.#errors++;
          res.writeHead(400).end();
        }
      });
      req.on('error', () => {
        this.#errors++;
      });
    });
  }

  get parseErrors(): number {
    return this.#errors;
  }

  listen(port: number): Promise<void> {
    return new Promise((resolve, reject) => {
      this.#server.once('error', reject);
      this.#server.listen(port, '127.0.0.1', () => {
        this.#server.off('error', reject);
        resolve();
      });
    });
  }

  close(): Promise<void> {
    return new Promise(resolve => {
      this.#server.closeAllConnections?.();
      this.#server.close(() => resolve());
    });
  }
}
