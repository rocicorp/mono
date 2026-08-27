import {createServer, type Server} from 'node:http';

export interface PercentileStats {
  readonly count: number;
  readonly sum: number;
  readonly avg: number;
  readonly min: number;
  readonly p50: number;
  readonly p90: number;
  readonly p95: number;
  readonly p99: number;
  readonly max: number;
}

export interface MetricSummary {
  readonly replicationLagMs: PercentileStats | null;
  readonly e2eServingLagMs: PercentileStats | null;
  readonly advancementLatencyMs: PercentileStats | null;
  readonly viewSyncerLagMs: PercentileStats | null;
  readonly pipelineResets: number;
  readonly transactionsReplicated: number;
  readonly changesReplicated: number;
  readonly flowControlWaits: number;
  readonly flowControlWaitDurationMs: PercentileStats | null;
}

interface RawDataPoint {
  readonly worker: string;
  readonly workerIndex: number;
  readonly timestamp: number;
  readonly value?: number | undefined;
  readonly count?: number | undefined;
  readonly sum?: number | undefined;
  readonly min?: number | undefined;
  readonly max?: number | undefined;
  readonly bounds?: readonly number[] | undefined;
  readonly bucketCounts?: readonly number[] | undefined;
}

export class OTelMetricsCollector {
  #server: Server | null = null;
  #port = 0;
  readonly #metrics = new Map<string, RawDataPoint[]>();

  async start(port = 0): Promise<number> {
    const server = createServer((req, res) => {
      if (req.method === 'POST') {
        const chunks: Buffer[] = [];
        req.on('data', chunk => chunks.push(chunk));
        req.on('end', () => {
          try {
            const raw = Buffer.concat(chunks).toString('utf-8');
            const json = JSON.parse(raw);
            this.#ingestOTLP(json);
            res.writeHead(200, {'Content-Type': 'application/json'});
            res.end(JSON.stringify({status: 'ok'}));
          } catch {
            res.writeHead(400, {'Content-Type': 'application/json'});
            res.end(JSON.stringify({status: 'error'}));
          }
        });
      } else {
        res.writeHead(404).end();
      }
    });

    await new Promise<void>((resolve, reject) => {
      server.listen(port, '127.0.0.1', () => resolve());
      server.once('error', reject);
    });

    const addr = server.address();
    this.#port = typeof addr === 'object' && addr ? addr.port : 0;
    this.#server = server;
    return this.#port;
  }

  get port(): number {
    return this.#port;
  }

  get endpoint(): string {
    return `http://127.0.0.1:${this.#port}/v1/metrics`;
  }

  reset(): void {
    this.#metrics.clear();
  }

  #ingestOTLP(body: Record<string, unknown>): void {
    const resourceMetrics = (body.resourceMetrics ?? []) as Record<
      string,
      unknown
    >[];

    for (const rm of resourceMetrics) {
      let worker = 'unknown';
      let workerIndex = 0;

      const resource = (rm.resource ?? {}) as Record<string, unknown>;
      const attributes = (resource.attributes ?? []) as {
        key: string;
        value: {stringValue?: string; intValue?: number};
      }[];

      for (const attr of attributes) {
        if (attr.key === 'worker' && attr.value?.stringValue) {
          worker = attr.value.stringValue;
        }
        if (attr.key === 'workerIndex' && attr.value?.intValue !== undefined) {
          workerIndex = attr.value.intValue;
        }
      }

      const scopeMetrics = (rm.scopeMetrics ?? []) as Record<string, unknown>[];
      for (const sm of scopeMetrics) {
        const metrics = (sm.metrics ?? []) as Record<string, unknown>[];
        for (const metric of metrics) {
          const name = String(metric.name ?? '');

          if (metric.gauge) {
            const gauge = metric.gauge as {
              dataPoints?: {
                timeUnixNano?: string;
                asDouble?: number;
                asInt?: number;
              }[];
            };
            for (const dp of gauge.dataPoints ?? []) {
              const val = dp.asDouble ?? dp.asInt ?? 0;
              this.#addPoint(name, {
                worker,
                workerIndex,
                timestamp: Date.now(),
                value: val,
              });
            }
          }

          if (metric.sum) {
            const sum = metric.sum as {
              dataPoints?: {
                timeUnixNano?: string;
                asDouble?: number;
                asInt?: number;
              }[];
            };
            for (const dp of sum.dataPoints ?? []) {
              const val = dp.asDouble ?? dp.asInt ?? 0;
              this.#addPoint(name, {
                worker,
                workerIndex,
                timestamp: Date.now(),
                value: val,
              });
            }
          }

          if (metric.histogram) {
            const hist = metric.histogram as {
              dataPoints?: {
                count?: number | string;
                sum?: number;
                min?: number;
                max?: number;
                explicitBounds?: number[];
                bucketCounts?: (number | string)[];
              }[];
            };
            for (const dp of hist.dataPoints ?? []) {
              const count = Number(dp.count ?? 0);
              const bucketCounts = (dp.bucketCounts ?? []).map(Number);
              this.#addPoint(name, {
                worker,
                workerIndex,
                timestamp: Date.now(),
                count,
                sum: dp.sum,
                min: dp.min,
                max: dp.max,
                bounds: dp.explicitBounds,
                bucketCounts,
              });
            }
          }
        }
      }
    }
  }

  #addPoint(name: string, point: RawDataPoint): void {
    let list = this.#metrics.get(name);
    if (!list) {
      list = [];
      this.#metrics.set(name, list);
    }
    list.push(point);
  }

  getSummary(): MetricSummary {
    return {
      replicationLagMs: this.#computeHistogramStats('zero_replication_lag_ms'),
      e2eServingLagMs: this.#computeHistogramStats('zero_e2e_serving_lag_ms'),
      advancementLatencyMs: this.#computeHistogramStats(
        'zero_advancement_latency_ms',
      ),
      viewSyncerLagMs: this.#computeGaugeStats('zero_view_syncer_lag_ms'),
      pipelineResets: this.#computeCounterSum('zero_pipeline_resets'),
      transactionsReplicated: this.#computeCounterSum(
        'zero_transactions_replicated',
      ),
      changesReplicated: this.#computeCounterSum('zero_changes_replicated'),
      flowControlWaits: this.#computeCounterSum('zero_flow_control_waits'),
      flowControlWaitDurationMs: this.#computeHistogramStats(
        'zero_flow_control_wait_ms',
      ),
    };
  }

  #computeHistogramStats(metricName: string): PercentileStats | null {
    const points = this.#metrics.get(metricName);
    if (!points || points.length === 0) {
      return null;
    }

    const values: number[] = [];
    let totalCount = 0;
    let totalSum = 0;
    let globalMin = Infinity;
    let globalMax = -Infinity;

    for (const p of points) {
      if (p.value !== undefined) {
        values.push(p.value);
        totalCount++;
        totalSum += p.value;
        if (p.value < globalMin) globalMin = p.value;
        if (p.value > globalMax) globalMax = p.value;
      } else if (p.bounds && p.bucketCounts && p.bucketCounts.length > 0) {
        totalCount += p.count ?? 0;
        if (p.sum !== undefined) totalSum += p.sum;
        if (p.min !== undefined && p.min < globalMin) globalMin = p.min;
        if (p.max !== undefined && p.max > globalMax) globalMax = p.max;

        for (let i = 0; i < p.bucketCounts.length; i++) {
          const bCount = p.bucketCounts[i];
          const mid =
            i === 0
              ? (p.bounds[0] ?? 1) / 2
              : i < p.bounds.length
                ? ((p.bounds[i - 1] ?? 0) + (p.bounds[i] ?? 0)) / 2
                : (p.bounds.at(-1) ?? 1) * 1.5;
          for (let j = 0; j < bCount; j++) {
            values.push(mid);
          }
        }
      }
    }

    if (values.length === 0) {
      return null;
    }

    values.sort((a, b) => a - b);

    const percentileAt = (p: number) => {
      const idx = Math.min(
        Math.floor((p / 100) * values.length),
        values.length - 1,
      );
      return values[idx] ?? 0;
    };

    return {
      count: totalCount || values.length,
      sum: totalSum || values.reduce((a, b) => a + b, 0),
      avg:
        (totalSum || values.reduce((a, b) => a + b, 0)) /
        (totalCount || values.length),
      min: globalMin !== Infinity ? globalMin : (values[0] ?? 0),
      p50: percentileAt(50),
      p90: percentileAt(90),
      p95: percentileAt(95),
      p99: percentileAt(99),
      max: globalMax !== -Infinity ? globalMax : (values.at(-1) ?? 0),
    };
  }

  #computeGaugeStats(metricName: string): PercentileStats | null {
    const points = this.#metrics.get(metricName);
    if (!points || points.length === 0) {
      return null;
    }

    const values = points
      .map(p => p.value)
      .filter((v): v is number => v !== undefined);

    if (values.length === 0) {
      return null;
    }

    values.sort((a, b) => a - b);
    const sum = values.reduce((a, b) => a + b, 0);

    const percentileAt = (p: number) => {
      const idx = Math.min(
        Math.floor((p / 100) * values.length),
        values.length - 1,
      );
      return values[idx] ?? 0;
    };

    return {
      count: values.length,
      sum,
      avg: sum / values.length,
      min: values[0] ?? 0,
      p50: percentileAt(50),
      p90: percentileAt(90),
      p95: percentileAt(95),
      p99: percentileAt(99),
      max: values.at(-1) ?? 0,
    };
  }

  #computeCounterSum(metricName: string): number {
    const points = this.#metrics.get(metricName);
    if (!points || points.length === 0) {
      return 0;
    }

    const byWorker = new Map<string, number>();
    for (const p of points) {
      const key = `${p.worker}-${p.workerIndex}`;
      const val = p.value ?? p.count ?? 0;
      byWorker.set(key, val);
    }

    let total = 0;
    for (const val of byWorker.values()) {
      total += val;
    }
    return total;
  }

  async stop(): Promise<void> {
    if (this.#server) {
      await new Promise<void>(resolve => {
        this.#server?.close(() => resolve());
      });
      this.#server = null;
    }
  }
}
