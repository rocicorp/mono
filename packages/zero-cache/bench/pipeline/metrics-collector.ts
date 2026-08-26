import Fastify, {type FastifyInstance} from 'fastify';

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
  readonly hydrationTimeMs: PercentileStats | null;
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
  #server: FastifyInstance | null = null;
  #port: number = 0;
  readonly #metrics = new Map<string, RawDataPoint[]>();

  async start(port = 0): Promise<number> {
    const server = Fastify({logger: false});

    server.addContentTypeParser(
      '*',
      {parseAs: 'string'},
      (_req, body, done) => {
        try {
          const json = typeof body === 'string' ? JSON.parse(body) : body;
          done(null, json);
        } catch {
          done(null, {});
        }
      },
    );

    const handler = (
      req: {body?: unknown},
      reply: {code: (n: number) => {send: (obj: unknown) => void}},
    ) => {
      try {
        const body = (req.body ?? {}) as Record<string, unknown>;
        this.#ingestOTLP(body);
        reply.code(200).send({status: 'ok'});
      } catch {
        reply.code(200).send({status: 'error'});
      }
    };

    server.post('/v1/metrics', handler);
    server.post('/v1/metrics/', handler);
    server.post('/*', handler);

    const address = await server.listen({port, host: '127.0.0.1'});
    const url = new URL(address);
    this.#port = Number(url.port);
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

      const resource = rm.resource as Record<string, unknown> | undefined;
      const attributes = (resource?.attributes ?? []) as {
        key: string;
        value: {stringValue?: string; intValue?: number};
      }[];

      for (const attr of attributes) {
        if (attr.key === 'process.worker') {
          worker = attr.value?.stringValue ?? worker;
        }
        if (attr.key === 'process.worker_index') {
          workerIndex = Number(attr.value?.intValue ?? workerIndex);
        }
      }

      const scopeMetrics = (rm.scopeMetrics ?? []) as Record<string, unknown>[];
      for (const sm of scopeMetrics) {
        const metrics = (sm.metrics ?? []) as Record<string, unknown>[];
        for (const metric of metrics) {
          const name = metric.name as string;
          if (!name) {
            continue;
          }

          this.#recordMetricData(name, worker, workerIndex, metric);
        }
      }
    }
  }

  #recordMetricData(
    name: string,
    worker: string,
    workerIndex: number,
    metric: Record<string, unknown>,
  ): void {
    if (!this.#metrics.has(name)) {
      this.#metrics.set(name, []);
    }
    const points = this.#metrics.get(name)!;

    const timestamp = Date.now();

    // 1. Gauge
    if (metric.gauge) {
      const g = metric.gauge as {
        dataPoints?: {asDouble?: number; asInt?: number | string}[];
      };
      for (const dp of g.dataPoints ?? []) {
        const val = Number(dp.asDouble ?? dp.asInt ?? 0);
        points.push({worker, workerIndex, timestamp, value: val});
      }
    }

    // 2. Sum / Counter
    if (metric.sum) {
      const s = metric.sum as {
        dataPoints?: {asDouble?: number; asInt?: number | string}[];
      };
      for (const dp of s.dataPoints ?? []) {
        const val = Number(dp.asDouble ?? dp.asInt ?? 0);
        points.push({worker, workerIndex, timestamp, value: val});
      }
    }

    // 3. Histogram
    if (metric.histogram) {
      const h = metric.histogram as {
        dataPoints?: {
          count?: number | string;
          sum?: number;
          min?: number;
          max?: number;
          explicitBounds?: number[];
          bucketCounts?: (number | string)[];
        }[];
      };
      for (const dp of h.dataPoints ?? []) {
        points.push({
          worker,
          workerIndex,
          timestamp,
          count: Number(dp.count ?? 0),
          sum: dp.sum,
          min: dp.min,
          max: dp.max,
          bounds: dp.explicitBounds,
          bucketCounts: dp.bucketCounts?.map(Number),
        });
      }
    }

    // 4. Exponential Histogram
    if (metric.exponentialHistogram) {
      const eh = metric.exponentialHistogram as {
        dataPoints?: {
          count?: number | string;
          sum?: number;
          min?: number;
          max?: number;
        }[];
      };
      for (const dp of eh.dataPoints ?? []) {
        points.push({
          worker,
          workerIndex,
          timestamp,
          count: Number(dp.count ?? 0),
          sum: dp.sum,
          min: dp.min,
          max: dp.max,
        });
      }
    }
  }

  getRawData(metricName: string): RawDataPoint[] {
    return this.#metrics.get(metricName) ?? [];
  }

  getSummary(): MetricSummary {
    return {
      replicationLagMs: this.#summarizeGaugeOrHistogram(
        'zero.replication.total_lag',
        1,
      ),
      e2eServingLagMs: this.#summarizeHistogram(
        'zero.sync.e2e_serving_lag',
        1000,
      ),
      advancementLatencyMs: this.#summarizeHistogram(
        'zero.sync.advance-time',
        1000,
      ),
      viewSyncerLagMs: this.#summarizeHistogram(
        'zero.sync.view_syncer_lag',
        1000,
      ),
      hydrationTimeMs: this.#summarizeHistogram(
        'zero.sync.hydration-time',
        1000,
      ),
      pipelineResets: this.#sumCounter('zero.sync.pipeline-resets'),
      transactionsReplicated: this.#sumCounter('zero.replication.transactions'),
      changesReplicated: this.#sumCounter('zero.replication.changes'),
      flowControlWaits: this.#sumCounter('zero.replication.flow_control.waits'),
      flowControlWaitDurationMs: this.#summarizeHistogram(
        'zero.replication.flow_control.wait_duration',
        1000,
      ),
    };
  }

  #sumCounter(name: string): number {
    const points = this.#metrics.get(name);
    if (!points || points.length === 0) {
      return 0;
    }
    const latestPerWorker = new Map<string, number>();
    for (const p of points) {
      const key = `${p.worker}:${p.workerIndex}`;
      latestPerWorker.set(key, p.value ?? p.count ?? 0);
    }
    let total = 0;
    for (const val of latestPerWorker.values()) {
      total += val;
    }
    return total;
  }

  #summarizeGaugeOrHistogram(
    name: string,
    multiplier: number,
  ): PercentileStats | null {
    const points = this.#metrics.get(name);
    if (!points || points.length === 0) {
      return null;
    }

    const values = points
      .map(p => p.value)
      .filter((v): v is number => v !== undefined)
      .map(v => v * multiplier);

    if (values.length === 0) {
      return this.#summarizeHistogram(name, multiplier);
    }

    return computePercentiles(values);
  }

  #summarizeHistogram(
    name: string,
    multiplier: number,
  ): PercentileStats | null {
    const points = this.#metrics.get(name);
    if (!points || points.length === 0) {
      return null;
    }

    const latestPerWorker = new Map<string, RawDataPoint>();
    for (const p of points) {
      const key = `${p.worker}:${p.workerIndex}`;
      latestPerWorker.set(key, p);
    }

    let totalCount = 0;
    let totalSum = 0;
    let globalMin = Number.POSITIVE_INFINITY;
    let globalMax = Number.NEGATIVE_INFINITY;

    const allValues: number[] = [];

    for (const p of latestPerWorker.values()) {
      if (p.count !== undefined) {
        totalCount += p.count;
      }
      if (p.sum !== undefined) {
        totalSum += p.sum * multiplier;
      }
      if (p.min !== undefined) {
        globalMin = Math.min(globalMin, p.min * multiplier);
      }
      if (p.max !== undefined) {
        globalMax = Math.max(globalMax, p.max * multiplier);
      }

      if (p.bounds && p.bucketCounts) {
        for (let i = 0; i < p.bucketCounts.length; i++) {
          const count = p.bucketCounts[i];
          const lower = i === 0 ? 0 : (p.bounds[i - 1] ?? 0);
          const upper = i < p.bounds.length ? p.bounds[i] : lower * 2;
          const mid = ((lower + upper) / 2) * multiplier;
          for (let k = 0; k < Math.min(count, 100); k++) {
            allValues.push(mid);
          }
        }
      }
    }

    if (totalCount === 0) {
      return null;
    }

    if (allValues.length > 0) {
      return computePercentiles(allValues);
    }

    const avg = totalCount > 0 ? totalSum / totalCount : 0;
    return {
      count: totalCount,
      sum: totalSum,
      avg,
      min: Number.isFinite(globalMin) ? globalMin : avg,
      p50: avg,
      p90: avg,
      p95: avg,
      p99: avg,
      max: Number.isFinite(globalMax) ? globalMax : avg,
    };
  }

  async stop(): Promise<void> {
    if (this.#server) {
      await this.#server.close();
      this.#server = null;
    }
  }
}

function computePercentiles(values: number[]): PercentileStats {
  values.sort((a, b) => a - b);
  const n = values.length;
  const sum = values.reduce((a, b) => a + b, 0);
  const avg = n > 0 ? sum / n : 0;

  const p = (pct: number) => {
    if (n === 0) {
      return 0;
    }
    const idx = Math.min(Math.floor((pct / 100) * n), n - 1);
    return values[idx];
  };

  return {
    count: n,
    sum,
    avg,
    min: values[0] ?? 0,
    p50: p(50),
    p90: p(90),
    p95: p(95),
    p99: p(99),
    max: values[n - 1] ?? 0,
  };
}
