import {createServer, type Server} from 'node:http';
import {gunzipSync} from 'node:zlib';

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
  readonly workerKey: string;
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
      if (req.method !== 'POST') {
        res.writeHead(404).end();
        return;
      }
      const chunks: Buffer[] = [];
      req.on('data', chunk => chunks.push(chunk));
      req.on('end', () => {
        try {
          let buffer = Buffer.concat(chunks);
          if (req.headers['content-encoding'] === 'gzip') {
            buffer = gunzipSync(buffer);
          }
          this.#ingestOTLP(JSON.parse(buffer.toString('utf-8')));
          res.writeHead(200, {'Content-Type': 'application/json'});
          res.end('{"status":"ok"}');
        } catch {
          res.writeHead(400, {'Content-Type': 'application/json'});
          res.end('{"status":"error"}');
        }
      });
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

  async stop(): Promise<void> {
    if (this.#server) {
      await new Promise<void>(resolve => {
        this.#server?.close(() => resolve());
      });
      this.#server = null;
    }
  }

  #ingestOTLP(body: Record<string, unknown>): void {
    const resourceMetrics = (body.resourceMetrics ?? []) as Record<
      string,
      unknown
    >[];

    for (const rm of resourceMetrics) {
      const resource = (rm.resource ?? {}) as Record<string, unknown>;
      const attributes = (resource.attributes ?? []) as {
        key: string;
        value: {stringValue?: string; intValue?: number};
      }[];

      let worker = 'unknown';
      let workerIndex = 0;
      for (const a of attributes) {
        if (a.key === 'worker' && a.value?.stringValue) {
          worker = a.value.stringValue;
        } else if (a.key === 'workerIndex' && a.value?.intValue !== undefined) {
          workerIndex = a.value.intValue;
        }
      }
      const workerKey = `${worker}-${workerIndex}`;

      const scopeMetrics = (rm.scopeMetrics ?? []) as Record<string, unknown>[];
      for (const sm of scopeMetrics) {
        for (const metric of (sm.metrics ?? []) as Record<string, unknown>[]) {
          const name = String(metric.name ?? '');

          if (metric.gauge) {
            const gauge = metric.gauge as {
              dataPoints?: {asDouble?: number; asInt?: number}[];
            };
            for (const dp of gauge.dataPoints ?? []) {
              this.#addPoint(name, {
                workerKey,
                value: dp.asDouble ?? dp.asInt ?? 0,
              });
            }
          }

          if (metric.sum) {
            const sum = metric.sum as {
              dataPoints?: {asDouble?: number; asInt?: number}[];
            };
            for (const dp of sum.dataPoints ?? []) {
              this.#addPoint(name, {
                workerKey,
                value: dp.asDouble ?? dp.asInt ?? 0,
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
              this.#addPoint(name, {
                workerKey,
                count: Number(dp.count ?? 0),
                sum: dp.sum,
                min: dp.min,
                max: dp.max,
                bounds: dp.explicitBounds,
                bucketCounts: (dp.bucketCounts ?? []).map(Number),
              });
            }
          }

          if (metric.exponentialHistogram) {
            const expHist = metric.exponentialHistogram as {
              dataPoints?: {
                count?: number | string;
                sum?: number;
                min?: number;
                max?: number;
              }[];
            };
            for (const dp of expHist.dataPoints ?? []) {
              this.#addPoint(name, {
                workerKey,
                count: Number(dp.count ?? 0),
                sum: dp.sum,
                min: dp.min,
                max: dp.max,
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
      replicationLagMs: this.#computeStats(
        [
          'zero.replication.total_lag',
          'zero.replication.last_total_lag',
          'zero.replication.replica_lag',
          'zero_replication_lag_ms',
        ],
        1,
      ),
      e2eServingLagMs: this.#computeStats(
        ['zero.sync.e2e_serving_lag', 'zero_e2e_serving_lag_ms'],
        1000,
      ),
      advancementLatencyMs: this.#computeStats(
        [
          'zero.sync.advance-time',
          'zero.sync.ivm.advance-time',
          'zero_advancement_latency_ms',
        ],
        1000,
      ),
      viewSyncerLagMs: this.#computeStats(
        ['zero.sync.view_syncer_lag', 'zero_view_syncer_lag_ms'],
        1000,
      ),
      pipelineResets: this.#computeCounterSum([
        'zero.sync.pipeline_resets',
        'zero_pipeline_resets',
      ]),
      transactionsReplicated: this.#computeCounterSum([
        'zero.change-streamer.tx_forwarded',
        'zero.replication.transactions_replicated',
        'zero_transactions_replicated',
      ]),
      changesReplicated: this.#computeCounterSum([
        'zero.change-streamer.changes_forwarded',
        'zero.replication.changes_replicated',
        'zero_changes_replicated',
      ]),
      flowControlWaits: this.#computeCounterSum([
        'zero.change-streamer.flow_control.waits',
        'zero.replication.flow_control.waits',
        'zero_flow_control_waits',
      ]),
      flowControlWaitDurationMs: this.#computeStats(
        [
          'zero.change-streamer.flow_control.wait_time',
          'zero.replication.flow_control.wait_time',
          'zero_flow_control_wait_ms',
        ],
        1000,
      ),
    };
  }

  #computeStats(
    candidateNames: readonly string[],
    multiplier = 1,
  ): PercentileStats | null {
    let points: RawDataPoint[] = [];
    for (const name of candidateNames) {
      const found = this.#metrics.get(name);
      if (found && found.length > 0) {
        points = found;
        break;
      }
    }
    if (points.length === 0) {
      return null;
    }

    const values: number[] = [];
    let totalCount = 0;
    let totalSum = 0;
    let globalMin = Infinity;
    let globalMax = -Infinity;

    for (const p of points) {
      if (p.value !== undefined) {
        const val = p.value * multiplier;
        values.push(val);
        totalCount++;
        totalSum += val;
        if (val < globalMin) globalMin = val;
        if (val > globalMax) globalMax = val;
      } else if (p.bounds && p.bucketCounts) {
        totalCount += p.count ?? 0;
        if (p.sum !== undefined) totalSum += p.sum * multiplier;
        if (p.min !== undefined && p.min * multiplier < globalMin) {
          globalMin = p.min * multiplier;
        }
        if (p.max !== undefined && p.max * multiplier > globalMax) {
          globalMax = p.max * multiplier;
        }

        for (let i = 0; i < p.bucketCounts.length; i++) {
          const bCount = p.bucketCounts[i];
          const mid =
            i === 0
              ? (p.bounds[0] ?? 1) / 2
              : i < p.bounds.length
                ? ((p.bounds[i - 1] ?? 0) + (p.bounds[i] ?? 0)) / 2
                : (p.bounds.at(-1) ?? 1) * 1.5;
          const val = mid * multiplier;
          for (let j = 0; j < bCount; j++) {
            values.push(val);
          }
        }
      } else if (
        p.count &&
        (p.min !== undefined || p.max !== undefined || p.sum !== undefined)
      ) {
        totalCount += p.count;
        if (p.sum !== undefined) totalSum += p.sum * multiplier;
        if (p.min !== undefined && p.min * multiplier < globalMin) {
          globalMin = p.min * multiplier;
        }
        if (p.max !== undefined && p.max * multiplier > globalMax) {
          globalMax = p.max * multiplier;
        }
        const representative =
          p.sum !== undefined && p.count > 0
            ? (p.sum / p.count) * multiplier
            : (p.max ?? p.min ?? 0) * multiplier;
        values.push(representative);
      }
    }

    return computePercentiles(
      values,
      totalCount,
      totalSum,
      globalMin,
      globalMax,
    );
  }

  #computeCounterSum(candidateNames: readonly string[]): number {
    let points: RawDataPoint[] = [];
    for (const name of candidateNames) {
      const found = this.#metrics.get(name);
      if (found && found.length > 0) {
        points = found;
        break;
      }
    }
    if (points.length === 0) {
      return 0;
    }

    const byWorker = new Map<string, number>();
    for (const p of points) {
      byWorker.set(p.workerKey, p.value ?? p.count ?? 0);
    }

    let total = 0;
    for (const v of byWorker.values()) {
      total += v;
    }
    return total;
  }
}

function computePercentiles(
  values: number[],
  totalCount?: number,
  totalSum?: number,
  globalMin?: number,
  globalMax?: number,
): PercentileStats | null {
  if (values.length === 0) {
    return null;
  }

  values.sort((a, b) => a - b);
  const count = totalCount || values.length;
  const sum = totalSum ?? values.reduce((a, b) => a + b, 0);
  const min =
    globalMin !== undefined && globalMin !== Infinity
      ? globalMin
      : (values[0] ?? 0);
  const max =
    globalMax !== undefined && globalMax !== -Infinity
      ? globalMax
      : (values.at(-1) ?? 0);

  const percentileAt = (p: number) => {
    const idx = Math.min(
      Math.floor((p / 100) * values.length),
      values.length - 1,
    );
    return values[idx] ?? 0;
  };

  return {
    count,
    sum,
    avg: count > 0 ? sum / count : 0,
    min,
    p50: percentileAt(50),
    p90: percentileAt(90),
    p95: percentileAt(95),
    p99: percentileAt(99),
    max,
  };
}
