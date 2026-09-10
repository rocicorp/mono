import type {MetricSummary, PercentileStats} from './metrics.ts';

export type CloudZeroPodResource = {
  readonly pod: string;
  readonly role: 'replication-manager' | 'view-syncer';
  readonly cpuCores: number;
  readonly memoryWorkingSetBytes: number;
  readonly memoryMB: number;
  readonly pipelines?: number | undefined;
};

export type CloudZeroMetricsSummary = {
  readonly stackId: string;
  readonly rmPod?: CloudZeroPodResource | undefined;
  readonly vsPods: readonly CloudZeroPodResource[];
  readonly vsSummary: {
    readonly podCount: number;
    readonly totalCpuCores: number;
    readonly avgCpuCores: number;
    readonly maxCpuCores: number;
    readonly totalMemoryMB: number;
    readonly maxMemoryMB: number;
    readonly totalPipelines: number;
  };
  readonly replicationLagMs: PercentileStats | null;
  readonly servingLagMs: PercentileStats | null;
};

export type ParsedMetric = {
  readonly name: string;
  readonly labels: Record<string, string>;
  readonly value: number;
};

const METRIC_LINE_PATTERN = /^([a-zA-Z0-9_]+)(?:\{([^}]+)\})?\s+([^\s]+)$/;
const LABEL_PAIR_PATTERN = /([a-zA-Z0-9_]+)="([^"]*)"/g;

export function parsePrometheusText(
  text: string,
  targetStackId?: string | undefined,
): ParsedMetric[] {
  const metrics: ParsedMetric[] = [];
  const lines = text.split('\n');

  for (const line of lines) {
    if (line.startsWith('#') || !line.trim()) {
      continue;
    }
    if (targetStackId && !line.includes(`stack_id="${targetStackId}"`)) {
      continue;
    }

    const match = line.match(METRIC_LINE_PATTERN);
    if (!match) {
      continue;
    }

    const [, name, rawLabels, strVal] = match;
    const numVal = Number(strVal);
    if (Number.isNaN(numVal)) {
      continue;
    }

    const labels: Record<string, string> = {};
    if (rawLabels) {
      for (const m of rawLabels.matchAll(LABEL_PAIR_PATTERN)) {
        labels[m[1]] = m[2];
      }
    }

    metrics.push({name, labels, value: numVal});
  }

  return metrics;
}

export function buildCloudZeroSnapshot(
  metrics: readonly ParsedMetric[],
  stackId: string,
): CloudZeroMetricsSummary {
  const cpuByPod = new Map<string, number>();
  const memByPod = new Map<string, number>();
  const pipelinesByPod = new Map<string, number>();
  const replLags: number[] = [];
  const servingLagStatsByStat = new Map<string, number[]>();
  const servingLagScalars: number[] = [];
  const lagHistogramBuckets: {le: number; count: number}[] = [];
  let lagHistogramSum: number | undefined;
  let lagHistogramCount: number | undefined;

  for (const m of metrics) {
    const pod = m.labels.pod;
    if (m.name === 'k8s_pod_cpu_usage' && pod) {
      cpuByPod.set(pod, m.value);
    } else if (m.name === 'k8s_pod_memory_working_set_bytes' && pod) {
      memByPod.set(pod, m.value);
    } else if (m.name === 'zero_sync_pipelines_total' && pod) {
      pipelinesByPod.set(pod, (pipelinesByPod.get(pod) ?? 0) + m.value);
    } else if (m.name === 'zero_replication_total_lag_millisecond') {
      replLags.push(m.value);
    } else if (m.name === 'zero_sync_serving_lag_stats_millisecond') {
      if (m.labels.stat) {
        const list = servingLagStatsByStat.get(m.labels.stat) ?? [];
        list.push(m.value);
        servingLagStatsByStat.set(m.labels.stat, list);
      } else {
        servingLagScalars.push(m.value);
      }
    } else if (m.name === 'zero_sync_serving_lag_millisecond') {
      servingLagScalars.push(m.value);
    } else if (
      m.name === 'zero_sync_view_syncer_lag_seconds_bucket' ||
      m.name === 'zero_sync_view_syncer_lag_bucket'
    ) {
      if (m.labels.le) {
        const le = m.labels.le === '+Inf' ? Infinity : Number(m.labels.le);
        if (!Number.isNaN(le)) {
          const multiplier = m.name.includes('_seconds_') ? 1000 : 1;
          lagHistogramBuckets.push({le: le * multiplier, count: m.value});
        }
      }
    } else if (
      m.name === 'zero_sync_view_syncer_lag_seconds_sum' ||
      m.name === 'zero_sync_view_syncer_lag_sum'
    ) {
      const multiplier = m.name.includes('_seconds_') ? 1000 : 1;
      lagHistogramSum = (lagHistogramSum ?? 0) + m.value * multiplier;
    } else if (
      m.name === 'zero_sync_view_syncer_lag_seconds_count' ||
      m.name === 'zero_sync_view_syncer_lag_count'
    ) {
      lagHistogramCount = (lagHistogramCount ?? 0) + m.value;
    }
  }

  let rmPod: CloudZeroPodResource | undefined;
  const vsPods: CloudZeroPodResource[] = [];

  const allPods = new Set([...cpuByPod.keys(), ...memByPod.keys()]);
  for (const pod of allPods) {
    const cpu = cpuByPod.get(pod) ?? 0;
    const memBytes = memByPod.get(pod) ?? 0;
    const memoryMB = Number((memBytes / (1024 * 1024)).toFixed(1));
    const pipelines = pipelinesByPod.get(pod);

    if (pod.includes('replication-manager')) {
      rmPod = {
        pod,
        role: 'replication-manager',
        cpuCores: cpu,
        memoryWorkingSetBytes: memBytes,
        memoryMB,
      };
    } else if (pod.includes('view-syncer')) {
      vsPods.push({
        pod,
        role: 'view-syncer',
        cpuCores: cpu,
        memoryWorkingSetBytes: memBytes,
        memoryMB,
        pipelines,
      });
    }
  }

  // Sort view-syncers deterministically by pod name
  vsPods.sort((a, b) => a.pod.localeCompare(b.pod));

  const totalVsCpu = vsPods.reduce((acc, p) => acc + p.cpuCores, 0);
  const maxVsCpu = vsPods.reduce((max, p) => Math.max(max, p.cpuCores), 0);
  const avgVsCpu = vsPods.length > 0 ? totalVsCpu / vsPods.length : 0;
  const totalVsMem = vsPods.reduce((acc, p) => acc + p.memoryMB, 0);
  const maxVsMem = vsPods.reduce((max, p) => Math.max(max, p.memoryMB), 0);
  const totalPipelines = vsPods.reduce((acc, p) => acc + (p.pipelines ?? 0), 0);

  const mins = servingLagStatsByStat.get('min') ?? [];
  const maxs =
    servingLagStatsByStat.get('max') ??
    (servingLagScalars.length > 0 ? servingLagScalars : []);
  const exactMin = mins.length > 0 ? Math.min(...mins) : undefined;
  const exactMax = maxs.length > 0 ? Math.max(...maxs) : undefined;

  const histogramLag =
    lagHistogramBuckets.length > 0
      ? computeHistogramPercentiles(
          lagHistogramBuckets,
          lagHistogramSum,
          lagHistogramCount,
          exactMin,
          exactMax,
        )
      : null;

  const servingLagMs =
    histogramLag ??
    computeServingLagStats(servingLagStatsByStat, servingLagScalars);

  return {
    stackId,
    rmPod,
    vsPods,
    vsSummary: {
      podCount: vsPods.length,
      totalCpuCores: Number(totalVsCpu.toFixed(4)),
      avgCpuCores: Number(avgVsCpu.toFixed(4)),
      maxCpuCores: Number(maxVsCpu.toFixed(4)),
      totalMemoryMB: Number(totalVsMem.toFixed(1)),
      maxMemoryMB: Number(maxVsMem.toFixed(1)),
      totalPipelines,
    },
    replicationLagMs: computeStatsFromNumbers(replLags),
    servingLagMs,
  };
}

function computeHistogramPercentiles(
  buckets: readonly {readonly le: number; readonly count: number}[],
  sum?: number | undefined,
  totalCount?: number | undefined,
  exactMin?: number | undefined,
  exactMax?: number | undefined,
): PercentileStats | null {
  if (buckets.length === 0) {
    return null;
  }
  const countByLe = new Map<number, number>();
  for (const b of buckets) {
    countByLe.set(b.le, (countByLe.get(b.le) ?? 0) + b.count);
  }
  const sorted = Array.from(countByLe.entries(), ([le, count]) => ({
    le,
    count,
  })).sort((a, b) => a.le - b.le);

  const n = totalCount ?? sorted.at(-1)?.count ?? 0;
  if (n === 0) {
    return null;
  }

  const finiteBuckets = sorted.filter(b => Number.isFinite(b.le));
  const highestFiniteBound = finiteBuckets.at(-1)?.le ?? 0;

  const quantile = (q: number): number => {
    const rank = q * n;
    for (let i = 0; i < sorted.length; i++) {
      if (sorted[i].count >= rank) {
        const prevBound = i === 0 ? 0 : sorted[i - 1].le;
        const prevCount = i === 0 ? 0 : sorted[i - 1].count;
        const bucketCount = sorted[i].count - prevCount;
        if (bucketCount <= 0 || !Number.isFinite(sorted[i].le)) {
          return Number(prevBound.toFixed(2));
        }
        const fraction = (rank - prevCount) / bucketCount;
        return Number(
          (prevBound + fraction * (sorted[i].le - prevBound)).toFixed(2),
        );
      }
    }
    return Number(highestFiniteBound.toFixed(2));
  };

  const firstPositiveIndex = sorted.findIndex(b => b.count > 0);
  const histMin =
    firstPositiveIndex <= 0 ? 0 : (sorted[firstPositiveIndex - 1]?.le ?? 0);
  const min = exactMin ?? histMin;
  const max = exactMax ?? highestFiniteBound;
  const s = sum ?? n * quantile(0.5);

  return {
    count: n,
    sum: Number(s.toFixed(2)),
    avg: Number((s / n).toFixed(2)),
    min,
    p50: quantile(0.5),
    p75: quantile(0.75),
    p90: quantile(0.9),
    p95: quantile(0.95),
    p99: quantile(0.99),
    max,
  };
}

function computeServingLagStats(
  statsByStat: ReadonlyMap<string, readonly number[]>,
  scalars: readonly number[],
): PercentileStats | null {
  if (statsByStat.size === 0) {
    return computeStatsFromNumbers(scalars);
  }

  const mins = statsByStat.get('min') ?? [];
  const p50s = statsByStat.get('p50') ?? [];
  const p75s = statsByStat.get('p75') ?? [];
  const p90s = statsByStat.get('p90') ?? [];
  const p95s = statsByStat.get('p95') ?? [];
  const p99s = statsByStat.get('p99') ?? [];
  const maxs = statsByStat.get('max') ?? (scalars.length > 0 ? scalars : []);

  const min = mins.length > 0 ? Math.min(...mins) : 0;
  const p50 = p50s.length > 0 ? Math.max(...p50s) : 0;
  const p75 = p75s.length > 0 ? Math.max(...p75s) : undefined;
  const p90 = p90s.length > 0 ? Math.max(...p90s) : undefined;
  const p95 = p95s.length > 0 ? Math.max(...p95s) : undefined;
  const p99 = p99s.length > 0 ? Math.max(...p99s) : (p75 ?? p50);
  const max = maxs.length > 0 ? Math.max(...maxs) : p99;
  const avg =
    p50s.length > 0 ? p50s.reduce((a, b) => a + b, 0) / p50s.length : p50;
  const count = p50s.length > 0 ? p50s.length : 1;

  return {
    count,
    sum: Number((avg * count).toFixed(2)),
    avg: Number(avg.toFixed(2)),
    min,
    p50,
    p75,
    p90,
    p95,
    p99,
    max,
  };
}

function computeStatsFromNumbers(
  values: readonly number[],
): PercentileStats | null {
  if (values.length === 0) {
    return null;
  }
  const sorted = values.toSorted((a, b) => a - b);
  const sum = sorted.reduce((acc, v) => acc + v, 0);
  const count = sorted.length;
  const avg = sum / count;

  const percentile = (p: number): number => {
    const idx = Math.min(
      sorted.length - 1,
      Math.max(0, Math.ceil((p / 100) * sorted.length) - 1),
    );
    return sorted[idx] ?? 0;
  };

  return {
    count,
    sum: Number(sum.toFixed(2)),
    avg: Number(avg.toFixed(2)),
    min: sorted[0] ?? 0,
    p50: percentile(50),
    p75: percentile(75),
    p90: percentile(90),
    p95: percentile(95),
    p99: percentile(99),
    max: sorted.at(-1) ?? 0,
  };
}

export class CloudZeroMetricsPoller {
  readonly #metricsUrl: string;
  readonly #apiKey: string;
  readonly #stackId: string;
  readonly #intervalMs: number;
  #timer: NodeJS.Timeout | null = null;
  #latest: CloudZeroMetricsSummary | null = null;
  readonly #snapshots: CloudZeroMetricsSummary[] = [];

  constructor(options: {
    metricsUrl: string;
    apiKey: string;
    stackId: string;
    intervalMs?: number | undefined;
  }) {
    this.#metricsUrl = options.metricsUrl;
    this.#apiKey = options.apiKey;
    this.#stackId = options.stackId;
    this.#intervalMs = Math.max(2000, options.intervalMs ?? 5000);
  }

  get latest(): CloudZeroMetricsSummary | null {
    return this.#latest;
  }

  get stackId(): string {
    return this.#stackId;
  }

  async fetchSnapshot(): Promise<CloudZeroMetricsSummary | null> {
    try {
      const res = await fetch(this.#metricsUrl, {
        headers: {
          authorization: `Bearer ${this.#apiKey}`,
          accept: 'text/plain',
        },
        signal: AbortSignal.timeout(5000),
      });
      if (!res.ok) {
        return null;
      }
      const text = await res.text();
      const parsed = parsePrometheusText(text, this.#stackId);
      const snapshot = buildCloudZeroSnapshot(parsed, this.#stackId);
      this.#latest = snapshot;
      this.#snapshots.push(snapshot);
      return snapshot;
    } catch {
      return null;
    }
  }

  start(): void {
    // Fire immediate initial poll
    void this.fetchSnapshot();

    this.#timer = setInterval(() => {
      void this.fetchSnapshot();
    }, this.#intervalMs);
  }

  reset(): void {
    this.#snapshots.length = 0;
    if (this.#latest) {
      this.#snapshots.push(this.#latest);
    }
  }

  async stop(): Promise<CloudZeroMetricsSummary | null> {
    if (this.#timer === null) {
      return this.#latest;
    }
    clearInterval(this.#timer);
    this.#timer = null;
    // Take final snapshot
    return await this.fetchSnapshot();
  }

  toMetricSummary(): {
    metricSummary: Partial<MetricSummary>;
    cloudzeroSummary: CloudZeroMetricsSummary | null;
  } {
    const latest = this.#latest;
    if (!latest) {
      return {metricSummary: {}, cloudzeroSummary: null};
    }

    // Aggregate peak CPU and memory across all snapshots taken during the run
    let peakRmCpu = latest.rmPod?.cpuCores ?? 0;
    let peakRmWorkingSetBytes = latest.rmPod?.memoryWorkingSetBytes ?? 0;
    let peakVsTotalCpu = latest.vsSummary.totalCpuCores;
    let peakVsMaxCpu = latest.vsSummary.maxCpuCores;
    let peakVsTotalMem = latest.vsSummary.totalMemoryMB;
    let peakVsMaxMem = latest.vsSummary.maxMemoryMB;
    let peakVsPipelines = latest.vsSummary.totalPipelines;

    for (const snap of this.#snapshots) {
      if (snap.rmPod) {
        peakRmCpu = Math.max(peakRmCpu, snap.rmPod.cpuCores);
        peakRmWorkingSetBytes = Math.max(
          peakRmWorkingSetBytes,
          snap.rmPod.memoryWorkingSetBytes,
        );
      }
      peakVsTotalCpu = Math.max(peakVsTotalCpu, snap.vsSummary.totalCpuCores);
      peakVsMaxCpu = Math.max(peakVsMaxCpu, snap.vsSummary.maxCpuCores);
      peakVsTotalMem = Math.max(peakVsTotalMem, snap.vsSummary.totalMemoryMB);
      peakVsMaxMem = Math.max(peakVsMaxMem, snap.vsSummary.maxMemoryMB);
      peakVsPipelines = Math.max(
        peakVsPipelines,
        snap.vsSummary.totalPipelines,
      );
    }

    const peakRmMb = Number((peakRmWorkingSetBytes / (1024 * 1024)).toFixed(1));
    const replicationLagMs =
      aggregateLagStats(this.#snapshots, 'replicationLagMs') ??
      latest.replicationLagMs;
    const servingLagMs =
      aggregateLagStats(this.#snapshots, 'servingLagMs') ?? latest.servingLagMs;

    const podCount = latest.vsSummary.podCount;
    const peakVsAvgCpu =
      podCount > 0 ? Number((peakVsTotalCpu / podCount).toFixed(4)) : 0;

    const aggregated: CloudZeroMetricsSummary = {
      ...latest,
      rmPod: latest.rmPod
        ? {
            ...latest.rmPod,
            cpuCores: Number(peakRmCpu.toFixed(4)),
            memoryWorkingSetBytes: peakRmWorkingSetBytes,
            memoryMB: peakRmMb,
          }
        : undefined,
      vsSummary: {
        ...latest.vsSummary,
        totalCpuCores: Number(peakVsTotalCpu.toFixed(4)),
        avgCpuCores: peakVsAvgCpu,
        maxCpuCores: Number(peakVsMaxCpu.toFixed(4)),
        totalMemoryMB: Number(peakVsTotalMem.toFixed(1)),
        maxMemoryMB: Number(peakVsMaxMem.toFixed(1)),
        totalPipelines: peakVsPipelines,
      },
      replicationLagMs,
      servingLagMs,
    };

    return {
      metricSummary: {
        replicationLagMs,
        e2eServingLagMs: servingLagMs,
      },
      cloudzeroSummary: aggregated,
    };
  }
}

function aggregateLagStats(
  snapshots: readonly CloudZeroMetricsSummary[],
  field: 'replicationLagMs' | 'servingLagMs',
): PercentileStats | null {
  const statsList = snapshots
    .map(s => s[field])
    .filter((s): s is PercentileStats => s !== null);
  if (statsList.length === 0) {
    return null;
  }
  const max = Math.max(...statsList.map(s => s.max));
  const p99 = Math.max(...statsList.map(s => s.p99));
  const min = Math.min(...statsList.map(s => s.min));

  const p50s = statsList.map(s => s.p50);
  const p50 = Number(
    (p50s.reduce((a, b) => a + b, 0) / p50s.length).toFixed(2),
  );

  const p75s = statsList
    .map(s => s.p75)
    .filter((v): v is number => v !== undefined && !Number.isNaN(v));
  const p75 = p75s.length > 0 ? Math.max(...p75s) : undefined;

  const p90s = statsList
    .map(s => s.p90)
    .filter((v): v is number => v !== undefined && !Number.isNaN(v));
  const p90 = p90s.length > 0 ? Math.max(...p90s) : undefined;

  const p95s = statsList
    .map(s => s.p95)
    .filter((v): v is number => v !== undefined && !Number.isNaN(v));
  const p95 = p95s.length > 0 ? Math.max(...p95s) : undefined;

  const count = statsList.reduce((acc, s) => acc + s.count, 0);
  const sum = statsList.reduce((acc, s) => acc + s.sum, 0);
  const avg = count > 0 ? sum / count : 0;

  return {
    count,
    sum: Number(sum.toFixed(2)),
    avg: Number(avg.toFixed(2)),
    min,
    p50,
    p75,
    p90,
    p95,
    p99,
    max,
  };
}
