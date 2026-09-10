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
  targetStackId?: string,
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
  const servingLags: number[] = [];

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
      servingLags.push(m.value);
    } else if (m.name === 'zero_sync_serving_lag_millisecond') {
      servingLags.push(m.value);
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
    servingLagMs: computeStatsFromNumbers(servingLags),
  };
}

function computeStatsFromNumbers(values: number[]): PercentileStats | null {
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
    sum,
    avg,
    min: sorted[0] ?? 0,
    p50: percentile(50),
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
    intervalMs?: number;
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

  async stop(): Promise<CloudZeroMetricsSummary | null> {
    if (this.#timer !== null) {
      clearInterval(this.#timer);
      this.#timer = null;
    }
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
    let peakRmMb = latest.rmPod?.memoryMB ?? 0;
    let peakVsTotalCpu = latest.vsSummary.totalCpuCores;
    let peakVsMaxCpu = latest.vsSummary.maxCpuCores;
    let peakVsTotalMem = latest.vsSummary.totalMemoryMB;
    let peakVsMaxMem = latest.vsSummary.maxMemoryMB;

    for (const snap of this.#snapshots) {
      if (snap.rmPod) {
        peakRmCpu = Math.max(peakRmCpu, snap.rmPod.cpuCores);
        peakRmMb = Math.max(peakRmMb, snap.rmPod.memoryMB);
      }
      peakVsTotalCpu = Math.max(peakVsTotalCpu, snap.vsSummary.totalCpuCores);
      peakVsMaxCpu = Math.max(peakVsMaxCpu, snap.vsSummary.maxCpuCores);
      peakVsTotalMem = Math.max(peakVsTotalMem, snap.vsSummary.totalMemoryMB);
      peakVsMaxMem = Math.max(peakVsMaxMem, snap.vsSummary.maxMemoryMB);
    }

    const aggregated: CloudZeroMetricsSummary = {
      ...latest,
      rmPod: latest.rmPod
        ? {
            ...latest.rmPod,
            cpuCores: Number(peakRmCpu.toFixed(4)),
            memoryMB: Number(peakRmMb.toFixed(1)),
          }
        : undefined,
      vsSummary: {
        ...latest.vsSummary,
        totalCpuCores: Number(peakVsTotalCpu.toFixed(4)),
        maxCpuCores: Number(peakVsMaxCpu.toFixed(4)),
        totalMemoryMB: Number(peakVsTotalMem.toFixed(1)),
        maxMemoryMB: Number(peakVsMaxMem.toFixed(1)),
      },
    };

    return {
      metricSummary: {
        replicationLagMs: latest.replicationLagMs,
        e2eServingLagMs: latest.servingLagMs,
      },
      cloudzeroSummary: aggregated,
    };
  }
}
