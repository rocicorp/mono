import {describe, expect, test} from 'vitest';
import {
  buildCloudZeroSnapshot,
  CloudZeroMetricsPoller,
  parsePrometheusText,
  type ParsedMetric,
} from './cloudzero-metrics.ts';

describe('parsePrometheusText', () => {
  test('parses prometheus metrics and filters by stack_id', () => {
    const raw = `
# HELP k8s_pod_cpu_usage Pod CPU usage
# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{pod="stack-a-view-syncer-0",stack_id="stack-a",namespace="tenant"} 0.125
k8s_pod_cpu_usage{pod="stack-b-view-syncer-0",stack_id="stack-b",namespace="tenant"} 0.999
# Another metric
zero_replication_total_lag_millisecond{stack_id="stack-a"} 42.5
`;

    const parsedA = parsePrometheusText(raw, 'stack-a');
    expect(parsedA).toHaveLength(2);
    expect(parsedA[0]).toEqual({
      name: 'k8s_pod_cpu_usage',
      labels: {
        pod: 'stack-a-view-syncer-0',
        stack_id: 'stack-a',
        namespace: 'tenant',
      },
      value: 0.125,
    });
    expect(parsedA[1]).toEqual({
      name: 'zero_replication_total_lag_millisecond',
      labels: {
        stack_id: 'stack-a',
      },
      value: 42.5,
    });

    const parsedB = parsePrometheusText(raw, 'stack-b');
    expect(parsedB).toHaveLength(1);
    expect(parsedB[0]?.value).toBe(0.999);
  });
});

describe('buildCloudZeroSnapshot', () => {
  test('normalizes RM and multiple View-Syncers', () => {
    const metrics: ParsedMetric[] = [
      {
        name: 'k8s_pod_cpu_usage',
        labels: {pod: 'ehbb-replication-manager-784f-abcd'},
        value: 0.05,
      },
      {
        name: 'k8s_pod_memory_working_set_bytes',
        labels: {pod: 'ehbb-replication-manager-784f-abcd'},
        value: 104857600, // 100 MB
      },
      {
        name: 'k8s_pod_cpu_usage',
        labels: {pod: 'ehbb-view-syncer-0'},
        value: 0.2,
      },
      {
        name: 'k8s_pod_memory_working_set_bytes',
        labels: {pod: 'ehbb-view-syncer-0'},
        value: 209715200, // 200 MB
      },
      {
        name: 'zero_sync_pipelines_total',
        labels: {pod: 'ehbb-view-syncer-0'},
        value: 6,
      },
      {
        name: 'k8s_pod_cpu_usage',
        labels: {pod: 'ehbb-view-syncer-1'},
        value: 0.4,
      },
      {
        name: 'k8s_pod_memory_working_set_bytes',
        labels: {pod: 'ehbb-view-syncer-1'},
        value: 314572800, // 300 MB
      },
      {
        name: 'zero_sync_pipelines_total',
        labels: {pod: 'ehbb-view-syncer-1'},
        value: 6,
      },
      {
        name: 'zero_replication_total_lag_millisecond',
        labels: {},
        value: 12.5,
      },
      {
        name: 'zero_sync_serving_lag_stats_millisecond',
        labels: {},
        value: 8.2,
      },
    ];

    const snapshot = buildCloudZeroSnapshot(metrics, 'ehbb');
    expect(snapshot.stackId).toBe('ehbb');

    // RM
    expect(snapshot.rmPod).toBeDefined();
    expect(snapshot.rmPod?.role).toBe('replication-manager');
    expect(snapshot.rmPod?.cpuCores).toBe(0.05);
    expect(snapshot.rmPod?.memoryMB).toBe(100);

    // VS pods
    expect(snapshot.vsPods).toHaveLength(2);
    expect(snapshot.vsPods[0]?.pod).toBe('ehbb-view-syncer-0');
    expect(snapshot.vsPods[1]?.pod).toBe('ehbb-view-syncer-1');

    // VS summary
    expect(snapshot.vsSummary.podCount).toBe(2);
    expect(snapshot.vsSummary.totalCpuCores).toBe(0.6);
    expect(snapshot.vsSummary.avgCpuCores).toBe(0.3);
    expect(snapshot.vsSummary.maxCpuCores).toBe(0.4);
    expect(snapshot.vsSummary.totalMemoryMB).toBe(500);
    expect(snapshot.vsSummary.maxMemoryMB).toBe(300);
    expect(snapshot.vsSummary.totalPipelines).toBe(12);

    // Lags
    expect(snapshot.replicationLagMs?.avg).toBe(12.5);
    expect(snapshot.servingLagMs?.avg).toBe(8.2);
  });
});

describe('CloudZeroMetricsPoller', () => {
  test('aggregates peak CPU and RAM across snapshots', async () => {
    const poller = new CloudZeroMetricsPoller({
      metricsUrl: 'http://example.com/metrics',
      apiKey: 'test-key',
      stackId: 'test-stack',
    });

    const metrics1: ParsedMetric[] = [
      {
        name: 'k8s_pod_cpu_usage',
        labels: {pod: 'test-replication-manager-1'},
        value: 0.1,
      },
      {
        name: 'k8s_pod_memory_working_set_bytes',
        labels: {pod: 'test-replication-manager-1'},
        value: 104857600, // 100 MB
      },
      {
        name: 'k8s_pod_cpu_usage',
        labels: {pod: 'test-view-syncer-1'},
        value: 0.3,
      },
      {
        name: 'k8s_pod_memory_working_set_bytes',
        labels: {pod: 'test-view-syncer-1'},
        value: 209715200, // 200 MB
      },
    ];

    const metrics2: ParsedMetric[] = [
      {
        name: 'k8s_pod_cpu_usage',
        labels: {pod: 'test-replication-manager-1'},
        value: 0.25,
      },
      {
        name: 'k8s_pod_memory_working_set_bytes',
        labels: {pod: 'test-replication-manager-1'},
        value: 157286400, // 150 MB
      },
      {
        name: 'k8s_pod_cpu_usage',
        labels: {pod: 'test-view-syncer-1'},
        value: 0.2,
      },
      {
        name: 'k8s_pod_memory_working_set_bytes',
        labels: {pod: 'test-view-syncer-1'},
        value: 262144000, // 250 MB
      },
    ];

    // Mock fetch for 2 calls
    let call = 0;
    const originalFetch = globalThis.fetch;
    globalThis.fetch = ((_input: RequestInfo | URL, _init?: RequestInit) => {
      call++;
      const metrics = call === 1 ? metrics1 : metrics2;
      const text = metrics
        .map(
          m =>
            `${m.name}{pod="${m.labels.pod}",stack_id="test-stack"} ${m.value}`,
        )
        .join('\n');
      return Promise.resolve(new Response(text, {status: 200}));
    }) as typeof fetch;

    try {
      await poller.fetchSnapshot();
      await poller.fetchSnapshot();

      const summary = poller.toMetricSummary();
      expect(summary.cloudzeroSummary).toBeDefined();
      expect(summary.cloudzeroSummary?.rmPod?.cpuCores).toBe(0.25);
      expect(summary.cloudzeroSummary?.rmPod?.memoryMB).toBe(150);
      expect(summary.cloudzeroSummary?.vsSummary.maxCpuCores).toBe(0.3);
      expect(summary.cloudzeroSummary?.vsSummary.maxMemoryMB).toBe(250);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});
