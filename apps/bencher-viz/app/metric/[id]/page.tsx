'use client';

import { useState, useEffect, useCallback } from 'react';
import { useParams, useRouter } from 'next/navigation';
import { Sparklines, SparklinesLine, SparklinesReferenceLine, SparklinesSpots } from 'react-sparklines';

interface MetricDataPoint {
  report: string;
  iteration: number;
  start_time: string;
  end_time: string;
  version: {
    number: number;
    hash: string;
  };
  metric: {
    uuid: string;
    value: number;
    lower_value: number | null;
    upper_value: number | null;
  };
  threshold?: {
    uuid: string;
    project: string;
    model: {
      test: string;
      lower_boundary: number | null;
      upper_boundary: number | null;
    };
  };
  boundary?: {
    baseline: number;
    lower_limit: number | null;
    upper_limit: number | null;
  };
  alert?: {
    uuid: string;
    limit: string;
    status: string;
    modified: string;
  } | null;
}

interface DetailedMetricData {
  benchmark: {
    uuid: string;
    name: string;
    slug: string;
  };
  branch: {
    uuid: string;
    name: string;
    slug: string;
  };
  testbed: {
    uuid: string;
    name: string;
    slug: string;
  };
  measure: {
    uuid: string;
    name: string;
    slug: string;
    units: string;
  };
  metrics: MetricDataPoint[];
}

export default function MetricDetailPage() {
  const params = useParams();
  const router = useRouter();
  const benchmarkId = params.id as string;

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [metricData, setMetricData] = useState<DetailedMetricData | null>(null);
  const [timeRange, setTimeRange] = useState(7); // days

  const fetchDetailedMetrics = useCallback(async (id: string) => {
    try {
      setLoading(true);
      setError(null);

      const response = await fetch(`/api/metrics/detail?id=${id}&days=${timeRange}`);

      if (!response.ok) {
        throw new Error('Failed to fetch metric details');
      }

      const data = await response.json();
      setMetricData(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  }, [timeRange]);

  useEffect(() => {
    if (benchmarkId) {
      void fetchDetailedMetrics(benchmarkId);
    }
  }, [benchmarkId, fetchDetailedMetrics]);

  const formatValue = (value: number) => {
    if (value >= 1000000) {
      return `${(value / 1000000).toFixed(2)}M`;
    } else if (value >= 1000) {
      return `${(value / 1000).toFixed(1)}K`;
    }
    return value.toFixed(2);
  };

  const formatDate = (dateString: string) => {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  const calculateStats = (metrics: MetricDataPoint[]) => {
    if (!metrics || metrics.length === 0) {
      return { min: 0, max: 0, avg: 0, latest: 0, first: 0, change: 0, stdDev: 0 };
    }

    const values = metrics.map(m => m.metric.value);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const sum = values.reduce((a, b) => a + b, 0);
    const avg = sum / values.length;
    const latest = metrics[metrics.length - 1]?.metric.value || 0;
    const first = metrics[0]?.metric.value || 0;
    const change = first ? ((latest - first) / first) * 100 : 0;

    // Calculate standard deviation
    const variance = values.reduce((acc, val) => acc + Math.pow(val - avg, 2), 0) / values.length;
    const stdDev = Math.sqrt(variance);

    return { min, max, avg, latest, first, change, stdDev };
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-gray-500">Loading metric details...</div>
      </div>
    );
  }

  if (error || !metricData) {
    return (
      <div className="min-h-screen bg-gray-50 p-8">
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg">
          Error: {error || 'Failed to load metric data'}
        </div>
        <button
          onClick={() => router.push('/')}
          className="mt-4 px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Back to Dashboard
        </button>
      </div>
    );
  }

  const stats = calculateStats(metricData.metrics);

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b">
        <div className="px-6 py-4">
          <button
            onClick={() => router.push('/')}
            className="text-blue-500 hover:text-blue-600 mb-4 flex items-center gap-2"
          >
            ← Back to Dashboard
          </button>

          <h1 className="text-2xl font-bold text-gray-900 mb-2">
            {metricData.benchmark.name}
          </h1>

          <div className="flex gap-6 text-sm text-gray-600">
            <span>Branch: <strong>{metricData.branch.name}</strong></span>
            <span>Testbed: <strong>{metricData.testbed.name}</strong></span>
            <span>Measure: <strong>{metricData.measure.name} ({metricData.measure.units})</strong></span>
          </div>
        </div>
      </div>

      {/* Time Range Selector */}
      <div className="px-6 py-4 bg-white border-b">
        <div className="flex gap-2">
          {[1, 7, 14, 30, 90].map(days => (
            <button
              key={days}
              onClick={() => setTimeRange(days)}
              className={`px-3 py-1 rounded ${
                timeRange === days
                  ? 'bg-blue-500 text-white'
                  : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
              }`}
            >
              {days === 1 ? '24h' : `${days}d`}
            </button>
          ))}
        </div>
      </div>

      <div className="px-6 py-6">
        {/* Large Chart */}
        <div className="bg-white rounded-lg shadow p-6 mb-6">
          <h2 className="text-lg font-semibold mb-4">Performance Trend</h2>
          <div className="h-64">
            {metricData.metrics.length > 0 ? (
              <Sparklines
                data={metricData.metrics.map(m => m.metric.value)}
                min={stats.min * 0.95}
                max={stats.max * 1.05}
                margin={5}
              >
                <SparklinesLine
                  color={stats.change >= 0 ? '#10B981' : '#EF4444'}
                  style={{ strokeWidth: 2, fill: "none" }}
                />
                <SparklinesReferenceLine
                  type="mean"
                  style={{ stroke: '#374151', strokeDasharray: '4 4', strokeWidth: 1.5, opacity: 0.6 }}
                />
                <SparklinesSpots
                  size={3}
                  style={{ fill: stats.change >= 0 ? '#10B981' : '#EF4444' }}
                />
              </Sparklines>
            ) : (
              <div className="flex items-center justify-center h-full text-gray-400">
                No data available for selected time range
              </div>
            )}
          </div>
        </div>

        {/* Statistics Cards */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <div className="bg-white rounded-lg shadow p-4">
            <div className="text-sm text-gray-500 mb-1">Latest Value</div>
            <div className="text-2xl font-bold text-gray-900">
              {formatValue(stats.latest)}
            </div>
            <div className="text-xs text-gray-500 mt-1">{metricData.measure.units}</div>
          </div>

          <div className="bg-white rounded-lg shadow p-4">
            <div className="text-sm text-gray-500 mb-1">Change</div>
            <div className={`text-2xl font-bold ${stats.change >= 0 ? 'text-green-600' : 'text-red-600'}`}>
              {stats.change >= 0 ? '+' : ''}{stats.change.toFixed(2)}%
            </div>
            <div className="text-xs text-gray-500 mt-1">vs {timeRange}d ago</div>
          </div>

          <div className="bg-white rounded-lg shadow p-4">
            <div className="text-sm text-gray-500 mb-1">Average</div>
            <div className="text-2xl font-bold text-gray-900">
              {formatValue(stats.avg)}
            </div>
            <div className="text-xs text-gray-500 mt-1">±{formatValue(stats.stdDev)}</div>
          </div>

          <div className="bg-white rounded-lg shadow p-4">
            <div className="text-sm text-gray-500 mb-1">Range</div>
            <div className="text-2xl font-bold text-gray-900">
              {formatValue(stats.min)} - {formatValue(stats.max)}
            </div>
            <div className="text-xs text-gray-500 mt-1">Min - Max</div>
          </div>
        </div>

        {/* Data Table */}
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <div className="px-6 py-4 border-b">
            <h2 className="text-lg font-semibold">Data Points ({metricData.metrics.length})</h2>
          </div>

          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50 border-b">
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Time</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Value</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Commit</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Version</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Change</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Alert</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {metricData.metrics.map((metric, index) => {
                  const prevValue = index > 0 ? metricData.metrics[index - 1].metric.value : metric.metric.value;
                  const change = prevValue ? ((metric.metric.value - prevValue) / prevValue) * 100 : 0;

                  return (
                    <tr key={`${metric.report}-${index}`} className="hover:bg-gray-50">
                      <td className="px-4 py-3 text-sm text-gray-900">
                        {formatDate(metric.start_time)}
                      </td>
                      <td className="px-4 py-3 text-sm font-medium text-gray-900">
                        {formatValue(metric.metric.value)}
                      </td>
                      <td className="px-4 py-3 text-sm">
                        <code className="text-xs bg-gray-100 px-1 py-0.5 rounded">
                          {metric.version.hash.substring(0, 8)}
                        </code>
                      </td>
                      <td className="px-4 py-3 text-sm text-gray-600">
                        #{metric.version.number}
                      </td>
                      <td className="px-4 py-3 text-sm">
                        {index > 0 && (
                          <span className={`font-medium ${change >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {change >= 0 ? '+' : ''}{change.toFixed(2)}%
                          </span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-sm">
                        {metric.alert && (
                          <span className={`px-2 py-1 text-xs rounded ${
                            metric.alert.status === 'active'
                              ? 'bg-red-100 text-red-700'
                              : 'bg-gray-100 text-gray-600'
                          }`}>
                            {metric.alert.status}
                          </span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}