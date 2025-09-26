'use client';

import { useState, useEffect, useCallback, Suspense } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import { Sparklines, SparklinesLine, SparklinesSpots, SparklinesReferenceLine } from 'react-sparklines';

interface SparklineData {
  benchmarkId: string;
  benchmarkName: string;
  hasAlert: boolean;
  alertInfo?: {
    limit: string;
    boundary?: {
      baseline?: number;
      lower_limit?: number | null;
      upper_limit?: number | null;
    };
    metric?: {
      value?: number;
    };
  };
  data: Array<{
    timestamp: number;
    value: number;
  }>;
}

interface ApiResponse {
  sparklines: SparklineData[];
  pagination: {
    page: number;
    perPage: number;
    total: number;
    totalPages: number;
  };
}

function HomeContent() {
  const router = useRouter();
  const searchParams = useSearchParams();

  // Initialize state from URL parameters
  const [search, setSearch] = useState(searchParams.get('search') || '');
  const [debouncedSearch, setDebouncedSearch] = useState(searchParams.get('search') || '');
  const [sparklines, setSparklines] = useState<SparklineData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [hasMore, setHasMore] = useState(true);
  const [timeRange, setTimeRange] = useState(parseInt(searchParams.get('days') || '7', 10));
  const [hoveredMetric, setHoveredMetric] = useState<SparklineData | null>(null);
  const [mousePosition, setMousePosition] = useState({ x: 0, y: 0 });

  // Update URL when parameters change
  const updateURL = useCallback((newSearch: string, newTimeRange: number) => {
    const params = new URLSearchParams();
    if (newSearch) {
      params.set('search', newSearch);
    }
    params.set('days', newTimeRange.toString());

    const newURL = params.toString() ? `?${params.toString()}` : '/';
    router.push(newURL, { scroll: false });
  }, [router]);

  // Debounce search input and update URL
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearch(search);
      setPage(1);
      updateURL(search, timeRange);
      // Don't clear sparklines here - let the fetch handle it
    }, 300);

    return () => clearTimeout(timer);
  }, [search, timeRange, updateURL]);

  // Fetch data
  const fetchMetrics = useCallback(async (pageNum: number, searchTerm: string, append: boolean = false) => {
    try {
      setLoading(true);
      setError(null);

      const params = new URLSearchParams({
        page: pageNum.toString(),
        perPage: '100',
        days: timeRange.toString(),
      });

      if (searchTerm) {
        params.append('search', searchTerm);
      }

      const response = await fetch(`/api/metrics/search?${params}`);

      if (!response.ok) {
        throw new Error('Failed to fetch metrics');
      }

      const data: ApiResponse = await response.json();

      if (append) {
        setSparklines(prev => [...prev, ...data.sparklines]);
      } else {
        setSparklines(data.sparklines);
      }

      setHasMore(data.pagination.page < data.pagination.totalPages);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
      if (!append) {
        setSparklines([]);
      }
    } finally {
      setLoading(false);
    }
  }, [timeRange]);

  // Initial load, search changes, and time range changes
  useEffect(() => {
    setPage(1);
    setSparklines([]);
    void fetchMetrics(1, debouncedSearch, false);
  }, [debouncedSearch, timeRange, fetchMetrics]);

  // Load more function
  const loadMore = () => {
    if (!loading && hasMore) {
      const nextPage = page + 1;
      setPage(nextPage);
      void fetchMetrics(nextPage, debouncedSearch, true);
    }
  };


  // Calculate min and max for better sparkline scaling
  const getMinMax = (data: Array<{ value: number }>) => {
    if (data.length === 0) return { min: 0, max: 1 };
    const values = data.map(d => d.value);
    return {
      min: Math.min(...values),
      max: Math.max(...values)
    };
  };

  // Format value for display
  const formatValue = (value: number) => {
    if (value >= 1000000) {
      return `${(value / 1000000).toFixed(2)}M`;
    } else if (value >= 1000) {
      return `${(value / 1000).toFixed(1)}K`;
    }
    return value.toFixed(0);
  };

  return (
    <div className="min-h-screen bg-gray-50">

      {/* Header */}
      <div className="sticky top-0 z-20 bg-white shadow-sm border-b">
        <div className="px-4 py-3 space-y-3">
          <div className="flex items-center gap-4">
            <h1 className="text-lg font-bold text-gray-900">Bencher Metrics</h1>

            {/* Search Input */}
            <div className="relative flex-1 max-w-md">
              <input
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Filter metrics..."
                className="w-full px-3 py-1.5 pl-8 text-sm border border-gray-300 rounded focus:ring-1 focus:ring-blue-500 focus:border-transparent"
              />
              <svg
                className="absolute left-2 top-2 h-4 w-4 text-gray-400"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                />
              </svg>
            </div>

            {/* Metrics count */}
            <span className="text-sm text-gray-500">
              {sparklines.length} metrics
            </span>
          </div>

          {/* Time Range Selector */}
          <div className="flex gap-2 items-center">
            <span className="text-sm text-gray-600">Time Range:</span>
            {[7, 14, 30, 90].map(days => (
              <button
                key={days}
                onClick={() => {
                  setTimeRange(days);
                  setPage(1);
                  setSparklines([]);
                }}
                className={`px-3 py-1 text-sm rounded transition-colors ${
                  timeRange === days
                    ? 'bg-blue-500 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                {days}d
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="px-4 py-4">
        {/* Loading State */}
        {loading && sparklines.length === 0 && (
          <div className="flex justify-center items-center h-64">
            <div className="text-gray-500">Loading metrics...</div>
          </div>
        )}

        {/* Error State */}
        {error && (
          <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg">
            Error: {error}
          </div>
        )}

        {/* Sparklines Grid */}
        {sparklines.length > 0 && (
          <>
            <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 xl:grid-cols-8 2xl:grid-cols-10 gap-2">
              {sparklines.map((sparkline) => {
                const { min, max } = getMinMax(sparkline.data);
                const latest = sparkline.data[sparkline.data.length - 1];
                const first = sparkline.data[0];
                const change = first && latest ? ((latest.value - first.value) / first.value) * 100 : 0;

                return (
                  <div
                    key={sparkline.benchmarkId}
                    className={`rounded border p-2 transition-all relative cursor-pointer hover:shadow-lg hover:z-10 ${
                      sparkline.hasAlert
                        ? 'bg-red-50 border-red-300'
                        : 'bg-white border-gray-200'
                    }`}
                    onClick={() => router.push(`/metric/${sparkline.benchmarkId}`)}
                    onMouseEnter={(e) => {
                      setHoveredMetric(sparkline);
                      const rect = e.currentTarget.getBoundingClientRect();
                      setMousePosition({
                        x: rect.left + rect.width / 2,
                        y: rect.top - 10
                      });
                    }}
                    onMouseLeave={() => setHoveredMetric(null)}
                  >
                    {/* Alert Indicator */}
                    {sparkline.hasAlert && (
                      <div className="absolute top-1 right-1 bg-red-500 text-white rounded-full p-0.5">
                        <svg className="w-2.5 h-2.5" fill="currentColor" viewBox="0 0 20 20">
                          <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
                        </svg>
                      </div>
                    )}

                    {/* Metric Name */}
                    <h3
                      className="text-xs font-medium text-gray-800 mb-1 truncate leading-tight"
                      title={sparkline.benchmarkName}
                    >
                      {sparkline.benchmarkName.split(' > ').pop() || sparkline.benchmarkName}
                    </h3>

                    {/* Sparkline */}
                    <div className="h-10 mb-1">
                      {sparkline.data.length > 0 ? (
                        <Sparklines
                          data={sparkline.data.map(d => d.value)}
                          min={min * 0.95}
                          max={max * 1.05}
                          margin={1}
                        >
                          <SparklinesLine
                            color={sparkline.hasAlert ? '#DC2626' : (change >= 0 ? '#10B981' : '#EF4444')}
                            style={{ strokeWidth: 1, fill: "none" }}
                          />
                          <SparklinesReferenceLine
                            type="mean"
                            style={{ stroke: '#374151', strokeDasharray: '2 2', strokeWidth: 1, opacity: 0.8 }}
                          />
                          <SparklinesSpots
                            size={0}
                            style={{ fill: sparkline.hasAlert ? '#DC2626' : (change >= 0 ? '#10B981' : '#EF4444'), opacity: 0 }}
                          />
                        </Sparklines>
                      ) : (
                        <div className="flex items-center justify-center h-full text-gray-400 text-xs">
                          No data
                        </div>
                      )}
                    </div>

                    {/* Compact Stats */}
                    <div className="flex justify-between items-center text-xs">
                      <span className="text-gray-600 font-medium">
                        {latest ? formatValue(latest.value) : 'N/A'}
                      </span>
                      <span className={`font-bold ${change >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {change >= 0 ? '+' : ''}{change.toFixed(0)}%
                      </span>
                    </div>
                  </div>
                );
              })}
            </div>

            {/* Load More Button */}
            {hasMore && (
              <div className="mt-8 flex justify-center">
                <button
                  onClick={loadMore}
                  disabled={loading}
                  className="px-6 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-600 disabled:bg-gray-300 disabled:cursor-not-allowed transition-colors"
                >
                  {loading ? 'Loading...' : 'Load More'}
                </button>
              </div>
            )}

            {/* Results Count */}
            <div className="mt-4 text-center text-sm text-gray-500">
              Showing {sparklines.length} metrics {!hasMore && '(all loaded)'}
            </div>
          </>
        )}

        {/* No Results */}
        {!loading && sparklines.length === 0 && !error && (
          <div className="text-center py-12 text-gray-500">
            No metrics found {search && `matching "${search}"`}
          </div>
        )}
      </div>

      {/* Tooltip */}
      {hoveredMetric && (
        <div
          className="fixed z-50 bg-gray-900 text-white px-3 py-2 rounded-lg text-sm max-w-md break-words pointer-events-none shadow-xl"
          style={{
            left: `${mousePosition.x}px`,
            top: `${mousePosition.y}px`,
            transform: 'translate(-50%, -100%)'
          }}
        >
          <div className="font-medium mb-1">{hoveredMetric.benchmarkName}</div>

          {hoveredMetric.hasAlert && hoveredMetric.alertInfo && (
            <div className="mt-2 pt-2 border-t border-gray-700 text-xs">
              <div className="flex items-center gap-1 text-red-400 mb-1">
                <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
                </svg>
                <span className="font-medium">Performance Alert</span>
              </div>
              {hoveredMetric.alertInfo.limit && (
                <div className="text-gray-300">
                  Limit exceeded: <span className="text-white">{hoveredMetric.alertInfo.limit}</span>
                </div>
              )}
              {hoveredMetric.alertInfo.boundary && (
                <div className="text-gray-300 mt-1">
                  {hoveredMetric.alertInfo.boundary.baseline && (
                    <div>Baseline: {formatValue(hoveredMetric.alertInfo.boundary.baseline)}</div>
                  )}
                  {(hoveredMetric.alertInfo.boundary.lower_limit != null ||
                    hoveredMetric.alertInfo.boundary.upper_limit != null) && (
                    <div>
                      Limits: {
                        hoveredMetric.alertInfo.boundary.lower_limit != null
                          ? formatValue(hoveredMetric.alertInfo.boundary.lower_limit)
                          : '—'
                      } to {
                        hoveredMetric.alertInfo.boundary.upper_limit != null
                          ? formatValue(hoveredMetric.alertInfo.boundary.upper_limit)
                          : '—'
                      }
                    </div>
                  )}
                </div>
              )}
              {hoveredMetric.alertInfo.metric && hoveredMetric.alertInfo.metric.value !== undefined && (
                <div className="text-gray-300 mt-1">
                  Actual value: <span className="text-red-400 font-medium">
                    {formatValue(hoveredMetric.alertInfo.metric.value)}
                  </span>
                </div>
              )}
            </div>
          )}

          <div
            className="absolute w-2 h-2 bg-gray-900 transform rotate-45"
            style={{
              bottom: '-4px',
              left: '50%',
              transform: 'translateX(-50%)'
            }}
          />
        </div>
      )}
    </div>
  );
}

export default function Home() {
  return (
    <Suspense fallback={
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-gray-500">Loading...</div>
      </div>
    }>
      <HomeContent />
    </Suspense>
  );
}