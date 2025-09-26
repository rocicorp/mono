'use client';

import { useState, useEffect, useCallback } from 'react';
import { Sparklines, SparklinesLine, SparklinesSpots, SparklinesReferenceLine } from 'react-sparklines';

interface SparklineData {
  benchmarkId: string;
  benchmarkName: string;
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

export default function Home() {
  const [search, setSearch] = useState('');
  const [debouncedSearch, setDebouncedSearch] = useState('');
  const [sparklines, setSparklines] = useState<SparklineData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [hasMore, setHasMore] = useState(true);
  const [selectedSparkline, setSelectedSparkline] = useState<SparklineData | null>(null);

  // Debounce search input
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearch(search);
      setPage(1);
      setSparklines([]);
    }, 300);

    return () => clearTimeout(timer);
  }, [search]);

  // Fetch data
  const fetchMetrics = useCallback(async (pageNum: number, searchTerm: string, append: boolean = false) => {
    try {
      setLoading(true);
      setError(null);

      const params = new URLSearchParams({
        page: pageNum.toString(),
        perPage: '100',
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
  }, []);

  // Initial load and search changes
  useEffect(() => {
    fetchMetrics(1, debouncedSearch, false);
  }, [debouncedSearch, fetchMetrics]);

  // Load more function
  const loadMore = () => {
    if (!loading && hasMore) {
      const nextPage = page + 1;
      setPage(nextPage);
      fetchMetrics(nextPage, debouncedSearch, true);
    }
  };

  // Format benchmark name for display
  const formatName = (name: string) => {
    // Truncate long names and add ellipsis
    const maxLength = 50;
    if (name.length > maxLength) {
      return name.substring(0, maxLength) + '...';
    }
    return name;
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
      {/* Modal */}
      {selectedSparkline && (
        <div
          className="fixed inset-0 bg-black bg-opacity-50 z-50 flex items-center justify-center p-4"
          onClick={() => setSelectedSparkline(null)}
        >
          <div
            className="bg-white rounded-lg shadow-2xl max-w-2xl w-full p-6 max-h-[90vh] overflow-y-auto"
            onClick={(e) => e.stopPropagation()}
          >
            {/* Close button */}
            <button
              onClick={() => setSelectedSparkline(null)}
              className="float-right text-gray-400 hover:text-gray-600 text-2xl leading-none"
            >
              ×
            </button>

            {/* Modal content */}
            <h2 className="text-lg font-semibold text-gray-900 mb-4 pr-8">
              {selectedSparkline.benchmarkName}
            </h2>

            {/* Large Sparkline */}
            <div className="h-48 mb-6">
              {selectedSparkline.data.length > 0 ? (
                (() => {
                  const { min, max } = getMinMax(selectedSparkline.data);
                  const latest = selectedSparkline.data[selectedSparkline.data.length - 1];
                  const first = selectedSparkline.data[0];
                  const change = first && latest ? ((latest.value - first.value) / first.value) * 100 : 0;

                  return (
                    <>
                      <Sparklines
                        data={selectedSparkline.data.map(d => d.value)}
                        min={min * 0.95}
                        max={max * 1.05}
                        margin={4}
                      >
                        <SparklinesLine
                          color={change >= 0 ? '#10B981' : '#EF4444'}
                          style={{ strokeWidth: 2, fill: "none" }}
                        />
                        <SparklinesReferenceLine
                          type="mean"
                          style={{ stroke: '#374151', strokeDasharray: '4 4', strokeWidth: 1.5, opacity: 0.8 }}
                        />
                        <SparklinesSpots
                          size={3}
                          style={{ fill: change >= 0 ? '#10B981' : '#EF4444' }}
                        />
                      </Sparklines>

                      {/* Detailed Stats */}
                      <div className="mt-6 grid grid-cols-2 gap-4">
                        <div className="bg-gray-50 rounded p-3">
                          <div className="text-sm text-gray-500 mb-1">Latest Value</div>
                          <div className="text-xl font-bold text-gray-900">
                            {latest ? formatValue(latest.value) : 'N/A'}
                          </div>
                        </div>
                        <div className="bg-gray-50 rounded p-3">
                          <div className="text-sm text-gray-500 mb-1">Change</div>
                          <div className={`text-xl font-bold ${change >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {change >= 0 ? '+' : ''}{change.toFixed(2)}%
                          </div>
                        </div>
                        <div className="bg-gray-50 rounded p-3">
                          <div className="text-sm text-gray-500 mb-1">Minimum</div>
                          <div className="text-xl font-bold text-gray-900">
                            {formatValue(min)}
                          </div>
                        </div>
                        <div className="bg-gray-50 rounded p-3">
                          <div className="text-sm text-gray-500 mb-1">Maximum</div>
                          <div className="text-xl font-bold text-gray-900">
                            {formatValue(max)}
                          </div>
                        </div>
                        <div className="bg-gray-50 rounded p-3">
                          <div className="text-sm text-gray-500 mb-1">Average</div>
                          <div className="text-xl font-bold text-gray-900">
                            {formatValue(selectedSparkline.data.reduce((sum, d) => sum + d.value, 0) / selectedSparkline.data.length)}
                          </div>
                        </div>
                        <div className="bg-gray-50 rounded p-3">
                          <div className="text-sm text-gray-500 mb-1">Data Points</div>
                          <div className="text-xl font-bold text-gray-900">
                            {selectedSparkline.data.length}
                          </div>
                        </div>
                      </div>

                      {/* Time range */}
                      <div className="mt-4 text-sm text-gray-500 text-center">
                        {first && latest && (
                          <>
                            {new Date(first.timestamp).toLocaleDateString()} — {new Date(latest.timestamp).toLocaleDateString()}
                          </>
                        )}
                      </div>
                    </>
                  );
                })()
              ) : (
                <div className="flex items-center justify-center h-full text-gray-400">
                  No data available
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Header */}
      <div className="sticky top-0 z-20 bg-white shadow-sm border-b">
        <div className="px-4 py-3">
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
                    className="bg-white rounded border border-gray-200 p-2 transition-all relative cursor-pointer hover:shadow-lg hover:z-10"
                    onClick={() => setSelectedSparkline(sparkline)}
                  >
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
                            color={change >= 0 ? '#10B981' : '#EF4444'}
                            style={{ strokeWidth: 1, fill: "none" }}
                          />
                          <SparklinesReferenceLine
                            type="mean"
                            style={{ stroke: '#374151', strokeDasharray: '2 2', strokeWidth: 1, opacity: 0.8 }}
                          />
                          <SparklinesSpots
                            size={0}
                            style={{ fill: change >= 0 ? '#10B981' : '#EF4444', opacity: 0 }}
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
    </div>
  );
}