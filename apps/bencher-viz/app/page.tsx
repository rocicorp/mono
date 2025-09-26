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
      {/* Header */}
      <div className="sticky top-0 z-10 bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <h1 className="text-2xl font-bold text-gray-900 mb-4">Bencher Metrics Dashboard</h1>

          {/* Search Input */}
          <div className="relative">
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Filter metrics by name..."
              className="w-full px-4 py-2 pl-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
            <svg
              className="absolute left-3 top-2.5 h-5 w-5 text-gray-400"
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
        </div>
      </div>

      {/* Main Content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
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
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
              {sparklines.map((sparkline) => {
                const { min, max } = getMinMax(sparkline.data);
                const latest = sparkline.data[sparkline.data.length - 1];
                const first = sparkline.data[0];
                const change = first && latest ? ((latest.value - first.value) / first.value) * 100 : 0;

                return (
                  <div
                    key={sparkline.benchmarkId}
                    className="bg-white rounded-lg shadow-sm border border-gray-200 p-4 hover:shadow-md transition-shadow"
                  >
                    {/* Metric Name */}
                    <h3
                      className="text-sm font-medium text-gray-900 mb-2 truncate"
                      title={sparkline.benchmarkName}
                    >
                      {formatName(sparkline.benchmarkName)}
                    </h3>

                    {/* Stats Row */}
                    <div className="flex justify-between items-center mb-2 text-xs">
                      <span className="text-gray-500">
                        Latest: <span className="font-semibold text-gray-700">{latest ? formatValue(latest.value) : 'N/A'}</span>
                      </span>
                      <span className={`font-semibold ${change >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {change >= 0 ? '↑' : '↓'} {Math.abs(change).toFixed(1)}%
                      </span>
                    </div>

                    {/* Sparkline */}
                    <div className="h-16">
                      {sparkline.data.length > 0 ? (
                        <Sparklines
                          data={sparkline.data.map(d => d.value)}
                          min={min * 0.95}
                          max={max * 1.05}
                          margin={2}
                        >
                          <SparklinesLine
                            color="#3B82F6"
                            style={{ strokeWidth: 1.5, fill: "none" }}
                          />
                          <SparklinesReferenceLine
                            type="mean"
                            style={{ stroke: '#94A3B8', strokeDasharray: '2 2', strokeWidth: 1 }}
                          />
                          <SparklinesSpots
                            size={2}
                            style={{ fill: "#3B82F6" }}
                          />
                        </Sparklines>
                      ) : (
                        <div className="flex items-center justify-center h-full text-gray-400 text-sm">
                          No data available
                        </div>
                      )}
                    </div>

                    {/* Min/Max Labels */}
                    {sparkline.data.length > 0 && (
                      <div className="flex justify-between mt-1 text-xs text-gray-400">
                        <span>Min: {formatValue(min)}</span>
                        <span>Max: {formatValue(max)}</span>
                      </div>
                    )}
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