import {NextRequest, NextResponse} from 'next/server';
import cache from '@/lib/cache';

interface BenchmarkItem {
  uuid: string;
  name: string;
  slug: string;
}

interface PerfDataPoint {
  metric: {
    value: number;
  };
  start_time: string;
}

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

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const search = searchParams.get('search') || '';
  const page = parseInt(searchParams.get('page') || '1', 10);
  const perPage = Math.min(
    parseInt(searchParams.get('perPage') || '100', 10),
    100,
  );
  const days = parseInt(searchParams.get('days') || '7', 10);

  // Required Bencher API configuration
  const BENCHER_API_TOKEN = process.env.BENCHER_API_TOKEN;
  const BENCHER_PROJECT = process.env.BENCHER_PROJECT;
  const BENCHER_API_URL =
    process.env.BENCHER_API_URL || 'https://api.bencher.dev';
  const BENCHER_BRANCH = process.env.BENCHER_BRANCH;
  const BENCHER_TESTBED = process.env.BENCHER_TESTBED;
  const BENCHER_MEASURE = process.env.BENCHER_MEASURE;

  if (!BENCHER_PROJECT) {
    return NextResponse.json(
      {error: 'BENCHER_PROJECT environment variable is required'},
      {status: 500},
    );
  }

  const headers: HeadersInit = {
    'Content-Type': 'application/json',
  };

  if (BENCHER_API_TOKEN) {
    headers['Authorization'] = `Bearer ${BENCHER_API_TOKEN}`;
  }

  try {
    // Step 1: Fetch active alerts to identify benchmarks with regressions
    const alertsCacheKey = `alerts:${BENCHER_PROJECT}:active`;
    const alertDetailsCacheKey = `alertDetails:${BENCHER_PROJECT}:active`;

    let alertedBenchmarkIds: Set<string> | null = cache.get(alertsCacheKey);
    interface AlertDetail {
      uuid: string;
      limit: string;
      boundary?: {
        baseline?: number;
        lower_limit?: number | null;
        upper_limit?: number | null;
      };
      status: string;
      modified: string;
      metric?: {
        value?: number;
      };
    }
    let alertDetails: Map<string, AlertDetail> | null =
      cache.get(alertDetailsCacheKey);

    if (!alertedBenchmarkIds || !alertDetails) {
      const alertsUrl = new URL(
        `/v0/projects/${BENCHER_PROJECT}/alerts`,
        BENCHER_API_URL,
      );
      alertsUrl.searchParams.append('status', 'active');
      alertsUrl.searchParams.append('per_page', '255');

      try {
        const alertsResponse = await fetch(alertsUrl.toString(), {headers});
        if (alertsResponse.ok) {
          const alertsData = await alertsResponse.json();
          alertedBenchmarkIds = new Set<string>();
          alertDetails = new Map<string, AlertDetail>();

          // Extract benchmark IDs and alert details
          if (Array.isArray(alertsData)) {
            for (const alert of alertsData) {
              if (alert.benchmark?.uuid) {
                alertedBenchmarkIds.add(alert.benchmark.uuid);
                // Store the most recent alert for each benchmark
                if (
                  !alertDetails.has(alert.benchmark.uuid) ||
                  new Date(alert.modified) >
                    new Date(alertDetails.get(alert.benchmark.uuid)!.modified)
                ) {
                  alertDetails.set(alert.benchmark.uuid, {
                    uuid: alert.uuid,
                    limit: alert.limit,
                    boundary: alert.boundary,
                    status: alert.status,
                    modified: alert.modified,
                    metric: alert.metric,
                  });
                }
              }
            }
          }

          // Cache both the IDs and details
          cache.set(alertsCacheKey, alertedBenchmarkIds);
          cache.set(alertDetailsCacheKey, alertDetails);
        }
      } catch (_error) {
        alertedBenchmarkIds = new Set<string>();
        alertDetails = new Map<string, AlertDetail>();
      }
    } else {
      // If we have cached data, get the alert details too
      alertDetails =
        cache.get(alertDetailsCacheKey) || new Map<string, AlertDetail>();
    }

    // Step 2: Fetch benchmarks list
    const benchmarksCacheKey = `benchmarks:${BENCHER_PROJECT}:${search || 'all'}`;

    // Check cache for benchmarks
    let benchmarks: BenchmarkItem[] | null = cache.get(benchmarksCacheKey);

    if (!benchmarks) {
      // Fetch benchmarks matching the search string
      const benchmarksUrl = new URL(
        `/v0/projects/${BENCHER_PROJECT}/benchmarks`,
        BENCHER_API_URL,
      );

      if (search) {
        benchmarksUrl.searchParams.append('search', search);
      }
      benchmarksUrl.searchParams.append('per_page', '255'); // Get max benchmarks for pagination
      benchmarksUrl.searchParams.append('sort', 'name');
      benchmarksUrl.searchParams.append('direction', 'asc');

      const benchmarksResponse = await fetch(benchmarksUrl.toString(), {
        headers,
      });

      if (!benchmarksResponse.ok) {
        const errorText = await benchmarksResponse.text();
        return NextResponse.json(
          {error: `Failed to fetch benchmarks: ${errorText}`},
          {status: benchmarksResponse.status},
        );
      }

      const benchmarksData = await benchmarksResponse.json();
      // The API returns an array directly, not wrapped in a data field
      benchmarks = Array.isArray(benchmarksData)
        ? benchmarksData
        : benchmarksData.data || [];

      // Cache the benchmarks list
      cache.set(benchmarksCacheKey, benchmarks);
    }
    benchmarks = benchmarks ?? [];

    // Sort benchmarks to prioritize those with alerts
    const sortedBenchmarks = [...benchmarks].sort((a, b) => {
      const aHasAlert = alertedBenchmarkIds?.has(a.uuid) || false;
      const bHasAlert = alertedBenchmarkIds?.has(b.uuid) || false;

      // Prioritize benchmarks with alerts
      if (aHasAlert && !bHasAlert) return -1;
      if (!aHasAlert && bHasAlert) return 1;

      // Otherwise maintain original order (alphabetical)
      return 0;
    });

    // Apply pagination to sorted benchmarks
    const startIndex = (page - 1) * perPage;
    const endIndex = startIndex + perPage;
    const paginatedBenchmarks = sortedBenchmarks.slice(startIndex, endIndex);

    if (paginatedBenchmarks.length === 0) {
      return NextResponse.json({
        sparklines: [],
        pagination: {
          page,
          perPage,
          total: benchmarks.length,
          totalPages: Math.ceil(benchmarks.length / perPage),
        },
        config: {
          project: BENCHER_PROJECT,
          branch: BENCHER_BRANCH,
          testbed: BENCHER_TESTBED,
          measure: BENCHER_MEASURE,
          lookbackMs: days * 24 * 60 * 60 * 1000,
        },
      });
    }

    // Step 2: For required UUIDs, fetch from environment or use defaults
    if (!BENCHER_BRANCH || !BENCHER_TESTBED || !BENCHER_MEASURE) {
      // Try to fetch the first available of each if not specified
      const missingConfig: string[] = [];
      if (!BENCHER_BRANCH) missingConfig.push('BENCHER_BRANCH');
      if (!BENCHER_TESTBED) missingConfig.push('BENCHER_TESTBED');
      if (!BENCHER_MEASURE) missingConfig.push('BENCHER_MEASURE');

      return NextResponse.json(
        {
          error:
            `Missing required environment variables: ${missingConfig.join(', ')}. ` +
            'These are needed to specify which branch, testbed, and measure to query for performance data.',
        },
        {status: 500},
      );
    }

    // Step 3: Fetch performance data for each paginated benchmark
    const startTime = Date.now() - days * 24 * 60 * 60 * 1000;
    const endTime = Date.now();

    const sparklines: SparklineData[] = [];

    // Batch requests for better performance
    const perfPromises = paginatedBenchmarks.map(async benchmark => {
      // Create cache key for performance data
      const perfCacheKey = `perf:${benchmark.uuid}:${BENCHER_BRANCH}:${BENCHER_TESTBED}:${BENCHER_MEASURE}:${days}d`;

      // Check cache first
      const cachedPerfData = cache.get(perfCacheKey) as SparklineData | undefined;
      if (cachedPerfData) {
        return cachedPerfData;
      }

      const perfUrl = new URL(
        `/v0/projects/${BENCHER_PROJECT}/perf`,
        BENCHER_API_URL,
      );

      perfUrl.searchParams.append('benchmarks', benchmark.uuid);
      perfUrl.searchParams.append('branches', BENCHER_BRANCH);
      perfUrl.searchParams.append('testbeds', BENCHER_TESTBED);
      perfUrl.searchParams.append('measures', BENCHER_MEASURE);
      perfUrl.searchParams.append('start_time', startTime.toString());
      perfUrl.searchParams.append('end_time', endTime.toString());

      try {
        const perfResponse = await fetch(perfUrl.toString(), {headers});

        if (!perfResponse.ok) {
          return null;
        }

        const perfData = await perfResponse.json();

        // Extract data points from the response
        // The metrics are in results[0].metrics array
        const dataPoints = perfData.results?.[0]?.metrics || [];

        const hasAlert = alertedBenchmarkIds?.has(benchmark.uuid) || false;
        const sparklineData: SparklineData = {
          benchmarkId: benchmark.uuid,
          benchmarkName: benchmark.name,
          hasAlert,
          alertInfo:
            hasAlert && alertDetails?.has(benchmark.uuid)
              ? alertDetails.get(benchmark.uuid)
              : undefined,
          data: dataPoints
            .map((point: PerfDataPoint) => ({
              timestamp: new Date(point.start_time).getTime(),
              value: point.metric.value,
            }))
            .sort((a: {timestamp: number; value: number}, b: {timestamp: number; value: number}) => a.timestamp - b.timestamp),
        };

        // Cache the sparkline data
        cache.set(perfCacheKey, sparklineData);

        return sparklineData;
      } catch (_error) {
        return null;
      }
    });

    const perfResults = await Promise.all(perfPromises);

    // Filter out failed requests
    for (const result of perfResults) {
      if (result) {
        sparklines.push(result);
      }
    }

    return NextResponse.json({
      sparklines,
      pagination: {
        page,
        perPage,
        total: benchmarks.length,
        totalPages: Math.ceil(benchmarks.length / perPage),
      },
      config: {
        project: BENCHER_PROJECT,
        branch: BENCHER_BRANCH,
        testbed: BENCHER_TESTBED,
        measure: BENCHER_MEASURE,
        lookbackMs: days * 24 * 60 * 60 * 1000,
      },
    });
  } catch (_error) {
    return NextResponse.json({error: 'Internal server error'}, {status: 500});
  }
}
