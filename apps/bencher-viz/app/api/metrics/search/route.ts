import { NextRequest, NextResponse } from 'next/server';

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

interface PerfResult {
  benchmark: {
    uuid: string;
    name: string;
  };
  results: PerfDataPoint[];
}

interface SparklineData {
  benchmarkId: string;
  benchmarkName: string;
  data: Array<{
    timestamp: number;
    value: number;
  }>;
}

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const search = searchParams.get('search') || '';
  const page = parseInt(searchParams.get('page') || '1', 10);
  const perPage = Math.min(parseInt(searchParams.get('perPage') || '100', 10), 100);

  // Required Bencher API configuration
  const BENCHER_API_TOKEN = process.env.BENCHER_API_TOKEN;
  const BENCHER_PROJECT = process.env.BENCHER_PROJECT;
  const BENCHER_API_URL = process.env.BENCHER_API_URL || 'https://api.bencher.dev';
  const BENCHER_BRANCH = process.env.BENCHER_BRANCH;
  const BENCHER_TESTBED = process.env.BENCHER_TESTBED;
  const BENCHER_MEASURE = process.env.BENCHER_MEASURE;

  if (!BENCHER_PROJECT) {
    return NextResponse.json(
      { error: 'BENCHER_PROJECT environment variable is required' },
      { status: 500 }
    );
  }

  const headers: HeadersInit = {
    'Content-Type': 'application/json',
  };

  if (BENCHER_API_TOKEN) {
    headers['Authorization'] = `Bearer ${BENCHER_API_TOKEN}`;
  }

  try {
    // Step 1: Search for benchmarks matching the search string
    const benchmarksUrl = new URL(
      `/v0/projects/${BENCHER_PROJECT}/benchmarks`,
      BENCHER_API_URL
    );

    if (search) {
      benchmarksUrl.searchParams.append('search', search);
    }
    benchmarksUrl.searchParams.append('per_page', '255'); // Get max benchmarks for pagination
    benchmarksUrl.searchParams.append('sort', 'name');
    benchmarksUrl.searchParams.append('direction', 'asc');

    const benchmarksResponse = await fetch(benchmarksUrl.toString(), { headers });

    if (!benchmarksResponse.ok) {
      const errorText = await benchmarksResponse.text();
      return NextResponse.json(
        { error: `Failed to fetch benchmarks: ${errorText}` },
        { status: benchmarksResponse.status }
      );
    }

    const benchmarksData = await benchmarksResponse.json();
    // The API returns an array directly, not wrapped in a data field
    const benchmarks: BenchmarkItem[] = Array.isArray(benchmarksData)
      ? benchmarksData
      : benchmarksData.data || [];

    // Debug logging
    console.log('Benchmarks API Response:', {
      url: benchmarksUrl.toString(),
      status: benchmarksResponse.status,
      totalBenchmarks: benchmarks.length,
      firstFew: benchmarks.slice(0, 3).map(b => b.name),
    });

    // Apply pagination to benchmarks
    const startIndex = (page - 1) * perPage;
    const endIndex = startIndex + perPage;
    const paginatedBenchmarks = benchmarks.slice(startIndex, endIndex);

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
          lookbackMs: 7 * 24 * 60 * 60 * 1000,
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
          error: `Missing required environment variables: ${missingConfig.join(', ')}. ` +
                  'These are needed to specify which branch, testbed, and measure to query for performance data.'
        },
        { status: 500 }
      );
    }

    // Step 3: Fetch performance data for each paginated benchmark
    const oneWeekAgo = Date.now() - (7 * 24 * 60 * 60 * 1000);
    const now = Date.now();

    const sparklines: SparklineData[] = [];

    // Batch requests for better performance
    const perfPromises = paginatedBenchmarks.map(async (benchmark) => {
      const perfUrl = new URL(
        `/v0/projects/${BENCHER_PROJECT}/perf`,
        BENCHER_API_URL
      );

      perfUrl.searchParams.append('benchmarks', benchmark.uuid);
      perfUrl.searchParams.append('branches', BENCHER_BRANCH);
      perfUrl.searchParams.append('testbeds', BENCHER_TESTBED);
      perfUrl.searchParams.append('measures', BENCHER_MEASURE);
      perfUrl.searchParams.append('start_time', oneWeekAgo.toString());
      perfUrl.searchParams.append('end_time', now.toString());

      try {
        const perfResponse = await fetch(perfUrl.toString(), { headers });

        if (!perfResponse.ok) {
          console.error(`Failed to fetch perf data for ${benchmark.name}: ${perfResponse.status}`);
          return null;
        }

        const perfData = await perfResponse.json();

        // Extract data points from the response
        // The metrics are in results[0].metrics array
        const dataPoints = perfData.results?.[0]?.metrics || [];

        const sparklineData: SparklineData = {
          benchmarkId: benchmark.uuid,
          benchmarkName: benchmark.name,
          data: dataPoints.map((point: PerfDataPoint) => ({
            timestamp: new Date(point.start_time).getTime(),
            value: point.metric.value,
          })).sort((a: any, b: any) => a.timestamp - b.timestamp),
        };

        return sparklineData;
      } catch (error) {
        console.error(`Error fetching perf data for ${benchmark.name}:`, error);
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
        lookbackMs: 7 * 24 * 60 * 60 * 1000,
      },
    });
  } catch (error) {
    console.error('Error in metrics search endpoint:', error);
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
}