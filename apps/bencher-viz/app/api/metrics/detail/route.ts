import { NextRequest, NextResponse } from 'next/server';
import cache from '@/lib/cache';

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const benchmarkId = searchParams.get('id');
  const days = parseInt(searchParams.get('days') || '7', 10);

  if (!benchmarkId) {
    return NextResponse.json(
      { error: 'Benchmark ID is required' },
      { status: 400 }
    );
  }

  // Required Bencher API configuration
  const BENCHER_API_TOKEN = process.env.BENCHER_API_TOKEN;
  const BENCHER_PROJECT = process.env.BENCHER_PROJECT;
  const BENCHER_API_URL = process.env.BENCHER_API_URL || 'https://api.bencher.dev';
  const BENCHER_BRANCH = process.env.BENCHER_BRANCH;
  const BENCHER_TESTBED = process.env.BENCHER_TESTBED;
  const BENCHER_MEASURE = process.env.BENCHER_MEASURE;

  if (!BENCHER_PROJECT || !BENCHER_BRANCH || !BENCHER_TESTBED || !BENCHER_MEASURE) {
    return NextResponse.json(
      { error: 'Missing required environment variables' },
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
    // Calculate time range
    const endTime = Date.now();
    const startTime = endTime - (days * 24 * 60 * 60 * 1000);

    // Create cache key for detail data
    const detailCacheKey = `detail:${benchmarkId}:${BENCHER_BRANCH}:${BENCHER_TESTBED}:${BENCHER_MEASURE}:${days}d`;

    // Check cache first
    const cachedData = cache.get(detailCacheKey);
    if (cachedData) {
      return NextResponse.json(cachedData);
    }

    // Fetch performance data with full details
    const perfUrl = new URL(
      `/v0/projects/${BENCHER_PROJECT}/perf`,
      BENCHER_API_URL
    );

    perfUrl.searchParams.append('benchmarks', benchmarkId);
    perfUrl.searchParams.append('branches', BENCHER_BRANCH);
    perfUrl.searchParams.append('testbeds', BENCHER_TESTBED);
    perfUrl.searchParams.append('measures', BENCHER_MEASURE);
    perfUrl.searchParams.append('start_time', startTime.toString());
    perfUrl.searchParams.append('end_time', endTime.toString());

    const perfResponse = await fetch(perfUrl.toString(), { headers });

    if (!perfResponse.ok) {
      const errorText = await perfResponse.text();
      return NextResponse.json(
        { error: `Failed to fetch performance data: ${errorText}` },
        { status: perfResponse.status }
      );
    }

    const perfData = await perfResponse.json();

    // The API returns results array with detailed metrics
    if (!perfData.results || perfData.results.length === 0) {
      return NextResponse.json(
        { error: 'No data found for this benchmark' },
        { status: 404 }
      );
    }

    const result = perfData.results[0];

    // Prepare the response data
    const responseData = {
      benchmark: result.benchmark,
      branch: result.branch,
      testbed: result.testbed,
      measure: result.measure,
      metrics: result.metrics || [],
      project: BENCHER_PROJECT,
      timeRange: {
        start: new Date(startTime).toISOString(),
        end: new Date(endTime).toISOString(),
        days
      }
    };

    // Cache the response data
    cache.set(detailCacheKey, responseData);

    // Return the full detailed data
    return NextResponse.json(responseData);
  } catch (_error) {
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
}