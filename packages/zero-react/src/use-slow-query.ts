// oxlint-disable no-console
import {useEffect, useMemo, useState} from 'react';
import {addContextToQuery, asQueryInternals} from './bindings.ts';
import type {QueryResult} from './use-query.tsx';
import {useQuery, type UseQueryOptions} from './use-query.tsx';
import {useZero} from './zero-provider.tsx';
import type {
  DefaultContext,
  DefaultSchema,
  PullRow,
  QueryOrQueryRequest,
  ReadonlyJSONValue,
  Schema,
} from './zero.ts';

export const TESTING_SLOW_COMPLETE_DELAY_MS = 5_000;

/**
 * Configuration options for useSlowQuery behavior.
 */
export interface SlowQueryConfig {
  /**
   * Delay in milliseconds before completing the query.
   * @default 5000
   */
  delayMs: number;

  /**
   * Percentage of data (0-100) to return in the initial "unknown" state.
   * After the delay, 100% of data is returned with the actual result type.
   *
   * @default 50
   * @example
   * 0 - Start with no data, then show all data after delay
   * 50 - Start with 50% of data (default)
   * 90 - Start with 90% of data, then show remaining 10%
   */
  unknownDataPercentage: number;
}

let globalConfig: SlowQueryConfig = {
  delayMs: TESTING_SLOW_COMPLETE_DELAY_MS,
  unknownDataPercentage: 70,
};

// Cache of queries that have already completed once
const completedQueryCache = new Set<string>();

/**
 * Configure the behavior of useSlowQuery.
 * This affects all subsequent calls to useSlowQuery.
 *
 * @param config - Configuration options
 * @example
 * setupSlowQuery({ delayMs: 3000, unknownDataPercentage: 0 });
 */
export function setupSlowQuery(config: Partial<SlowQueryConfig>): void {
  globalConfig = {...globalConfig, ...config};
}

/**
 * Reset the slow query configuration to defaults and clear the completed query cache.
 * Useful for testing or resetting state.
 */
export function resetSlowQuery(): void {
  globalConfig = {
    delayMs: TESTING_SLOW_COMPLETE_DELAY_MS,
    unknownDataPercentage: 50,
  };
  completedQueryCache.clear();
}

/**
 * Wrapper around useQuery that simulates slow query completion for testing.
 * Returns a percentage of data with type 'unknown' initially, then returns full data
 * after the configured delay. Timer resets when query hash changes.
 *
 * Once a query completes for the first time, subsequent calls with the same
 * query hash will return data immediately without delay.
 *
 * This simulates a growth pattern where data loads progressively:
 * 1. Initial: returns configured percentage of data with type 'unknown'
 * 2. After configured delay: returns 100% of data with actual type (typically 'complete')
 * 3. Subsequent calls: returns 100% of data immediately
 *
 * Use setupSlowQuery() to configure the delay duration and initial data percentage.
 */
export function useSlowQuery<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
>(
  query: QueryOrQueryRequest<
    TTable,
    TInput,
    TOutput,
    TSchema,
    TReturn,
    TContext
  >,
  options?: UseQueryOptions | boolean,
): QueryResult<TReturn> {
  const [data, result] = useQuery(query, options);
  const zero = useZero<TSchema, undefined, TContext>();

  // Track which queries are currently delayed
  const [delayedQueries, setDelayedQueries] = useState<Set<string>>(new Set());

  // Get query hash to detect when query changes
  const queryHash = useMemo(() => {
    // Access query internals to get hash
    const q = addContextToQuery(query, zero.context);
    const internals = asQueryInternals(q);
    return internals.hash();
  }, [query, zero.context]);

  // Check if this query has already completed before
  const hasCompletedBefore = completedQueryCache.has(queryHash);

  // Reset and start timer when query hash changes (only if not previously completed)
  useEffect(() => {
    if (hasCompletedBefore) {
      console.log(
        `[useSlowQuery] Query ${queryHash.slice(0, 8)}... already completed before, returning immediately`,
      );
      return;
    }

    // Mark this query as delayed
    setDelayedQueries(prev => new Set(prev).add(queryHash));

    console.log(
      `[useSlowQuery] Starting ${globalConfig.delayMs}ms delay for query hash: ${queryHash.slice(0, 8)}...`,
    );

    const timeout = setTimeout(() => {
      // Remove from delayed set after timeout
      setDelayedQueries(prev => {
        const next = new Set(prev);
        next.delete(queryHash);
        return next;
      });

      // Mark as completed in global cache
      completedQueryCache.add(queryHash);

      console.log(`[useSlowQuery] Query ${queryHash.slice(0, 8)}... completed`);
    }, globalConfig.delayMs);

    return () => clearTimeout(timeout);
  }, [queryHash, hasCompletedBefore]);

  const isDelayed = !hasCompletedBefore && delayedQueries.has(queryHash);

  // If still delayed, return configured percentage of data with 'unknown' type
  if (isDelayed) {
    const partialData = Array.isArray(data)
      ? data.slice(
          0,
          Math.ceil((data.length * globalConfig.unknownDataPercentage) / 100),
        )
      : data;

    console.log(
      `[useSlowQuery] Returning partial data (${globalConfig.unknownDataPercentage}%, length=${Array.isArray(partialData) ? partialData.length : 'N/A'}) for query ${queryHash.slice(0, 8)}...`,
    );
    return [partialData, {type: 'unknown'}] as QueryResult<TReturn>;
  }

  // After delay, return actual data and result
  console.log(
    `[useSlowQuery] Returning full data (length=${Array.isArray(data) ? data.length : 'N/A'}) for query ${queryHash.slice(0, 8)}...`,
  );
  return [data, result];
}
