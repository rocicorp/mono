import {renderHook} from '@testing-library/react';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {addContextToQuery, asQueryInternals} from './bindings.ts';
import {useQuery} from './use-query.tsx';
import {resetSlowQuery, useSlowQuery} from './use-slow-query.ts';
import {useZero} from './zero-provider.tsx';

const TESTING_SLOW_COMPLETE_DELAY_MS = 5000;

// Mock dependencies
vi.mock('./use-query.tsx', () => ({
  useQuery: vi.fn(),
}));

vi.mock('./bindings.ts', () => ({
  addContextToQuery: vi.fn(),
  asQueryInternals: vi.fn(),
}));

vi.mock('./zero-provider.tsx', () => ({
  useZero: vi.fn(),
}));

describe('useSlowQuery', () => {
  // oxlint-disable-next-line no-explicit-any
  const mockQuery = {table: 'test', where: {}} as any;

  // Helper function to setup common mocks
  const setupMocks = (
    // oxlint-disable-next-line no-explicit-any
    data: any,
    resultType: 'complete' | 'unknown' = 'complete',
    hashValue = 'test-hash-123',
  ) => {
    const mockResult = {type: resultType} as const;
    vi.mocked(useQuery).mockReturnValue([data, mockResult]);
    // oxlint-disable-next-line no-explicit-any
    vi.mocked(useZero).mockReturnValue({context: undefined} as any);
    // oxlint-disable-next-line no-explicit-any
    vi.mocked(addContextToQuery).mockReturnValue(mockQuery as any);
    // oxlint-disable-next-line no-explicit-any
    vi.mocked(asQueryInternals).mockReturnValue({hash: () => hashValue} as any);
    return mockResult;
  };

  beforeEach(() => {
    vi.useFakeTimers();
    vi.clearAllMocks();
    resetSlowQuery(); // Reset the cache before each test
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  test('returns half data for incomplete results', () => {
    // Arrange
    const mockData = [{id: '1'}, {id: '2'}, {id: '3'}, {id: '4'}];
    setupMocks(mockData, 'unknown');

    // Act
    const {result} = renderHook(() => useSlowQuery(mockQuery));

    // Assert - incomplete results also get halved
    expect(result.current).toEqual([
      [{id: '1'}, {id: '2'}], // Half of 4 items = 2 items
      {type: 'unknown'},
    ]);
  });

  test('returns half data with unknown type when result is complete', () => {
    // Arrange
    const mockData = [{id: '1'}, {id: '2'}, {id: '3'}, {id: '4'}];
    setupMocks(mockData);

    // Act
    const {result} = renderHook(() => useSlowQuery(mockQuery));

    // Assert - should return half data with unknown type
    expect(result.current).toEqual([
      [{id: '1'}, {id: '2'}], // Half of 4 items = 2 items
      {type: 'unknown'},
    ]);
  });

  test('returns full data after timeout completes', async () => {
    // Arrange
    const mockData = [{id: '1'}, {id: '2'}, {id: '3'}, {id: '4'}];
    const mockResult = setupMocks(mockData);

    // Act
    const {result} = renderHook(() => useSlowQuery(mockQuery));

    // Initially should return half data
    expect(result.current).toEqual([[{id: '1'}, {id: '2'}], {type: 'unknown'}]);

    // Advance time past the delay
    await vi.advanceTimersByTimeAsync(TESTING_SLOW_COMPLETE_DELAY_MS);

    // State should be updated immediately after advancing timers
    expect(result.current).toEqual([mockData, mockResult]);
  });

  test('resets timer when query hash changes', async () => {
    // Arrange
    const mockData1 = [{id: '1'}, {id: '2'}, {id: '3'}, {id: '4'}];
    const mockData2 = [{id: 'a'}, {id: 'b'}, {id: 'c'}];
    const mockResult = {type: 'complete' as const};
    vi.mocked(useQuery).mockReturnValue([mockData1, mockResult]);

    let hashCounter = 0;
    vi.mocked(asQueryInternals).mockImplementation(
      () =>
        ({
          hash: () => `test-hash-${++hashCounter}`,
          // oxlint-disable-next-line no-explicit-any
        }) as any,
    );

    // Act
    const {result, rerender} = renderHook(({query}) => useSlowQuery(query), {
      initialProps: {query: mockQuery},
    });

    // Initially should return half data
    expect(result.current[1]).toEqual({type: 'unknown'});

    // Advance time partway through the delay (3 seconds out of 5)
    await vi.advanceTimersByTimeAsync(3000);

    // Change the query - this should reset the timer
    vi.mocked(useQuery).mockReturnValue([mockData2, mockResult]);
    rerender({query: {table: 'test', where: {new: true}}});

    // Should still be delayed even after the previous 3 seconds
    expect(result.current[1]).toEqual({type: 'unknown'});

    // Advance only 2 more seconds (total 5 from original, but only 2 from reset)
    await vi.advanceTimersByTimeAsync(2000);

    // Should still be delayed (need 3 more seconds from reset point)
    expect(result.current[1]).toEqual({type: 'unknown'});

    // Advance the remaining time (3 more seconds = 5 total from reset)
    await vi.advanceTimersByTimeAsync(3000);

    // Now should be complete with new data
    expect(result.current).toEqual([mockData2, mockResult]);
  });

  test('handles non-array data correctly', () => {
    // Arrange
    const mockData = {single: 'item'};
    setupMocks(mockData);

    // Act
    const {result} = renderHook(() => useSlowQuery(mockQuery));

    // Assert - non-array data should be returned as-is with unknown type
    expect(result.current).toEqual([{single: 'item'}, {type: 'unknown'}]);
  });

  test('slices array data correctly with odd number of items', () => {
    // Arrange
    const mockData = [{id: '1'}, {id: '2'}, {id: '3'}, {id: '4'}, {id: '5'}];
    setupMocks(mockData);

    // Act
    const {result} = renderHook(() => useSlowQuery(mockQuery));

    // Assert - floor(5 / 2) = 2 items
    expect(result.current).toEqual([[{id: '1'}, {id: '2'}], {type: 'unknown'}]);
  });

  test('cleans up timeout on unmount', () => {
    // Arrange
    const mockData = [{id: '1'}, {id: '2'}];
    setupMocks(mockData);

    const clearTimeoutSpy = vi.spyOn(globalThis, 'clearTimeout');

    // Act
    const {unmount} = renderHook(() => useSlowQuery(mockQuery));

    unmount();

    // Assert - clearTimeout should have been called
    expect(clearTimeoutSpy).toHaveBeenCalled();
  });

  test('computes query hash using asQueryInternals', () => {
    // Arrange
    const mockData = [{id: '1'}];
    const hashFn = vi.fn(() => 'test-hash-123');
    const mockResult = {type: 'complete' as const};
    vi.mocked(useQuery).mockReturnValue([mockData, mockResult]);
    // oxlint-disable-next-line no-explicit-any
    vi.mocked(asQueryInternals).mockReturnValue({hash: hashFn} as any);

    // Act
    renderHook(() => useSlowQuery(mockQuery));

    // Assert - asQueryInternals should be called to compute hash
    expect(vi.mocked(asQueryInternals)).toHaveBeenCalled();
    expect(hashFn).toHaveBeenCalled();
  });

  test('handles empty array correctly', () => {
    // Arrange
    const mockData: Array<{id: string}> = [];
    setupMocks(mockData);

    // Act
    const {result} = renderHook(() => useSlowQuery(mockQuery));

    // Assert - empty array should return empty array
    expect(result.current).toEqual([[], {type: 'unknown'}]);
  });

  test('handles single item array correctly', () => {
    // Arrange
    const mockData = [{id: '1'}];
    setupMocks(mockData);

    // Act
    const {result} = renderHook(() => useSlowQuery(mockQuery));

    // Assert - floor(1 / 2) = 0 items
    expect(result.current).toEqual([[], {type: 'unknown'}]);
  });

  test('multiple queries with different hashes are tracked independently', async () => {
    // Arrange
    const mockData1 = [{id: '1'}, {id: '2'}];
    const mockData2 = [{id: 'a'}, {id: 'b'}];
    const mockResult = {type: 'complete' as const};

    const query1 = mockQuery;
    // oxlint-disable-next-line no-explicit-any
    const query2 = {table: 'test', where: {other: true}} as any;

    // Set up useQuery to return data based on which query is being used
    vi.mocked(useQuery).mockImplementation(query => {
      const queryStr = JSON.stringify(query);
      if (queryStr === JSON.stringify(query1)) {
        return [mockData1, mockResult];
      }
      return [mockData2, mockResult];
    });

    // Set up asQueryInternals to return hash based on query
    vi.mocked(asQueryInternals).mockImplementation(
      query =>
        ({
          hash: () => `hash-${JSON.stringify(query)}`,
          // oxlint-disable-next-line no-explicit-any
        }) as any,
    );

    // Query 1
    const {result: result1} = renderHook(() => useSlowQuery(query1));

    // Query 2
    const {result: result2} = renderHook(() => useSlowQuery(query2));

    // Both should be delayed initially
    expect(result1.current[1]).toEqual({type: 'unknown'});
    expect(result2.current[1]).toEqual({type: 'unknown'});

    // Advance time to complete both queries
    await vi.advanceTimersByTimeAsync(TESTING_SLOW_COMPLETE_DELAY_MS);

    // Both should complete (they were started at the same time in test)
    expect(result1.current).toEqual([mockData1, mockResult]);
    expect(result2.current).toEqual([mockData2, mockResult]);
  });

  test('completed query with same hash does not re-delay on re-render', async () => {
    // Arrange
    const mockData = [{id: '1'}, {id: '2'}, {id: '3'}, {id: '4'}];
    const mockResult = {type: 'complete' as const};
    const hash = 'consistent-hash-123';
    setupMocks(mockData, 'complete', hash);

    // Act - initial render
    const {result, rerender} = renderHook(() => useSlowQuery(mockQuery));

    // Initially should return half data with unknown type
    expect(result.current).toEqual([[{id: '1'}, {id: '2'}], {type: 'unknown'}]);

    // Advance time to complete the query
    await vi.advanceTimersByTimeAsync(TESTING_SLOW_COMPLETE_DELAY_MS);

    // Should now return full data with complete type
    expect(result.current).toEqual([mockData, mockResult]);

    // Re-render with same query (same hash)
    rerender();

    // Should immediately return full data without re-delaying
    expect(result.current).toEqual([mockData, mockResult]);

    // Even if we advance time, it should stay complete (no new timer started)
    await vi.advanceTimersByTimeAsync(TESTING_SLOW_COMPLETE_DELAY_MS);
    expect(result.current).toEqual([mockData, mockResult]);
  });

  test('completed query stays complete when component remounts with same hash', async () => {
    // Arrange
    const mockData = [{id: '1'}, {id: '2'}, {id: '3'}, {id: '4'}];
    const mockResult = {type: 'complete' as const};
    const hash = 'persistent-hash-456';
    setupMocks(mockData, 'complete', hash);

    // First mount
    const {result: result1, unmount} = renderHook(() =>
      useSlowQuery(mockQuery),
    );

    // Initially delayed
    expect(result1.current[1]).toEqual({type: 'unknown'});

    // Complete the query
    await vi.advanceTimersByTimeAsync(TESTING_SLOW_COMPLETE_DELAY_MS);
    expect(result1.current).toEqual([mockData, mockResult]);

    // Unmount
    unmount();

    // Remount with same query hash
    const {result: result2} = renderHook(() => useSlowQuery(mockQuery));

    // Should immediately be complete (no delay)
    expect(result2.current).toEqual([mockData, mockResult]);
  });

  test('new query hash gets delayed even if previous different hash completed', async () => {
    // Arrange
    const mockData1 = [{id: '1'}, {id: '2'}];
    const mockData2 = [{id: 'a'}, {id: 'b'}];
    const mockResult = {type: 'complete' as const};
    // oxlint-disable-next-line no-explicit-any
    const mockQuery1 = {table: 'test1', where: {}} as any;
    // oxlint-disable-next-line no-explicit-any
    const mockQuery2 = {table: 'test2', where: {}} as any;

    vi.mocked(useQuery).mockReturnValue([mockData1, mockResult]);
    // oxlint-disable-next-line no-explicit-any
    vi.mocked(useZero).mockReturnValue({context: undefined} as any);

    let currentHash = 'first-hash';
    // oxlint-disable-next-line no-explicit-any
    vi.mocked(addContextToQuery).mockImplementation(query => query as any);
    vi.mocked(asQueryInternals).mockImplementation(
      () =>
        ({
          hash: () => currentHash,
          // oxlint-disable-next-line no-explicit-any
        }) as any,
    );

    // Act - first query
    const {result, rerender} = renderHook(({query}) => useSlowQuery(query), {
      initialProps: {query: mockQuery1},
    });

    // Initially delayed
    expect(result.current[1]).toEqual({type: 'unknown'});

    // Complete first query
    await vi.advanceTimersByTimeAsync(TESTING_SLOW_COMPLETE_DELAY_MS);
    expect(result.current).toEqual([mockData1, mockResult]);

    // Change to new query with different hash
    currentHash = 'second-hash';
    vi.mocked(useQuery).mockReturnValue([mockData2, mockResult]);
    rerender({query: mockQuery2});

    // New hash should be delayed
    expect(result.current).toEqual([[{id: 'a'}], {type: 'unknown'}]);

    // Complete second query
    await vi.advanceTimersByTimeAsync(TESTING_SLOW_COMPLETE_DELAY_MS);
    expect(result.current).toEqual([mockData2, mockResult]);
  });
});
