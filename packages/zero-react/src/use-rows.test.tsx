import {renderHook, waitFor} from '@testing-library/react';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {
  MockSocket,
  zeroForTest,
} from '../../zero-client/src/client/test-utils.ts';
import {
  getPageQuery,
  getSingleQuery,
  type Item,
  mutators,
  schema,
  type StartRow,
  toStartRow,
} from './test-helpers.ts';
import {useRows} from './use-rows.ts';
import {ZeroProvider} from './zero-provider.tsx';

describe('useRows', () => {
  const createTestZero = () =>
    zeroForTest({
      kvStore: 'mem',
      schema,
      mutators,
    });
  let z: ReturnType<typeof createTestZero>;

  // Helper function to render useRows with common configuration
  const renderUseRows = (
    pageSize: number,
    anchor: Parameters<typeof useRows<Item, StartRow>>[0]['anchor'],
    zero = z,
  ) =>
    renderHook(
      () =>
        useRows<Item, StartRow>({
          pageSize,
          anchor,
          getPageQuery,
          getSingleQuery,
          toStartRow,
        }),
      {
        wrapper: ({children}) => (
          <ZeroProvider zero={zero}>{children}</ZeroProvider>
        ),
      },
    );

  beforeEach(async () => {
    vi.stubGlobal('WebSocket', MockSocket as unknown as typeof WebSocket);

    z = createTestZero();

    // Establish mock connection
    void z.triggerConnected();

    // Populate 1000 rows for pagination testing
    await z.mutate(mutators.populateItems({count: 1000})).client;
  });

  afterEach(async () => {
    await z.close();
    vi.restoreAllMocks();
  });

  test('forward pagination from start', async () => {
    const {result} = renderUseRows(100, {kind: 'forward', index: 0});

    // Wait for data to load
    await waitFor(() => {
      expect(result.current?.rowsLength).toBe(100);
    });

    // Small delay to ensure queries are fully registered
    await new Promise(resolve => setTimeout(resolve, 50));

    // Mark queries as complete
    await z.markAllQueriesAsGot();

    // Wait for complete status
    await waitFor(() => {
      expect(result.current?.complete).toBe(true);
    });

    // Check initial state
    expect(result.current?.rowsEmpty).toBe(false);
    expect(result.current?.atStart).toBe(true);
    expect(result.current?.atEnd).toBe(false);
    expect(result.current?.firstRowIndex).toBe(0);

    // Verify rows
    expect(result.current?.rowAt(0)?.id).toBe('1');
    expect(result.current?.rowAt(99)?.id).toBe('100');
    expect(result.current?.rowAt(100)).toBeUndefined();
  });

  test('forward pagination - second page', async () => {
    const {result} = renderUseRows(100, {
      kind: 'forward',
      index: 100,
      startRow: {name: 'Item 0100'},
    });

    await waitFor(() => {
      expect(result.current?.rowsLength).toBe(100);
    });

    // Mark queries as complete
    await z.markAllQueriesAsGot();

    await waitFor(() => {
      expect(result.current?.complete).toBe(true);
    });

    expect(result.current?.atStart).toBe(false);
    expect(result.current?.atEnd).toBe(false);
    expect(result.current?.firstRowIndex).toBe(100);

    expect(result.current?.rowAt(100)?.id).toBe('101');
    expect(result.current?.rowAt(199)?.id).toBe('200');
  });

  test('forward pagination - last page', async () => {
    const {result} = renderUseRows(100, {
      kind: 'forward',
      index: 900,
      startRow: {name: 'Item 0900'},
    });

    await waitFor(() => {
      expect(result.current?.rowsLength).toBe(100);
    });

    // Mark queries as complete
    await z.markAllQueriesAsGot();

    await waitFor(() => {
      expect(result.current?.complete).toBe(true);
    });

    expect(result.current?.atStart).toBe(false);
    expect(result.current?.firstRowIndex).toBe(900);
    expect(result.current?.rowAt(900)?.id).toBe('901');
    expect(result.current?.rowAt(999)?.id).toBe('1000');
  });

  test('backward pagination', async () => {
    const {result} = renderUseRows(100, {
      kind: 'backward',
      index: 500,
      startRow: {name: 'Item 0500'},
    });

    await waitFor(() => {
      expect(result.current?.rowsLength).toBeGreaterThan(0);
    });

    // Mark queries as complete
    await z.markAllQueriesAsGot();

    await waitFor(() => {
      expect(result.current?.complete).toBe(true);
    });

    expect(result.current?.atStart).toBe(false);
    expect(result.current?.atEnd).toBe(false);

    // With backward anchor, we load backward from index 500
    expect(result.current?.firstRowIndex).toBeLessThanOrEqual(500);
  });

  test('empty result set', async () => {
    const z2 = zeroForTest({
      kvStore: 'mem',
      schema,
      mutators,
    });

    void z2.triggerConnected();

    const {result} = renderUseRows(100, {kind: 'forward', index: 0}, z2);

    await waitFor(() => {
      expect(result.current).not.toBeNull();
    });

    // Mark queries as complete
    await z2.markAllQueriesAsGot();

    await waitFor(() => {
      expect(result.current?.complete).toBe(true);
    });

    expect(result.current?.rowsLength).toBe(0);
    expect(result.current?.rowsEmpty).toBe(true);
    expect(result.current?.atStart).toBe(true);

    await z2.close();
  });

  test('permalink anchor - item in middle', async () => {
    const {result} = renderUseRows(
      10, // Must be even for permalink
      {kind: 'permalink', index: 500, id: '500'},
    );

    // Wait for data to load
    await waitFor(() => expect(result.current.rowsLength).toBeGreaterThan(0));

    await z.markAllQueriesAsGot();

    await waitFor(() => expect(result.current.complete).toBe(true));

    // Permalink should center around the anchor item
    // With pageSize=10, we get 5 items before and 4 items after the anchor
    expect(result.current.rowsLength).toBe(10);
    expect(result.current.firstRowIndex).toBe(495); // 500 - 5
    expect(result.current.permalinkNotFound).toBe(false);

    // Check the anchor item (ID 500 = "Item 0500")
    expect(result.current.rowAt(500)).toEqual({
      id: '500',
      name: 'Item 0500',
    });

    // Check items before
    expect(result.current.rowAt(495)).toEqual({
      id: '495',
      name: 'Item 0495',
    });
    expect(result.current.rowAt(499)).toEqual({
      id: '499',
      name: 'Item 0499',
    });

    // Check items after
    expect(result.current.rowAt(501)).toEqual({
      id: '501',
      name: 'Item 0501',
    });
    expect(result.current.rowAt(504)).toEqual({
      id: '504',
      name: 'Item 0504',
    });

    // Out of range should be undefined
    expect(result.current.rowAt(494)).toBeUndefined();
    expect(result.current.rowAt(505)).toBeUndefined();

    expect(result.current.atStart).toBe(false);
    expect(result.current.atEnd).toBe(false);
  });

  test('permalink anchor - item near start', async () => {
    const {result} = renderUseRows(10, {kind: 'permalink', index: 2, id: '3'});

    await waitFor(() => expect(result.current.rowsLength).toBeGreaterThan(0));

    await new Promise(resolve => setTimeout(resolve, 50));
    await z.markAllQueriesAsGot();

    await waitFor(() => expect(result.current.complete).toBe(true));

    // Near start, we have 2 items before (IDs 1, 2) and anchor at ID 3 (index 2)
    expect(result.current.rowsLength).toBe(7); // 2 before + anchor + 4 after
    expect(result.current.firstRowIndex).toBe(0);
    expect(result.current.atStart).toBe(true);
    expect(result.current.atEnd).toBe(false);

    expect(result.current.rowAt(0)).toEqual({id: '1', name: 'Item 0001'});
    expect(result.current.rowAt(1)).toEqual({id: '2', name: 'Item 0002'});
    expect(result.current.rowAt(2)).toEqual({id: '3', name: 'Item 0003'});
    expect(result.current.rowAt(6)).toEqual({id: '7', name: 'Item 0007'});
  });

  test('permalink anchor - item near end', async () => {
    const {result} = renderUseRows(10, {
      kind: 'permalink',
      index: 997,
      id: '998',
    });

    await waitFor(() => expect(result.current.rowsLength).toBeGreaterThan(0));

    await z.markAllQueriesAsGot();

    await waitFor(() => expect(result.current.complete).toBe(true));

    // Near end, we have 5 items before and anchor (ID 998 at index 997), but only 2 items after (IDs 999, 1000)
    expect(result.current.rowsLength).toBe(8); // 5 before + anchor + 2 after
    expect(result.current.firstRowIndex).toBe(992); // 997 - 5
    expect(result.current.atStart).toBe(false);
    expect(result.current.atEnd).toBe(true);

    expect(result.current.rowAt(992)).toEqual({
      id: '993',
      name: 'Item 0993',
    });
    expect(result.current.rowAt(997)).toEqual({
      id: '998',
      name: 'Item 0998',
    });
    expect(result.current.rowAt(999)).toEqual({
      id: '1000',
      name: 'Item 1000',
    });
  });

  test('permalink anchor - item not found', async () => {
    const {result} = renderUseRows(10, {
      kind: 'permalink',
      index: 500,
      id: '9999',
    });

    // Initial state
    expect(result.current.rowsLength).toBe(0);
    expect(result.current.complete).toBe(false);

    await z.markAllQueriesAsGot();

    // Wait for the item query to complete (which returns undefined)
    await waitFor(() => expect(result.current.complete).toBe(true));

    expect(result.current.permalinkNotFound).toBe(true);
    expect(result.current.rowsLength).toBe(0);
    expect(result.current.rowAt(500)).toBeUndefined();
  });

  test('reactive updates - adding new items', async () => {
    const {result} = renderUseRows(10, {kind: 'forward', index: 0});

    // Wait for initial data (10 items)
    await waitFor(() => expect(result.current?.rowsLength).toBe(10));

    await z.markAllQueriesAsGot();

    await waitFor(() => expect(result.current?.complete).toBe(true));

    // Verify initial items
    expect(result.current.rowAt(0)?.id).toBe('1');
    expect(result.current.rowAt(9)?.id).toBe('10');

    // Add new items that should appear at the start of the query results
    // Use names that sort before "Item 0001"
    await z.mutate(mutators.addItem({id: '1001', name: 'Item 0000'})).client;
    await z.mutate(mutators.addItem({id: '1002', name: 'Item 00005'})).client;

    // Wait for the hook to react to the new data
    // The new items should appear at the start since they sort before existing items
    await waitFor(() => {
      const firstItem = result.current.rowAt(0);
      return firstItem?.id === '1001';
    });

    // Verify the new items are included and properly sorted
    expect(result.current.rowsLength).toBe(10);
    expect(result.current.rowAt(0)).toEqual({
      id: '1001',
      name: 'Item 0000',
    });
    expect(result.current.rowAt(1)).toEqual({
      id: '1002',
      name: 'Item 00005',
    });
    expect(result.current.rowAt(2)).toEqual({id: '1', name: 'Item 0001'});

    // Original items shifted down - item 10 and 9 should be pushed out
    expect(result.current.rowAt(9)).toEqual({id: '8', name: 'Item 0008'});
    expect(result.current.rowAt(10)).toBeUndefined();
  });

  test('reactive updates - backward pagination', async () => {
    const {result} = renderUseRows(10, {
      kind: 'backward',
      index: 500,
      startRow: {name: 'Item 0500'},
    });

    // Wait for initial data
    await waitFor(() => expect(result.current?.rowsLength).toBeGreaterThan(0));

    await z.markAllQueriesAsGot();

    await waitFor(() => expect(result.current?.complete).toBe(true));

    const initialFirstIndex = result.current.firstRowIndex;
    const initialLength = result.current.rowsLength;

    // Add items that should appear in the backward pagination range
    // These items sort in the range we're viewing (around index 500, going backward)
    await z.mutate(mutators.addItem({id: '1001', name: 'Item 04965'})).client;
    await z.mutate(mutators.addItem({id: '1002', name: 'Item 04975'})).client;

    // Wait for reactive update
    await waitFor(() => {
      const itemIds = Array.from({length: result.current.rowsLength}, (_, i) =>
        result.current.rowAt(initialFirstIndex + i),
      ).map(item => item?.id);
      return itemIds.includes('1001');
    });

    // The new items should be included in the backward pagination
    expect(result.current.rowsLength).toBe(initialLength);
    // Verify the new items appear in the range
    const itemIds = Array.from({length: result.current.rowsLength}, (_, i) =>
      result.current.rowAt(initialFirstIndex + i),
    ).map(item => item?.id);
    expect(itemIds).toContain('1001');
    expect(itemIds).toContain('1002');
  });

  test('reactive updates - permalink anchor', async () => {
    const {result} = renderUseRows(10, {
      kind: 'permalink',
      index: 500,
      id: '501',
    });

    // Wait for data to load
    await waitFor(() => expect(result.current.rowsLength).toBeGreaterThan(0));

    await z.markAllQueriesAsGot();

    await waitFor(() => expect(result.current.complete).toBe(true));

    // Verify the permalink item is centered
    expect(result.current.permalinkNotFound).toBe(false);
    expect(result.current.rowAt(500)).toEqual({id: '501', name: 'Item 0501'});

    const initialFirstIndex = result.current.firstRowIndex;

    // Add items that sort near the permalink item (around Item 0501)
    await z.mutate(mutators.addItem({id: '1001', name: 'Item 04985'})).client;
    await z.mutate(mutators.addItem({id: '1002', name: 'Item 04995'})).client;

    // Wait for reactive update
    await waitFor(() => {
      // New items should appear in the visible range
      const itemIds = Array.from({length: result.current.rowsLength}, (_, i) =>
        result.current.rowAt(initialFirstIndex + i),
      ).map(item => item?.id);
      return itemIds.includes('1001');
    });

    // Verify the new items are included and the permalink item is still visible
    expect(result.current.permalinkNotFound).toBe(false);
    // The permalink item should still be accessible (though its index may have changed)
    const itemIds = Array.from({length: result.current.rowsLength}, (_, i) =>
      result.current.rowAt(initialFirstIndex + i),
    ).map(item => item?.id);
    expect(itemIds).toContain('501');
    expect(itemIds).toContain('1001');
    expect(itemIds).toContain('1002');
  });
});
