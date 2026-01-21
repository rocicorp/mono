import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import {
  MockSocket,
  zeroForTest,
} from '../../zero-client/src/client/test-utils.ts';
import {
  getPageQuery,
  getSingleQuery,
  mutators,
  schema,
  toStartRow,
  type Item,
} from './test-helpers.ts';
import { useZeroVirtualizer } from './use-zero-virtualizer.ts';
import { ZeroProvider } from './zero-provider.tsx';

// Mock wouter's useHistoryState since it needs browser history API
vi.mock('wouter/use-browser-location', () => ({
  useHistoryState: () => null,
}));

describe('useZeroVirtualizer', () => {
  const createTestZero = () =>
    zeroForTest({
      kvStore: 'mem',
      schema,
      mutators,
    });
  let z: ReturnType<typeof createTestZero>;

  // Mock scroll element
  let mockScrollElement: HTMLDivElement;

  beforeEach(async () => {
    vi.stubGlobal('WebSocket', MockSocket as unknown as typeof WebSocket);

    // Create a mock scroll element
    mockScrollElement = document.createElement('div');
    Object.defineProperty(mockScrollElement, 'scrollTop', {
      writable: true,
      value: 0,
    });
    Object.defineProperty(mockScrollElement, 'clientHeight', {
      value: 800,
    });
    document.body.appendChild(mockScrollElement);

    z = createTestZero();
    void z.triggerConnected();

    // Populate data for testing
    await z.mutate(mutators.populateItems({count: 1000})).client;
  });

  afterEach(async () => {
    document.body.removeChild(mockScrollElement);
    await z.close();
    vi.restoreAllMocks();
  });

  test('basic initialization', async () => {
    const {result} = renderHook(
      () =>
        useZeroVirtualizer({
          estimateSize: () => 50,
          getScrollElement: () => mockScrollElement,
          listContextParams: 'default',
          getPageQuery,
          getSingleQuery,
          toStartRow,
        }),
      {
        wrapper: ({children}) => (
          <ZeroProvider zero={z}>{children}</ZeroProvider>
        ),
      },
    );

    // Wait for initial data to load
    await waitFor(() => {
      expect(result.current.complete).toBe(false);
    });

    await z.markAllQueriesAsGot();

    await waitFor(() => {
      expect(result.current.complete).toBe(true);
    });

    // Verify basic state
    expect(result.current.rowsEmpty).toBe(false);
    expect(result.current.permalinkNotFound).toBe(false);
    expect(result.current.virtualizer).toBeDefined();
    expect(result.current.estimatedTotal).toBeGreaterThan(0);

    // Verify we can access rows
    expect(result.current.rowAt(0)).toEqual({id: '1', name: 'Item 0001'});
  });

  test('permalink loading', async () => {
    const {result} = renderHook(
      () =>
        useZeroVirtualizer({
          estimateSize: () => 50,
          getScrollElement: () => mockScrollElement,
          listContextParams: 'default',
          permalinkID: '500',
          getPageQuery,
          getSingleQuery,
          toStartRow,
        }),
      {
        wrapper: ({children}) => (
          <ZeroProvider zero={z}>{children}</ZeroProvider>
        ),
      },
    );

    // Wait for data to load
    await waitFor(() => {
      expect(result.current.complete).toBe(false);
    });

    await z.markAllQueriesAsGot();

    await waitFor(() => {
      expect(result.current.complete).toBe(true);
    });

    // Verify permalink item is accessible
    expect(result.current.permalinkNotFound).toBe(false);

    // Find all loaded items - the permalink may be at any position after index adjustments
    const loadedItems: Item[] = [];
    for (let i = 0; i < result.current.virtualizer.getTotalSize(); i++) {
      const row = result.current.rowAt(i);
      if (row) {
        loadedItems.push(row);
      }
    }

    // The permalink item should be among the loaded items
    expect(loadedItems.some(item => item.id === '500')).toBe(true);
  });

  test('permalink not found', async () => {
    const {result} = renderHook(
      () =>
        useZeroVirtualizer({
          estimateSize: () => 50,
          getScrollElement: () => mockScrollElement,
          listContextParams: 'default',
          permalinkID: '9999',
          getPageQuery,
          getSingleQuery,
          toStartRow,
        }),
      {
        wrapper: ({children}) => (
          <ZeroProvider zero={z}>{children}</ZeroProvider>
        ),
      },
    );

    await z.markAllQueriesAsGot();

    await waitFor(() => {
      expect(result.current.complete).toBe(true);
    });

    expect(result.current.permalinkNotFound).toBe(true);
    expect(result.current.rowsEmpty).toBe(true);
  });

  test('empty result set', async () => {
    const z2 = zeroForTest({
      kvStore: 'mem',
      schema,
      mutators,
    });

    void z2.triggerConnected();

    const {result} = renderHook(
      () =>
        useZeroVirtualizer({
          estimateSize: () => 50,
          getScrollElement: () => mockScrollElement,
          listContextParams: 'default',
          getPageQuery,
          getSingleQuery,
          toStartRow,
        }),
      {
        wrapper: ({children}) => (
          <ZeroProvider zero={z2}>{children}</ZeroProvider>
        ),
      },
    );

    await z2.markAllQueriesAsGot();

    await waitFor(() => {
      expect(result.current.complete).toBe(true);
    });

    expect(result.current.rowsEmpty).toBe(true);
    // Estimated total stays at 1 (loading skeleton) since empty results don't update it
    expect(result.current.estimatedTotal).toBe(1);
    // Total is 1 since both ends are reached (atStart and atEnd are true for empty results)
    expect(result.current.total).toBe(1);

    await z2.close();
  });

  test('list context change resets state', async () => {
    const {result, rerender} = renderHook(
      ({listContextParams}: {listContextParams: string}) =>
        useZeroVirtualizer({
          estimateSize: () => 50,
          getScrollElement: () => mockScrollElement,
          listContextParams,
          getPageQuery,
          getSingleQuery,
          toStartRow,
        }),
      {
        initialProps: {listContextParams: 'filter1'},
        wrapper: ({children}) => (
          <ZeroProvider zero={z}>{children}</ZeroProvider>
        ),
      },
    );

    // Wait for initial load
    await waitFor(() => {
      expect(result.current.complete).toBe(false);
    });

    await z.markAllQueriesAsGot();

    await waitFor(() => {
      expect(result.current.complete).toBe(true);
    });

    const initialEstimatedTotal = result.current.estimatedTotal;
    expect(initialEstimatedTotal).toBeGreaterThan(0);

    // Change list context (e.g., applying a filter)
    rerender({listContextParams: 'filter2'});

    // Wait for state to potentially update
    await new Promise(resolve => setTimeout(resolve, 100));

    await z.markAllQueriesAsGot();

    await waitFor(
      () => {
        expect(result.current.complete).toBe(true);
      },
      {timeout: 2000},
    );

    // Should still have data (mock doesn't actually filter)
    expect(result.current.rowsEmpty).toBe(false);
  });

  test('virtualizer count includes loading skeleton', async () => {
    const {result} = renderHook(
      () =>
        useZeroVirtualizer({
          estimateSize: () => 50,
          getScrollElement: () => mockScrollElement,
          listContextParams: 'default',
          getPageQuery,
          getSingleQuery,
          toStartRow,
        }),
      {
        wrapper: ({children}) => (
          <ZeroProvider zero={z}>{children}</ZeroProvider>
        ),
      },
    );

    await waitFor(() => {
      expect(result.current.complete).toBe(false);
    });

    await z.markAllQueriesAsGot();

    await waitFor(() => {
      expect(result.current.complete).toBe(true);
    });

    // Virtualizer count should include skeleton row at end if not at end
    const virtualizerCount = result.current.virtualizer.options.count;
    const estimatedTotal = result.current.estimatedTotal;

    // Since we haven't reached the end, count should be estimatedTotal + 1 (skeleton)
    expect(virtualizerCount).toBe(estimatedTotal + 1);
  });

  test('total is undefined until both ends reached', async () => {
    const {result} = renderHook(
      () =>
        useZeroVirtualizer({
          estimateSize: () => 50,
          getScrollElement: () => mockScrollElement,
          listContextParams: 'default',
          getPageQuery,
          getSingleQuery,
          toStartRow,
        }),
      {
        wrapper: ({children}) => (
          <ZeroProvider zero={z}>{children}</ZeroProvider>
        ),
      },
    );

    await waitFor(() => {
      expect(result.current.complete).toBe(false);
    });

    await z.markAllQueriesAsGot();

    await waitFor(() => {
      expect(result.current.complete).toBe(true);
    });

    // Initially at start but not at end
    expect(result.current.total).toBeUndefined();
  });

  test('estimated total increases as data loads', async () => {
    const {result} = renderHook(
      () =>
        useZeroVirtualizer({
          estimateSize: () => 50,
          getScrollElement: () => mockScrollElement,
          listContextParams: 'default',
          getPageQuery,
          getSingleQuery,
          toStartRow,
        }),
      {
        wrapper: ({children}) => (
          <ZeroProvider zero={z}>{children}</ZeroProvider>
        ),
      },
    );

    // Initial estimated total should be small (loading skeleton)
    const initialEstimatedTotal = result.current.estimatedTotal;
    expect(initialEstimatedTotal).toBeLessThanOrEqual(1);

    await waitFor(() => {
      expect(result.current.complete).toBe(false);
    });

    await z.markAllQueriesAsGot();

    await waitFor(() => {
      expect(result.current.complete).toBe(true);
    });

    // Estimated total should increase after data loads
    expect(result.current.estimatedTotal).toBeGreaterThan(
      initialEstimatedTotal,
    );
  });

  test('rowAt returns correct items', async () => {
    const {result} = renderHook(
      () =>
        useZeroVirtualizer({
          estimateSize: () => 50,
          getScrollElement: () => mockScrollElement,
          listContextParams: 'default',
          getPageQuery,
          getSingleQuery,
          toStartRow,
        }),
      {
        wrapper: ({children}) => (
          <ZeroProvider zero={z}>{children}</ZeroProvider>
        ),
      },
    );

    await waitFor(() => {
      expect(result.current.complete).toBe(false);
    });

    await z.markAllQueriesAsGot();

    await waitFor(() => {
      expect(result.current.complete).toBe(true);
    });

    // Test rowAt function
    expect(result.current.rowAt(0)).toEqual({id: '1', name: 'Item 0001'});
    expect(result.current.rowAt(1)).toEqual({id: '2', name: 'Item 0002'});
    expect(result.current.rowAt(99)).toEqual({id: '100', name: 'Item 0100'});

    // Out of range should be undefined
    expect(result.current.rowAt(10000)).toBeUndefined();
  });

  test('TanStack Virtual options are forwarded', async () => {
    let observeElementOffsetCalled = false;
    let observeElementRectCalled = false;

    const {result} = renderHook(
      () =>
        useZeroVirtualizer({
          estimateSize: () => 50,
          getScrollElement: () => mockScrollElement,
          listContextParams: 'default',
          getPageQuery,
          getSingleQuery,
          toStartRow,
          observeElementOffset: (_instance, cb) => {
            observeElementOffsetCalled = true;
            cb(0, false);
            return () => undefined;
          },
          observeElementRect: (_instance, cb) => {
            observeElementRectCalled = true;
            cb({height: 800, width: 400});
            return () => undefined;
          },
        }),
      {
        wrapper: ({children}) => (
          <ZeroProvider zero={z}>{children}</ZeroProvider>
        ),
      },
    );

    // Wait for hook to initialize
    await waitFor(() => {
      expect(result.current.virtualizer).toBeDefined();
    });

    // Verify custom callbacks were called
    expect(observeElementOffsetCalled).toBe(true);
    expect(observeElementRectCalled).toBe(true);
  });

  /* TODO: Fix complex scrolling test - custom callbacks interfere with paging logic
  test('scrolling down updates visible items correctly', async () => {
    const estimateSize = 50;
    const toItemName = (index: number) =>
      `Item ${String(index + 1).padStart(4, '0')}`;

    const z = createTestZero();
    void z.triggerConnected();
    await z.mutate(mutators.populateItems({count: 250})).client;

    const scrollElement = {
      scrollTop: 0,
      clientHeight: 800,
      scrollHeight: 800,
    } as unknown as Element;

    let offsetCallback:
      | ((offset: number, isScrolling: boolean) => void)
      | null = null;

    const getPageQuerySpy = vi.fn(getPageQuery);

    const {result} = renderHook(
      () =>
        useZeroVirtualizer({
          estimateSize: () => estimateSize,
          getScrollElement: () => scrollElement,
          listContextParams: 'default',
          getPageQuery: getPageQuerySpy,
          getSingleQuery,
          toStartRow,
          overscan: 0,
          initialRect: {height: 800, width: 400},
          observeElementRect: (_instance, cb) => {
            cb({height: 800, width: 400});
            return () => undefined;
          },
          observeElementOffset: (_instance, cb) => {
            offsetCallback = cb;
            cb(scrollElement.scrollTop, false);
            return () => {
              offsetCallback = null;
            };
          },
          scrollToFn: (offset, _options, _instance) => {
            scrollElement.scrollTop = offset;
            offsetCallback?.(offset, true);
            offsetCallback?.(offset, false);
          },
        }),
      {
        wrapper: ({children}) => (
          <ZeroProvider zero={z}>{children}</ZeroProvider>
        ),
      },
    );

    await z.markAllQueriesAsGot();

    await waitFor(() => {
      expect(result.current.complete).toBe(true);
    });

    expect(getPageQuerySpy).toHaveBeenCalledWith(101, null, 'forward');

    const updateScrollHeight = () => {
      const estimatedHeight =
        result.current.virtualizer.options.count * estimateSize;
      (scrollElement as {scrollHeight: number}).scrollHeight = Math.max(
        scrollElement.clientHeight,
        estimatedHeight,
      );
    };

    result.current.virtualizer.scrollRect = {
      height: 800,
      width: 400,
    };

    await waitFor(() => {
      result.current.virtualizer.measure();
      updateScrollHeight();
      expect(
        result.current.virtualizer.getVirtualItems().length,
      ).toBeGreaterThan(0);
    });

    const initialQueryCount = getPageQuerySpy.mock.calls.length;

    // Incrementally scroll down to trigger paging and test that visible items match scroll position
    const scrollStep = 1000;
    const maxScrollAttempts = 10; // Reduced - we just need to trigger some paging

    // Test Y position in viewport (e.g., 100px from top of viewport)
    const testYPosition = 100;

    for (let attempt = 0; attempt < maxScrollAttempts; attempt++) {
      updateScrollHeight();

      const currentScrollTop = scrollElement.scrollTop;
      const currentScrollHeight = (scrollElement as {scrollHeight: number})
        .scrollHeight;
      const maxScrollTop = Math.max(
        0,
        currentScrollHeight - scrollElement.clientHeight,
      );
      const newScrollTop = Math.min(
        currentScrollTop + scrollStep,
        maxScrollTop,
      );

      // Use the scrollToFn which properly updates scrollTop and calls offsetCallback
      result.current.virtualizer.scrollToOffset(newScrollTop);
      result.current.virtualizer.measure();

      // Force virtualizer to compute range with new scroll position
      result.current.virtualizer.getVirtualItems();

      await z.markAllQueriesAsGot();

      // Verify that item at Y position matches expected row
      // Expected row index = floor((scrollTop + Y) / estimateSize)
      const expectedRowIndex = Math.floor(
        (newScrollTop + testYPosition) / estimateSize,
      );
      const expectedItem = result.current.rowAt(expectedRowIndex);

      // Verify the item exists and has the expected name
      if (expectedItem) {
        expect(expectedItem.name).toBe(toItemName(expectedRowIndex));
      }

      // If we can't scroll further, we're done
      if (newScrollTop >= maxScrollTop && newScrollTop === currentScrollTop) {
        break;
      }
    }

    // Verify that paging was triggered
    await waitFor(() => {
      expect(getPageQuerySpy.mock.calls.length).toBeGreaterThan(
        initialQueryCount,
      );
    });

    await waitFor(() => {
      expect(result.current.complete).toBe(true);
    });

    // Verify all page queries are using the correct parameters
    const calls = getPageQuerySpy.mock.calls;
    expect(calls.length).toBeGreaterThan(0);

    // First query should have null start (initial load from top)
    expect(calls[0][0]).toBe(101); // limit
    expect(calls[0][1]).toBeNull(); // start
    expect(calls[0][2]).toBe('forward'); // direction

    // Check all queries have correct limit and direction
    for (const [limit, start, dir] of calls) {
      expect(limit).toBe(101);
      expect(dir).toBe('forward');

      // If start is provided, verify it has correct format
      if (start) {
        expect(start.name).toMatch(/^Item \d{4}$/);
      }
    }

    // Verify start rows are in increasing order (forward pagination)
    const startIndices: number[] = [];
    for (const [, start] of calls) {
      if (start) {
        const index = Number(start.name.slice('Item '.length)) - 1;
        startIndices.push(index);
      }
    }

    // Each start should be greater than or equal to the previous (forward paging)
    for (let i = 1; i < startIndices.length; i++) {
      expect(startIndices[i]).toBeGreaterThanOrEqual(startIndices[i - 1]);
    }

    await z.close();
  });
  */
  /* TODO: Fix ReactDOM rendering test
  test('basic ReactDOM rendering', async () => {
    const estimateSize = 50;

    const z = createTestZero();
    void z.triggerConnected();
    await z.mutate(mutators.populateItems({count: 50})).client;

    const getPageQuerySpy = vi.fn(getPageQuery);

    function VirtualList() {
      const {virtualizer, rowAt} = useZeroVirtualizer({
        estimateSize: () => estimateSize,
        getScrollElement: () => document.getElementById('scroll-container'),
        listContextParams: 'default',
        getPageQuery: getPageQuerySpy,
        getSingleQuery,
        toStartRow,
        overscan: 0,
      });

      const virtualItems = virtualizer.getVirtualItems();

      return (
        <div
          id="scroll-container"
          style={{
            height: '800px',
            overflow: 'auto',
            position: 'relative',
          }}
        >
          <div
            style={{
              height: `${virtualizer.getTotalSize()}px`,
              position: 'relative',
            }}
          >
            {virtualItems.map(item => (
              <div
                key={item.key}
                data-index={item.index}
                style={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  width: '100%',
                  height: `${item.size}px`,
                  transform: `translateY(${item.start}px)`,
                }}
              >
                {rowAt(item.index)?.name ?? 'Loading...'}
              </div>
            ))}
          </div>
        </div>
      );
    }

    // Create a real DOM container and render with ReactDOM directly
    const container = document.createElement('div');
    document.body.appendChild(container);
    const root = createRoot(container);

    try {
      root.render(
        <ZeroProvider zero={z}>
          <VirtualList />
        </ZeroProvider>,
      );

      await z.markAllQueriesAsGot();

      // Wait for initial items to render
      await waitFor(() => {
        const items = container.querySelectorAll('[data-index]');
        expect(items.length).toBeGreaterThan(0);
      });

      // Verify first item rendered correctly
      const firstItem = container.querySelector('[data-index="0"]');
      expect(firstItem).toBeTruthy();
      expect(firstItem?.textContent).toBe('Item 0001');

      // Verify scroll container exists
      const scrollContainer = document.getElementById('scroll-container');
      expect(scrollContainer).toBeTruthy();
      expect(scrollContainer?.style.height).toBe('800px');

      // Verify initial page query was called
      expect(getPageQuerySpy).toHaveBeenCalled();
    } finally {
      root.unmount();
      document.body.removeChild(container);
      await z.close();
    }
  });
  */
});
