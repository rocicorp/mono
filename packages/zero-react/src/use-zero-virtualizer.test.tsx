import type {Virtualizer} from '@tanstack/react-virtual';
import {act, renderHook, waitFor} from '@testing-library/react';
import type {ReactNode} from 'react';
import type {Root} from 'react-dom/client';
import {createRoot} from 'react-dom/client';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
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
import type {UseZeroVirtualizerOptions} from './use-zero-virtualizer.ts';
import {useZeroVirtualizer} from './use-zero-virtualizer.ts';
import {ZeroProvider} from './zero-provider.tsx';

// Mock wouter's useHistoryState since it needs browser history API
vi.mock('wouter/use-browser-location', () => ({
  useHistoryState: () => null,
}));

// Helper components and utilities
function VirtualScrollContainer({
  result,
  virtualItems,
}: {
  result: {
    virtualizer: Virtualizer<HTMLElement, Element>;
    rowAt: (index: number) => Item | undefined;
    total: number | undefined;
    estimatedTotal: number;
  };
  virtualItems: ReturnType<
    Virtualizer<HTMLElement, Element>['getVirtualItems']
  >;
}) {
  return (
    <>
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
            height: `${result.virtualizer.getTotalSize()}px`,
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
              {result.rowAt(item.index)?.name ?? 'Loading...'}
            </div>
          ))}
        </div>
      </div>

      <div id="zero-virtualizer-total">{result.total}</div>
      <div id="zero-virtualizer-estimated-total">{result.estimatedTotal}</div>
    </>
  );
}

function createTestContainer() {
  const container = document.createElement('div');
  document.body.appendChild(container);
  return container;
}

function cleanupTestContainer(root: Root, container: HTMLElement) {
  root.unmount();
  document.body.removeChild(container);
}

describe('useZeroVirtualizer', () => {
  const createTestZero = () =>
    zeroForTest({
      kvStore: 'mem',
      schema,
      mutators,
      logLevel: 'debug',
    });
  let z: ReturnType<typeof createTestZero>;

  function createWrapper(zero: ReturnType<typeof createTestZero>) {
    return ({children}: {children: ReactNode}) => (
      <ZeroProvider zero={zero}>{children}</ZeroProvider>
    );
  }

  // Mock scroll element
  let mockScrollElement: HTMLDivElement;

  function createBaseHookOptions(
    overrides: Partial<
      UseZeroVirtualizerOptions<
        HTMLElement,
        Element,
        string,
        Item,
        typeof toStartRow extends (row: Item) => infer R ? R : never
      >
    > = {},
  ) {
    return {
      estimateSize: () => 50,
      getScrollElement: () => mockScrollElement,
      listContextParams: 'default',
      getPageQuery,
      getSingleQuery,
      toStartRow,
      ...overrides,
    };
  }

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
      () => useZeroVirtualizer(createBaseHookOptions()),
      {wrapper: createWrapper(z)},
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
      () => useZeroVirtualizer(createBaseHookOptions({permalinkID: '500'})),
      {wrapper: createWrapper(z)},
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
      () => useZeroVirtualizer(createBaseHookOptions({permalinkID: '9999'})),
      {wrapper: createWrapper(z)},
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
      () => useZeroVirtualizer(createBaseHookOptions()),
      {wrapper: createWrapper(z2)},
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
        useZeroVirtualizer(createBaseHookOptions({listContextParams})),
      {
        initialProps: {listContextParams: 'filter1'},
        wrapper: createWrapper(z),
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
      () => useZeroVirtualizer(createBaseHookOptions()),
      {wrapper: createWrapper(z)},
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
      () => useZeroVirtualizer(createBaseHookOptions()),
      {wrapper: createWrapper(z)},
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
      () => useZeroVirtualizer(createBaseHookOptions()),
      {wrapper: createWrapper(z)},
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
      () => useZeroVirtualizer(createBaseHookOptions()),
      {wrapper: createWrapper(z)},
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
        useZeroVirtualizer(
          createBaseHookOptions({
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
        ),
      {wrapper: createWrapper(z)},
    );

    // Wait for hook to initialize
    await waitFor(() => {
      expect(result.current.virtualizer).toBeDefined();
    });

    // Verify custom callbacks were called
    expect(observeElementOffsetCalled).toBe(true);
    expect(observeElementRectCalled).toBe(true);
  });

  test('scrolling down updates visible items correctly', async () => {
    const estimateSize = 25;
    const toItemName = (index: number) =>
      `Item ${String(index + 1).padStart(4, '0')}`;

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
        wrapper: ({children}: {children: ReactNode}) => (
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
      const totalSize = result.current.virtualizer.getTotalSize();
      (scrollElement as {scrollHeight: number}).scrollHeight = Math.max(
        scrollElement.clientHeight,
        totalSize,
      );
    };

    await waitFor(() => {
      result.current.virtualizer.measure();
      updateScrollHeight();
      expect(
        result.current.virtualizer.getVirtualItems().length,
      ).toBeGreaterThan(0);
    });

    // Incrementally scroll down to trigger paging and test that visible items match scroll position
    const scrollStep = 700;
    const maxScrollAttempts = 100;

    // Test Y position in viewport (e.g., 100px from top of viewport)
    const testYPosition = 100;

    for (let attempt = 0; attempt < maxScrollAttempts; attempt++) {
      updateScrollHeight();
      const currentScrollTop = scrollElement.scrollTop;
      const currentScrollHeight = scrollElement.scrollHeight;
      const maxScrollTop = Math.max(
        0,
        currentScrollHeight - scrollElement.clientHeight,
      );
      const newScrollTop = Math.min(
        currentScrollTop + scrollStep,
        maxScrollTop,
      );

      act(() => {
        result.current.virtualizer.scrollToOffset(newScrollTop);
        result.current.virtualizer.measure();
      });

      // Force virtualizer to compute range with new scroll position
      result.current.virtualizer.getVirtualItems();

      // Mark all queries repeatedly until complete
      await z.markAllQueriesAsGot();
      await waitFor(() => {
        expect(result.current.complete).toBe(true);
      });

      // Verify that item at Y position matches expected row
      // Expected row index = floor((scrollTop + Y) / estimateSize)
      const expectedRowIndex = Math.floor(
        (newScrollTop + testYPosition) / estimateSize,
      );
      const expectedItem = result.current.rowAt(expectedRowIndex);

      // Verify the item exists and has the expected name
      expect(expectedItem?.name).toBe(toItemName(expectedRowIndex));

      // If we can't scroll further, we're done
      if (newScrollTop >= maxScrollTop && newScrollTop === currentScrollTop) {
        break;
      }
    }

    expect(result.current.total).toBe(1000);

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
  });

  test('ReactDOM rendering and scrolling to do paging', async () => {
    const estimateSize = 50;
    const MIN_PAGE_SIZE = 100; // From use-zero-virtualizer.ts

    const getPageQuerySpy = vi.fn(getPageQuery);

    let offsetCallback:
      | ((offset: number, isScrolling: boolean) => void)
      | null = null;

    function VirtualList() {
      const {virtualizer, rowAt} = useZeroVirtualizer({
        estimateSize: () => estimateSize,
        getScrollElement: () => document.getElementById('scroll-container'),
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
          const scrollContainer = document.getElementById('scroll-container');
          if (scrollContainer) {
            cb(scrollContainer.scrollTop, false);
          }
          return () => {
            offsetCallback = null;
          };
        },
        scrollToFn: (offset, _options, _instance) => {
          const scrollContainer = document.getElementById('scroll-container');
          if (scrollContainer) {
            scrollContainer.scrollTop = offset;
            offsetCallback?.(offset, true);
            offsetCallback?.(offset, false);
          }
        },
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

      // Test scrolling behavior - need to access the virtualizer instance
      // We'll render a component that exposes it
      let virtualizerInstance!: Virtualizer<HTMLElement, Element>;

      function VirtualListWithRef() {
        const result = useZeroVirtualizer(
          createBaseHookOptions({
            getScrollElement: () => document.getElementById('scroll-container'),
            getPageQuery: getPageQuerySpy,
            overscan: 0,
          }),
        );

        virtualizerInstance = result.virtualizer;
        const virtualItems = result.virtualizer.getVirtualItems();

        return (
          <VirtualScrollContainer result={result} virtualItems={virtualItems} />
        );
      }

      // Re-render with the ref version
      root.render(
        <ZeroProvider zero={z}>
          <VirtualListWithRef />
        </ZeroProvider>,
      );

      await waitFor(() => {
        expect(virtualizerInstance).toBeTruthy();
      });

      // if (scrollContainer && virtualizerInstance) {
      // Scroll down in increments to trigger paging
      // Scroll all the way past the end to ensure we load everything
      const scrollStep = 1000;
      const maxScrollAttempts = 55; // Scroll 55,000px to go past the end (50,000px total height)

      // Formula for expected estimatedTotal based on paging behavior:
      // Initial page loads MIN_PAGE_SIZE items (100 when MIN_PAGE_SIZE=100)
      // The page size is calculated as: max(MIN_PAGE_SIZE, makeEven(ceil(scrollHeight / estimateSize) * 3))
      // With scrollHeight=800, estimateSize=50: max(100, makeEven(ceil(800/50) * 3)) = max(100, 48) = 100
      //
      // However, the actual paging load behavior results in chunks of ~60 items after the initial load.
      // This is because estimatedTotal is computed as firstRowIndex + rowsLength, where rowsLength
      // is based on the actual loaded data range, which depends on the visible range + overscan.
      // The virtualizer loads data in a pattern that results in ~60 item increments.
      const scrollHeight = 800; // From initialRect
      const itemsPerViewport = Math.ceil(scrollHeight / estimateSize);
      const viewportMultiplier = 3; // From use-zero-virtualizer.ts page size calculation
      const calculatedPageSize = Math.max(
        MIN_PAGE_SIZE,
        itemsPerViewport * viewportMultiplier,
      );
      // Observed: after initial MIN_PAGE_SIZE, additional chunks are ~60% of calculated page size
      const SUBSEQUENT_PAGE_SIZE = Math.ceil(calculatedPageSize * 0.6);
      // First additional page has 1 extra item (observed behavior)
      const FIRST_ADDITIONAL_PAGE_SIZE = SUBSEQUENT_PAGE_SIZE + 1;

      const computeExpectedEstimatedTotal = (index: number): number => {
        if (index < MIN_PAGE_SIZE) {
          return MIN_PAGE_SIZE;
        }
        const pageNumber =
          Math.floor((index - MIN_PAGE_SIZE) / SUBSEQUENT_PAGE_SIZE) + 1;
        return (
          MIN_PAGE_SIZE +
          FIRST_ADDITIONAL_PAGE_SIZE +
          (pageNumber - 1) * SUBSEQUENT_PAGE_SIZE
        );
      };

      for (let attempt = 0; attempt < maxScrollAttempts; attempt++) {
        const scrollOffset = (attempt + 1) * scrollStep;

        act(() => {
          virtualizerInstance.scrollToOffset(scrollOffset);
        });

        await z.markAllQueriesAsGot();

        // Wait for React to update with new data
        await waitFor(() => {
          virtualizerInstance.measure();
          const visibleItems = container.querySelectorAll('[data-index]');
          expect(visibleItems.length).toBeGreaterThan(0);
        });

        virtualizerInstance.measure();
        const visibleItems = container.querySelectorAll('[data-index]');
        expect(visibleItems.length).toBeGreaterThan(0);

        // Verify exact items based on scroll position
        // Expected first visible index = floor(scrollOffset / estimateSize)
        const expectedFirstVisibleIndex = Math.floor(
          scrollOffset / estimateSize,
        );

        // Only verify items if we're within the data range
        if (expectedFirstVisibleIndex < 1000) {
          await waitFor(() => {
            const expectedItem = container.querySelector(
              `[data-index="${expectedFirstVisibleIndex}"]`,
            );

            const expectedName = `Item ${String(expectedFirstVisibleIndex + 1).padStart(4, '0')}`;
            expect(expectedItem?.textContent).toBe(expectedName);
          });
        }

        // Check total and estimatedTotal
        const totalEl = document.getElementById('zero-virtualizer-total');
        const estimatedTotalEl = document.getElementById(
          'zero-virtualizer-estimated-total',
        );

        const estimatedTotal = Number(estimatedTotalEl?.textContent);

        // total should be undefined initially, then become exactly 1000 once we reach the end
        const totalText = totalEl?.textContent;
        if (totalText) {
          const total = Number(totalText);
          expect(total).toBe(1000);
          // Once we know the total, estimatedTotal should exactly match it
          expect(estimatedTotal).toBe(1000);
        } else {
          // Verify exact estimatedTotal using formula
          const expectedEstimatedTotal = Math.min(
            computeExpectedEstimatedTotal(expectedFirstVisibleIndex),
            1000,
          );
          expect(estimatedTotal).toBe(expectedEstimatedTotal);
        }
      }

      // After scrolling 40,000px (to item ~800), we should have reached the end
      const finalTotalEl = document.getElementById('zero-virtualizer-total');
      const finalTotalText = finalTotalEl?.textContent;
      const finalTotal = Number(finalTotalText);
      expect(finalTotal).toBe(1000);

      const finalEstimatedTotalEl = document.getElementById(
        'zero-virtualizer-estimated-total',
      );
      const finalEstimatedTotal = Number(finalEstimatedTotalEl?.textContent);
      expect(finalEstimatedTotal).toBe(1000);
    } finally {
      cleanupTestContainer(root, container);
    }
  });

  test('ReactDOM rendering with bidirectional scrolling', async () => {
    const getPageQuerySpy = vi.fn(getPageQuery);

    // Create a real DOM container and render with ReactDOM directly
    const container = createTestContainer();
    const root = createRoot(container);

    let virtualizerInstance!: Virtualizer<HTMLElement, Element>;

    function VirtualListWithRef() {
      const result = useZeroVirtualizer(
        createBaseHookOptions({
          getScrollElement: () => document.getElementById('scroll-container'),
          getPageQuery: getPageQuerySpy,
          overscan: 0,
        }),
      );

      virtualizerInstance = result.virtualizer;
      const virtualItems = result.virtualizer.getVirtualItems();

      return (
        <VirtualScrollContainer result={result} virtualItems={virtualItems} />
      );
    }

    try {
      root.render(
        <ZeroProvider zero={z}>
          <VirtualListWithRef />
        </ZeroProvider>,
      );

      await waitFor(() => {
        expect(virtualizerInstance).toBeTruthy();
      });

      await z.markAllQueriesAsGot();

      // Wait for initial items to render
      await waitFor(() => {
        const items = container.querySelectorAll('[data-index]');
        expect(items.length).toBeGreaterThan(0);
      });

      // Verify first item rendered correctly at the start
      await waitFor(() => {
        const firstItem = container.querySelector('[data-index="0"]');
        expect(firstItem?.textContent).toBe('Item 0001');
      });

      // Scroll down to bottom - need to scroll all the way to load all data
      const scrollStepDown = 1000;
      const maxScrollDown = 55; // Scroll 55,000px to past the bottom (50,000px total height)

      for (let attempt = 0; attempt < maxScrollDown; attempt++) {
        const scrollOffset = (attempt + 1) * scrollStepDown;

        act(() => {
          virtualizerInstance.scrollToOffset(scrollOffset);
        });

        await z.markAllQueriesAsGot();

        await waitFor(() => {
          virtualizerInstance.measure();
          const visibleItems = container.querySelectorAll('[data-index]');
          expect(visibleItems.length).toBeGreaterThan(0);
        });
      }

      // Verify we reached the end - wait for total to be set
      await waitFor(() => {
        const totalElAfterDown = document.getElementById(
          'zero-virtualizer-total',
        );
        expect(totalElAfterDown?.textContent).toBe('1000');
      });

      const estimatedTotalElAfterDown = document.getElementById(
        'zero-virtualizer-estimated-total',
      );
      expect(estimatedTotalElAfterDown?.textContent).toBe('1000');

      // Now scroll back to the top gradually to exercise backward anchors
      const scrollStepUp = 1000;
      const startScrollOffset = 55000;

      for (let attempt = 0; attempt < 55; attempt++) {
        const scrollOffset = Math.max(
          0,
          startScrollOffset - (attempt + 1) * scrollStepUp,
        );

        act(() => {
          virtualizerInstance.scrollToOffset(scrollOffset);
        });

        await z.markAllQueriesAsGot();

        await waitFor(() => {
          virtualizerInstance.measure();
          const visibleItems = container.querySelectorAll('[data-index]');
          expect(visibleItems.length).toBeGreaterThan(0);
        });

        // If we've reached offset 0, verify the first item
        if (scrollOffset === 0) {
          await waitFor(() => {
            const firstItem = container.querySelector('[data-index="0"]');
            expect(firstItem?.textContent).toBe('Item 0001');
          });
          break;
        }
      }

      // After scrolling back to top, total should still be 1000
      const totalElAfterUp = document.getElementById('zero-virtualizer-total');
      expect(totalElAfterUp?.textContent).toBe('1000');

      const estimatedTotalElAfterUp = document.getElementById(
        'zero-virtualizer-estimated-total',
      );
      expect(estimatedTotalElAfterUp?.textContent).toBe('1000');
    } finally {
      cleanupTestContainer(root, container);
    }
  });

  test('ReactDOM rendering with permalink in middle, scroll to bottom, then back to top', async () => {
    const getPageQuerySpy = vi.fn(getPageQuery);
    const getSingleQuerySpy = vi.fn(getSingleQuery);

    // Create a real DOM container and render with ReactDOM directly
    const container = createTestContainer();
    const root = createRoot(container);

    let virtualizerInstance!: Virtualizer<HTMLElement, Element>;

    // Start at item 500 (middle of 1000 items)
    const permalinkID = '500';

    function VirtualListWithRef() {
      const result = useZeroVirtualizer(
        createBaseHookOptions({
          permalinkID,
          getScrollElement: () => document.getElementById('scroll-container'),
          getPageQuery: getPageQuerySpy,
          getSingleQuery: getSingleQuerySpy,
          overscan: 0,
        }),
      );

      virtualizerInstance = result.virtualizer;
      const virtualItems = result.virtualizer.getVirtualItems();

      return (
        <VirtualScrollContainer result={result} virtualItems={virtualItems} />
      );
    }

    try {
      root.render(
        <ZeroProvider zero={z}>
          <VirtualListWithRef />
        </ZeroProvider>,
      );

      await waitFor(() => {
        expect(virtualizerInstance).toBeTruthy();
      });

      await z.markAllQueriesAsGot();

      // Wait for permalink item to load
      // The permalink is for item with ID '500', which corresponds to "Item 0500" (0-based index 499)
      // But the virtualizer may render it at a different virtual index depending on the anchor
      await waitFor(() => {
        // Find the item by text content instead of index
        const items = container.querySelectorAll('[data-index]');
        let found = false;
        for (const item of items) {
          if (item.textContent === 'Item 0500') {
            found = true;
            break;
          }
        }
        expect(found).toBe(true);
      });

      // Verify getSingleQuery was called for the permalink
      expect(getSingleQuerySpy).toHaveBeenCalledWith('500');

      // Get the current scroll offset (should be positioned at the permalink)
      const initialScrollOffset = virtualizerInstance.scrollOffset ?? 0;
      expect(initialScrollOffset).toBeGreaterThan(0);

      // Scroll down to bottom incrementally from the permalink position
      const scrollStepDown = 1000;
      // Need to scroll to well past the end to ensure we load all data
      // Total height will be ~50,000px for 1000 items
      const maxScrollAttempts = 60;

      for (let attempt = 0; attempt < maxScrollAttempts; attempt++) {
        const scrollOffset =
          initialScrollOffset + (attempt + 1) * scrollStepDown;

        act(() => {
          virtualizerInstance.scrollToOffset(scrollOffset);
        });

        await z.markAllQueriesAsGot();

        await waitFor(() => {
          virtualizerInstance.measure();
          const visibleItems = container.querySelectorAll('[data-index]');
          expect(visibleItems.length).toBeGreaterThan(0);
        });

        // Check if we reached the bottom (both total and estimatedTotal = 1000)
        const totalEl = document.getElementById('zero-virtualizer-total');
        const estimatedTotalEl = document.getElementById(
          'zero-virtualizer-estimated-total',
        );
        if (
          totalEl?.textContent === '1000' &&
          estimatedTotalEl?.textContent === '1000'
        ) {
          break;
        }
      }

      // Verify we reached near the end (may not have total=1000 without atStart=true from permalink)
      await waitFor(() => {
        const estimatedTotalElAtBottom = document.getElementById(
          'zero-virtualizer-estimated-total',
        );
        // Should be close to or at 1000
        const estimatedTotal = Number(estimatedTotalElAtBottom?.textContent);
        expect(estimatedTotal).toBeGreaterThan(500);
      });

      // Check if we have items near the end visible (e.g., Item 0990+)
      await waitFor(() => {
        const items = container.querySelectorAll('[data-index]');
        let foundEndItem = false;
        for (const item of items) {
          // Check for items in the 990-1000 range
          const match = item.textContent?.match(/Item (\d+)/);
          if (match) {
            const itemNum = Number(match[1]);
            if (itemNum >= 990) {
              foundEndItem = true;
              break;
            }
          }
        }
        expect(foundEndItem).toBe(true);
      });

      // Now scroll back up gradually to test backward navigation
      const currentScrollOffset = virtualizerInstance.scrollOffset ?? 0;
      const scrollStepUp = 1000;
      // Scroll back up  partway (10 steps) to test backward paging
      const stepsToScrollUp = 10;

      for (let attempt = 0; attempt < stepsToScrollUp; attempt++) {
        const scrollOffset = Math.max(
          0,
          currentScrollOffset - (attempt + 1) * scrollStepUp,
        );

        act(() => {
          virtualizerInstance.scrollToOffset(scrollOffset);
        });

        await z.markAllQueriesAsGot();

        await waitFor(() => {
          virtualizerInstance.measure();
          const visibleItems = container.querySelectorAll('[data-index]');
          expect(visibleItems.length).toBeGreaterThan(0);
        });
      }

      // Verify we scrolled back and have data rendered (not just all "Loading...")
      await waitFor(() => {
        const items = container.querySelectorAll('[data-index]');
        expect(items.length).toBeGreaterThan(0);
        // Just verify we have some content (not "Loading...")
        let hasContent = false;
        for (const item of items) {
          if (item.textContent && item.textContent !== 'Loading...') {
            hasContent = true;
            break;
          }
        }
        expect(hasContent).toBe(true);
      });

      // Estimated total should still be reasonable (close to or at the full count)
      const estimatedTotalElAfterUp = document.getElementById(
        'zero-virtualizer-estimated-total',
      );
      const estimatedTotal = Number(estimatedTotalElAfterUp?.textContent);
      expect(estimatedTotal).toBeGreaterThan(500);
    } finally {
      cleanupTestContainer(root, container);
    }
  });
});
