import {useVirtualizer, type Virtualizer} from '@tanstack/react-virtual';
import {useEffect, useLayoutEffect, useMemo, useReducer, useState} from 'react';
import {assert} from '../../shared/src/asserts.ts';
import {pagingReducer, type PagingState} from './paging-reducer.ts';
import type {UseQueryOptions} from './use-query.tsx';
import {
  useRows,
  type Anchor,
  type GetPageQuery,
  type GetSingleQuery,
} from './use-rows.ts';

// Make sure this is even since we half it for permalink loading
const MIN_PAGE_SIZE = 100;

const NUM_ROWS_FOR_LOADING_SKELETON = 1;

/**
 * State object that captures the virtualizer's scroll position and pagination state.
 * Used for persisting and restoring the virtualizer state across navigation or page reloads.
 *
 * @typeParam TStartRow - The type of the start row data used for pagination anchoring
 */
export type PermalinkHistoryState<TStartRow> = Readonly<{
  /** The anchor point for pagination (includes position, direction, and start row data) */
  anchor: Anchor<TStartRow>;
  /** The scroll position in pixels from the top of the scrollable container */
  scrollTop: number;
  /** The estimated total number of rows in the list */
  estimatedTotal: number;
  /** Whether the virtualizer has reached the start of the list */
  hasReachedStart: boolean;
  /** Whether the virtualizer has reached the end of the list */
  hasReachedEnd: boolean;
}>;

const TOP_ANCHOR = Object.freeze({
  index: 0,
  kind: 'forward',
  startRow: undefined,
}) satisfies Anchor<unknown>;

type TanstackUseVirtualizerOptions<
  TScrollElement extends Element,
  TItemElement extends Element,
> = Parameters<typeof useVirtualizer<TScrollElement, TItemElement>>[0];

/**
 * Options for configuring the Zero virtualizer.
 * Extends Tanstack Virtual's options with Zero-specific pagination and state management.
 *
 * @typeParam TScrollElement - The type of the scrollable container element
 * @typeParam TItemElement - The type of the individual item elements
 * @typeParam TListContextParams - The type of parameters that define the list's query context
 * @typeParam TRow - The type of row data returned from queries
 * @typeParam TStartRow - The type of data needed to anchor pagination (typically a subset of TRow)
 */
export type UseZeroVirtualizerOptions<
  TScrollElement extends Element,
  TItemElement extends Element,
  TListContextParams,
  TRow,
  TStartRow,
> = Omit<
  TanstackUseVirtualizerOptions<TScrollElement, TItemElement>,
  // count is managed by useZeroVirtualizer
  | 'count'
  // initialOffset is managed by useZeroVirtualizer
  | 'initialOffset'
  // Only support vertical lists for now
  | 'horizontal'
> & {
  /** Parameters that define the list's query context (filters, sort order, etc.) */
  listContextParams: TListContextParams;

  /** Optional ID to highlight/scroll to a specific row (permalink functionality) */
  permalinkID?: string | null | undefined;

  /** Function that returns a query for fetching a page of rows */
  getPageQuery: GetPageQuery<TRow, TStartRow>;
  /** Function that returns a query for fetching a single row by ID */
  getSingleQuery: GetSingleQuery<TRow>;
  /** Optional query options */
  options?: UseQueryOptions | undefined;
  /** Function to extract the start row data from a full row (for pagination anchoring) */
  toStartRow: (row: TRow) => TStartRow;

  /**
   * Optional current permalink state for restoring virtualizer position.
   * If provided along with `onPermalinkStateChange`, enables state persistence.
   * If not provided, virtualizer operates in uncontrolled mode.
   */
  permalinkState?: PermalinkHistoryState<TStartRow> | null | undefined;
  /**
   * Optional callback invoked when the virtualizer state changes.
   * Use this to persist state (e.g., to browser history, local storage, etc.).
   * Called with the new state approximately 100ms after scroll/pagination changes.
   */
  onPermalinkStateChange?: (state: PermalinkHistoryState<TStartRow>) => void;
};

const createPermalinkAnchor = (id: string) =>
  ({
    id,
    index: NUM_ROWS_FOR_LOADING_SKELETON,
    kind: 'permalink',
  }) as const;

/**
 * Result object returned by the useZeroVirtualizer hook.
 *
 * @typeParam TScrollElement - The type of the scrollable container element
 * @typeParam TItemElement - The type of the individual item elements
 * @typeParam TRow - The type of row data returned from queries
 */
export type ZeroVirtualizerResult<
  TScrollElement extends Element,
  TItemElement extends Element,
  TRow,
> = {
  /** The Tanstack Virtual virtualizer instance for rendering virtual items */
  virtualizer: Virtualizer<TScrollElement, TItemElement>;
  /** Function to get the row data at a specific index, or undefined if not loaded */
  rowAt: (index: number) => TRow | undefined;
  /** Whether all initially requested data has finished loading */
  complete: boolean;
  /** Whether the list is empty (no rows exist matching the query) */
  rowsEmpty: boolean;
  /** Whether the specified permalinkID was not found in the query results */
  permalinkNotFound: boolean;
  /** Estimated total number of rows (may be inexact until both ends are reached) */
  estimatedTotal: number;
  /** Exact total number of rows, or undefined if not yet known (requires reaching both ends) */
  total: number | undefined;
};

/**
 * Hook that creates a virtualized list with bidirectional pagination and state persistence.
 *
 * This hook combines Tanstack Virtual's efficient virtualization with Zero's reactive queries
 * to create infinitely scrolling lists that load data on demand. It supports:
 * - Bidirectional scrolling (load more items at top or bottom)
 * - Permalink functionality (jump to and highlight specific items)
 * - State persistence (restore scroll position across navigation)
 * - Dynamic page sizing based on viewport
 *
 * @typeParam TScrollElement - The type of the scrollable container element
 * @typeParam TItemElement - The type of the individual item elements
 * @typeParam TListContextParams - The type of parameters that define the list's query context
 * @typeParam TRow - The type of row data returned from queries
 * @typeParam TStartRow - The type of data needed to anchor pagination
 *
 * @param options - Configuration options including query functions, sizing, and state management
 * @returns An object containing the virtualizer instance, row accessor, and state flags
 *
 * @example
 * ```tsx
 * const {virtualizer, rowAt, complete} = useZeroVirtualizer({
 *   estimateSize: () => 50,
 *   getScrollElement: () => scrollRef.current,
 *   listContextParams: {projectId: 'abc'},
 *   getPageQuery: (limit, start, dir) => z.query.issues.where(...).limit(limit),
 *   getSingleQuery: (id) => z.query.issues.where('id', id),
 *   toStartRow: (row) => ({id: row.id, created: row.created}),
 * });
 * ```
 */
export function useZeroVirtualizer<
  TScrollElement extends Element,
  TItemElement extends Element,
  TListContextParams,
  TRow,
  TStartRow,
>({
  // Tanstack Virtual params
  estimateSize,
  overscan = 5, // Virtualizer defaults to 1.
  getScrollElement,

  // Zero specific params
  listContextParams,
  permalinkID,
  getPageQuery,
  getSingleQuery,
  options,
  toStartRow,

  // Permalink state persistence
  permalinkState,
  onPermalinkStateChange,

  ...restVirtualizerOptions
}: UseZeroVirtualizerOptions<
  TScrollElement,
  TItemElement,
  TListContextParams,
  TRow,
  TStartRow
>): ZeroVirtualizerResult<TScrollElement, TItemElement, TRow> {
  // Initialize paging state from permalinkState directly to avoid Strict Mode double-mount rows
  const [
    {
      estimatedTotal,
      hasReachedStart,
      hasReachedEnd,
      queryAnchor,
      pagingPhase,
      pendingScrollAdjustment,
    },
    dispatch,
  ] = useReducer(
    pagingReducer<TListContextParams, TStartRow>,
    undefined,
    (): PagingState<TListContextParams, TStartRow> => {
      const anchor = permalinkState
        ? permalinkState.anchor
        : permalinkID
          ? createPermalinkAnchor(permalinkID)
          : TOP_ANCHOR;
      return {
        estimatedTotal:
          permalinkState?.estimatedTotal ?? NUM_ROWS_FOR_LOADING_SKELETON,
        hasReachedStart: permalinkState?.hasReachedStart ?? false,
        hasReachedEnd: permalinkState?.hasReachedEnd ?? false,
        queryAnchor: {
          anchor,
          listContextParams,
        },
        pagingPhase: 'idle',
        pendingScrollAdjustment: 0,
      };
    },
  );

  const isListContextCurrent =
    queryAnchor.listContextParams === listContextParams;

  const anchor = useMemo(() => {
    if (isListContextCurrent) {
      return queryAnchor.anchor;
    }
    return permalinkID ? createPermalinkAnchor(permalinkID) : TOP_ANCHOR;
  }, [isListContextCurrent, queryAnchor.anchor, permalinkID]);

  const [pageSize, setPageSize] = useState(MIN_PAGE_SIZE);

  const {
    rowAt,
    rowsLength,
    complete,
    rowsEmpty,
    atStart,
    atEnd,
    firstRowIndex,
    permalinkNotFound,
  } = useRows({
    pageSize,
    anchor,
    options,
    getPageQuery,
    getSingleQuery,
    toStartRow,
  });

  const newEstimatedTotal = firstRowIndex + rowsLength;

  const virtualizer: Virtualizer<TScrollElement, TItemElement> = useVirtualizer(
    {
      ...restVirtualizerOptions,
      count:
        Math.max(estimatedTotal, newEstimatedTotal) +
        (!atEnd ? NUM_ROWS_FOR_LOADING_SKELETON : 0),
      estimateSize,
      overscan,
      getScrollElement,
      initialOffset: () => {
        if (permalinkState?.scrollTop !== undefined) {
          return permalinkState.scrollTop;
        }
        if (anchor.kind === 'permalink') {
          // TODO: Support dynamic item sizes
          return anchor.index * estimateSize(0);
        }
        return 0;
      },
      horizontal: false,
    },
  );

  useEffect(() => {
    // Make sure page size is enough to fill the scroll element at least
    // 3 times.  Don't shrink page size.
    const newPageSize = virtualizer.scrollRect
      ? Math.max(
          MIN_PAGE_SIZE,
          makeEven(
            Math.ceil(
              virtualizer.scrollRect?.height /
                // TODO: Support dynamic item sizes
                estimateSize(0),
            ) * 3,
          ),
        )
      : MIN_PAGE_SIZE;
    if (newPageSize > pageSize) {
      setPageSize(newPageSize);
    }
  }, [pageSize, virtualizer.scrollRect, estimateSize]);

  useEffect(() => {
    if (!isListContextCurrent || !onPermalinkStateChange) {
      return;
    }
    const timeoutId = setTimeout(() => {
      onPermalinkStateChange({
        anchor,
        scrollTop: virtualizer.scrollOffset ?? 0,
        estimatedTotal,
        hasReachedStart,
        hasReachedEnd,
      });
    }, 100);

    return () => clearTimeout(timeoutId);
  }, [
    anchor,
    virtualizer.scrollOffset,
    estimatedTotal,
    hasReachedStart,
    hasReachedEnd,
    isListContextCurrent,
    onPermalinkStateChange,
  ]);

  useEffect(() => {
    if (atStart) {
      dispatch({type: 'REACHED_START'});
    }
  }, [atStart]);

  useEffect(() => {
    if (atEnd) {
      dispatch({type: 'REACHED_END'});
    }
  }, [atEnd]);

  useEffect(() => {
    if (complete && newEstimatedTotal > estimatedTotal) {
      dispatch({type: 'UPDATE_ESTIMATED_TOTAL', newTotal: newEstimatedTotal});
    }
  }, [estimatedTotal, complete, newEstimatedTotal]);

  // Apply scroll adjustments synchronously with layout to prevent visual jumps
  useLayoutEffect(() => {
    if (pendingScrollAdjustment !== 0) {
      virtualizer.scrollToOffset(
        (virtualizer.scrollOffset ?? 0) +
          pendingScrollAdjustment *
            // TODO: Support dynamic item sizes
            estimateSize(0),
      );

      dispatch({type: 'SCROLL_ADJUSTED'});
    }
  }, [pendingScrollAdjustment, virtualizer, estimateSize]);

  useEffect(() => {
    if (rowsEmpty || !isListContextCurrent) {
      return;
    }

    if (pagingPhase === 'skipping' && pendingScrollAdjustment === 0) {
      dispatch({type: 'PAGING_COMPLETE'});
      return;
    }

    // Skip if there's a pending scroll adjustment - let useLayoutEffect handle it
    if (pendingScrollAdjustment !== 0) {
      return;
    }

    // First row is before start of list - need to shift down
    if (firstRowIndex < 0) {
      const placeholderRows = !atStart ? NUM_ROWS_FOR_LOADING_SKELETON : 0;
      const offset = -firstRowIndex + placeholderRows;
      const newAnchor = {
        ...anchor,
        index: anchor.index + offset,
      };
      dispatch({type: 'SHIFT_ANCHOR_DOWN', offset, newAnchor});
      return;
    }

    if (atStart && firstRowIndex > 0) {
      dispatch({type: 'RESET_TO_TOP', offset: -firstRowIndex});
      return;
    }
  }, [
    firstRowIndex,
    anchor,
    atStart,
    pendingScrollAdjustment,
    pagingPhase,
    rowsEmpty,
    isListContextCurrent,
    // virtualizer - omitted to avoid infinite render loops from scroll events
  ]);

  // Use layoutEffect to restore scroll position synchronously to avoid visual jumps
  useLayoutEffect(() => {
    if (!isListContextCurrent) {
      if (permalinkState) {
        virtualizer.scrollToOffset(permalinkState.scrollTop);
        dispatch({
          type: 'RESET_STATE',
          estimatedTotal: permalinkState.estimatedTotal,
          hasReachedStart: permalinkState.hasReachedStart,
          hasReachedEnd: permalinkState.hasReachedEnd,
          anchor: permalinkState.anchor,
          listContextParams,
        });
      } else if (permalinkID) {
        virtualizer.scrollToOffset(
          NUM_ROWS_FOR_LOADING_SKELETON *
            // TODO: Support dynamic item sizes
            estimateSize(0),
        );
        dispatch({
          type: 'RESET_STATE',
          estimatedTotal: NUM_ROWS_FOR_LOADING_SKELETON,
          hasReachedStart: false,
          hasReachedEnd: false,
          anchor: createPermalinkAnchor(permalinkID),
          listContextParams,
        });
      } else {
        virtualizer.scrollToOffset(0);
        dispatch({
          type: 'RESET_STATE',
          estimatedTotal: 0,
          hasReachedStart: true,
          hasReachedEnd: false,
          anchor: TOP_ANCHOR,
          listContextParams,
        });
      }
    }
  }, [
    isListContextCurrent,
    permalinkState,
    permalinkID,
    virtualizer,
    estimateSize,
    listContextParams,
  ]);

  const total = hasReachedStart && hasReachedEnd ? estimatedTotal : undefined;

  const virtualItems = virtualizer.getVirtualItems();

  useEffect(() => {
    if (
      !isListContextCurrent ||
      virtualItems.length === 0 ||
      !complete ||
      pagingPhase !== 'idle' ||
      pendingScrollAdjustment !== 0
    ) {
      return;
    }

    if (atStart) {
      if (firstRowIndex !== 0) {
        dispatch({type: 'UPDATE_ANCHOR', anchor: TOP_ANCHOR});
        return;
      }
    }

    const updateAnchorForEdge = (
      targetIndex: number,
      type: 'forward' | 'backward',
      indexOffset: number,
    ) => {
      const index = toBoundIndex(targetIndex, firstRowIndex, rowsLength);
      const startRow = rowAt(index);
      assert(startRow !== undefined || type === 'forward');
      dispatch({
        type: 'UPDATE_ANCHOR',
        anchor: {
          index: index + indexOffset,
          kind: type,
          startRow,
        } as Anchor<TStartRow>,
      });
    };

    const firstItem = virtualItems[0];
    const lastItem = virtualItems[virtualItems.length - 1];
    const nearPageEdgeThreshold = getNearPageEdgeThreshold(pageSize);

    const distanceFromStart = firstItem.index - firstRowIndex;
    const distanceFromEnd = firstRowIndex + rowsLength - lastItem.index;

    if (!atStart && distanceFromStart <= nearPageEdgeThreshold) {
      updateAnchorForEdge(
        lastItem.index + 2 * nearPageEdgeThreshold,
        'backward',
        0,
      );
      return;
    }

    if (!atEnd && distanceFromEnd <= nearPageEdgeThreshold) {
      updateAnchorForEdge(
        firstItem.index - 2 * nearPageEdgeThreshold,
        'forward',
        1,
      );
      return;
    }
  }, [
    isListContextCurrent,
    virtualItems,
    pagingPhase,
    pendingScrollAdjustment,
    complete,
    pageSize,
    firstRowIndex,
    rowsLength,
    atStart,
    atEnd,
    rowAt,
  ]);

  return {
    virtualizer,
    rowAt,
    complete,
    rowsEmpty,
    permalinkNotFound,
    estimatedTotal,
    total,
  };
}

/**
 * Clamps an index to be within the valid range of rows.
 * @param targetIndex - The desired index to clamp
 * @param firstRowIndex - The first valid row index
 * @param rowsLength - The number of rows available
 * @returns The clamped index within [firstRowIndex, firstRowIndex + rowsLength - 1]
 */
function toBoundIndex(
  targetIndex: number,
  firstRowIndex: number,
  rowsLength: number,
): number {
  if (rowsLength === 0) {
    return firstRowIndex;
  }
  return Math.max(
    firstRowIndex,
    Math.min(firstRowIndex + rowsLength - 1, targetIndex),
  );
}

function getNearPageEdgeThreshold(pageSize: number) {
  return Math.ceil(pageSize / 10);
}

function makeEven(n: number) {
  return n % 2 === 0 ? n : n + 1;
}
