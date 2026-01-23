import {useVirtualizer, type Virtualizer} from '@tanstack/react-virtual';
import {useEffect, useLayoutEffect, useMemo, useReducer, useState} from 'react';
import {useHistoryState} from 'wouter/use-browser-location';
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

type PermalinkHistoryState<TStartRow> = Readonly<{
  anchor: Anchor<TStartRow>;
  scrollTop: number;
  estimatedTotal: number;
  hasReachedStart: boolean;
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
  // Zero specific params
  listContextParams: TListContextParams;

  permalinkID?: string | null | undefined;

  getPageQuery: GetPageQuery<TRow, TStartRow>;
  getSingleQuery: GetSingleQuery<TRow>;
  options?: UseQueryOptions | undefined;
  toStartRow: (row: TRow) => TStartRow;
};

const createPermalinkAnchor = (id: string) =>
  ({
    id,
    index: NUM_ROWS_FOR_LOADING_SKELETON,
    kind: 'permalink',
  }) as const;

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

  ...restVirtualizerOptions
}: UseZeroVirtualizerOptions<
  TScrollElement,
  TItemElement,
  TListContextParams,
  TRow,
  TStartRow
>): {
  virtualizer: Virtualizer<TScrollElement, TItemElement>;
  rowAt: (index: number) => TRow | undefined;
  complete: boolean;
  rowsEmpty: boolean;
  permalinkNotFound: boolean;
  estimatedTotal: number;
  total: number | undefined;
} {
  const historyState = usePermalinkHistoryState<TStartRow>();

  // Initialize paging state from history.state directly to avoid Strict Mode double-mount rows
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
      const anchor = historyState
        ? historyState.anchor
        : permalinkID
          ? createPermalinkAnchor(permalinkID)
          : TOP_ANCHOR;
      return {
        estimatedTotal:
          historyState?.estimatedTotal ?? NUM_ROWS_FOR_LOADING_SKELETON,
        hasReachedStart: historyState?.hasReachedStart ?? false,
        hasReachedEnd: historyState?.hasReachedEnd ?? false,
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
        if (historyState?.scrollTop !== undefined) {
          return historyState.scrollTop;
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
    if (!isListContextCurrent) {
      return;
    }
    const timeoutId = setTimeout(() => {
      replaceHistoryState({
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
      if (historyState) {
        virtualizer.scrollToOffset(historyState.scrollTop);
        dispatch({
          type: 'RESET_STATE',
          estimatedTotal: historyState.estimatedTotal,
          hasReachedStart: historyState.hasReachedStart,
          hasReachedEnd: historyState.hasReachedEnd,
          anchor: historyState.anchor,
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
    historyState,
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

const zeroHistoryKey = '@rocicorp/zero/react/virtual/v0';

function usePermalinkHistoryState<
  TStartRow,
>(): PermalinkHistoryState<TStartRow> | null {
  const maybeHistoryState = useHistoryState<unknown>();
  if (maybeHistoryState === null || typeof maybeHistoryState !== 'object') {
    return null;
  }

  if (!(zeroHistoryKey in maybeHistoryState)) {
    return null;
  }

  return maybeHistoryState[zeroHistoryKey] as PermalinkHistoryState<TStartRow>;
}

function replaceHistoryState<TStartRow>(
  data: PermalinkHistoryState<TStartRow>,
) {
  const historyState = history.state || {};
  historyState[zeroHistoryKey] = data;
  history.replaceState(historyState, '', document.location.href);
}
