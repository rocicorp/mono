import {useVirtualizer, type Virtualizer} from '@tanstack/react-virtual';
import {useEffect, useMemo, useState} from 'react';
import {useHistoryState} from 'wouter/use-browser-location';
import {assert} from '../../shared/src/asserts.ts';
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

type QueryAnchor<TListContextParams, TStartRow> = {
  readonly anchor: Anchor<TStartRow>;
  /**
   * Associates an anchor with list query params to coordinate state during
   * navigation. When list context params change (e.g., filter/sort changes or
   * browser back/forward navigation), the anchor and scroll position must be
   * updated atomically with the new query results.
   *
   * When `listContextParams !== queryAnchor.listContextParams`:
   * - Use history state to restore previous scroll position and anchor if
   *   navigating back
   * - Use permalink anchor if loading a specific item
   * - Otherwise reset to top
   *
   * During the transition (while `!isListContextCurrent`), skip paging logic
   * and count updates to avoid querying with mismatched anchor/params or
   * calculating counts from inconsistent state.
   */
  readonly listContextParams: TListContextParams;
};

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
  const [estimatedTotal, setEstimatedTotal] = useState(
    NUM_ROWS_FOR_LOADING_SKELETON,
  );
  const [hasReachedStart, setHasReachedStart] = useState(false);
  const [hasReachedEnd, setHasReachedEnd] = useState(false);
  const [skipPagingLogic, setSkipPagingLogic] = useState(false);

  const historyState = usePermalinkHistoryState<TStartRow>();

  const createPermalinkAnchor = (id: string) =>
    ({
      id,
      index: NUM_ROWS_FOR_LOADING_SKELETON,
      kind: 'permalink',
    }) as const;

  // Initialize queryAnchor from history.state directly to avoid Strict Mode double-mount rows
  const [queryAnchor, setQueryAnchor] = useState<
    QueryAnchor<TListContextParams, TStartRow>
  >(() => {
    const anchor = historyState
      ? historyState.anchor
      : permalinkID
        ? createPermalinkAnchor(permalinkID)
        : TOP_ANCHOR;
    return {
      anchor,
      listContextParams,
    };
  });

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
  }, [pageSize, virtualizer.scrollRect]);

  useEffect(() => {
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
  ]);

  useEffect(() => {
    if (atStart) {
      setHasReachedStart(true);
    }
  }, [atStart]);

  useEffect(() => {
    if (atEnd) {
      setHasReachedEnd(true);
    }
  }, [atEnd]);

  useEffect(() => {
    if (complete && newEstimatedTotal > estimatedTotal) {
      setEstimatedTotal(newEstimatedTotal);
    }
  }, [estimatedTotal, complete, newEstimatedTotal]);

  const [pendingScrollAdjustment, setPendingScrollAdjustment] =
    useState<number>(0);

  const updateAnchor = (anchor: Anchor<TStartRow>) => {
    setQueryAnchor({
      anchor,
      listContextParams,
    });
  };

  useEffect(() => {
    if (rowsEmpty || !isListContextCurrent) {
      return;
    }

    if (skipPagingLogic && pendingScrollAdjustment === 0) {
      setSkipPagingLogic(false);
      return;
    }

    // There is a pending scroll adjustment from last anchor change.
    if (pendingScrollAdjustment !== 0) {
      virtualizer.scrollToOffset(
        (virtualizer.scrollOffset ?? 0) +
          pendingScrollAdjustment *
            // TODO: Support dynamic item sizes
            estimateSize(0),
      );

      setEstimatedTotal(estimatedTotal + pendingScrollAdjustment);
      setPendingScrollAdjustment(0);
      setSkipPagingLogic(true);

      return;
    }

    // First row is before start of list - need to shift down
    if (firstRowIndex < 0) {
      const placeholderRows = !atStart ? NUM_ROWS_FOR_LOADING_SKELETON : 0;
      const offset = -firstRowIndex + placeholderRows;

      setSkipPagingLogic(true);
      setPendingScrollAdjustment(offset);
      const newAnchor = {
        ...anchor,
        index: anchor.index + offset,
      };
      updateAnchor(newAnchor);
      return;
    }

    if (atStart && firstRowIndex > 0) {
      setPendingScrollAdjustment(-firstRowIndex);
      updateAnchor(TOP_ANCHOR);
      return;
    }
  }, [
    firstRowIndex,
    anchor,
    atStart,
    pendingScrollAdjustment,
    skipPagingLogic,
    rowsEmpty,
    isListContextCurrent,
    estimatedTotal,
    // virtualizer, Do not depend on virtualizer. TDZ.
  ]);

  useEffect(() => {
    if (!isListContextCurrent) {
      const scrollElement = getScrollElement();

      const resetState = (
        scrollTop: number,
        estimatedTotal: number,
        hasReachedStart: boolean,
        hasReachedEnd: boolean,
        anchor: Anchor<TStartRow>,
      ) => {
        if (scrollElement) {
          scrollElement.scrollTop = scrollTop;
        }
        setEstimatedTotal(estimatedTotal);
        setHasReachedStart(hasReachedStart);
        setHasReachedEnd(hasReachedEnd);
        updateAnchor(anchor);
      };

      if (historyState) {
        resetState(
          historyState.scrollTop,
          historyState.estimatedTotal,
          historyState.hasReachedStart,
          historyState.hasReachedEnd,
          historyState.anchor,
        );
      } else if (permalinkID) {
        resetState(
          NUM_ROWS_FOR_LOADING_SKELETON *
            // TODO: Support dynamic item sizes
            estimateSize(0),
          NUM_ROWS_FOR_LOADING_SKELETON,
          false,
          false,
          createPermalinkAnchor(permalinkID),
        );
      } else {
        resetState(0, 0, true, false, TOP_ANCHOR);
      }

      setSkipPagingLogic(true);
    }
  }, [
    isListContextCurrent,
    historyState,
    permalinkID,
    // getScrollElement - ignore
  ]);

  const total = hasReachedStart && hasReachedEnd ? estimatedTotal : undefined;

  const virtualItems = virtualizer.getVirtualItems();

  useEffect(() => {
    if (
      !isListContextCurrent ||
      virtualItems.length === 0 ||
      !complete ||
      skipPagingLogic ||
      pendingScrollAdjustment !== 0
    ) {
      return;
    }

    if (atStart) {
      if (firstRowIndex !== 0) {
        updateAnchor(TOP_ANCHOR);
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
      updateAnchor({
        index: index + indexOffset,
        kind: type,
        startRow,
      } as Anchor<TStartRow>);
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
    skipPagingLogic,
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
    rowAt: rowAt,
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
