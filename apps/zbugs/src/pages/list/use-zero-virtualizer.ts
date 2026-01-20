import type {UseQueryOptions} from '@rocicorp/zero/react';
import type {Virtualizer} from '@tanstack/react-virtual';
import {useVirtualizer, type VirtualizerOptions} from '@tanstack/react-virtual';
import {useEffect, useMemo, useState} from 'react';
import {useHistoryState} from 'wouter/use-browser-location';
import {assert} from '../../../../../packages/shared/src/asserts.ts';
import {ITEM_SIZE} from './list-page.tsx';
import {
  useIssues,
  type Anchor,
  type GetPageQuery,
  type GetSingleQuery,
} from './use-issues.tsx';

// Make sure this is even since we half it for permalink loading
const MIN_PAGE_SIZE = 100;

const NUM_ROWS_FOR_LOADING_SKELETON = 1;

type QueryAnchor<TListContextParams, TIssueRowSort> = {
  readonly anchor: Anchor<TIssueRowSort>;
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

type PermalinkHistoryState<TIssueRowSort> = Readonly<{
  anchor: Anchor<TIssueRowSort>;
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

export function useZeroVirtualizer<
  TScrollElement extends Element,
  TItemElement extends Element,
  TListContextParams,
  TIssue,
  TIssueRowSort,
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
}: {
  // Tanstack Virtual params
  estimateSize: (index: number) => number;
  overscan?: number;
  getScrollElement: VirtualizerOptions<
    TScrollElement,
    TItemElement
  >['getScrollElement'];

  // Zero specific params
  listContextParams: TListContextParams;

  permalinkID?: string | null | undefined;

  getPageQuery: GetPageQuery<TIssue, TIssueRowSort>;
  getSingleQuery: GetSingleQuery<TIssue>;
  options?: UseQueryOptions | undefined;
  toStartRow: (issue: TIssue) => TIssueRowSort;
}): {
  virtualizer: Virtualizer<TScrollElement, TItemElement>;
  issueAt: (index: number) => TIssue | undefined;
  complete: boolean;
  issuesEmpty: boolean;
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

  const historyState = usePermalinkHistoryState<TIssueRowSort>();

  // Initialize queryAnchor from history.state directly to avoid Strict Mode double-mount issues
  const [queryAnchor, setQueryAnchor] = useState<
    QueryAnchor<TListContextParams, TIssueRowSort>
  >(() => {
    // const historyState = maybeGetPermalinkHistoryState<TIssueRowSort>();
    const anchor = (
      historyState
        ? historyState.anchor
        : permalinkID
          ? {
              index: NUM_ROWS_FOR_LOADING_SKELETON,
              kind: 'permalink',
              id: permalinkID,
            }
          : TOP_ANCHOR
    ) as Anchor<TIssueRowSort>;
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

    // TODO(arv): DRY
    if (permalinkID) {
      return {
        index: NUM_ROWS_FOR_LOADING_SKELETON,
        kind: 'permalink',
        id: permalinkID,
      } satisfies Anchor<TIssueRowSort>;
    }
    return TOP_ANCHOR;
  }, [isListContextCurrent, queryAnchor.anchor]);

  const [pageSize, setPageSize] = useState(MIN_PAGE_SIZE);

  const {
    issueAt,
    issuesLength,
    complete,
    issuesEmpty,
    atStart,
    atEnd,
    firstIssueIndex,
    permalinkNotFound,
  } = useIssues({
    pageSize,
    anchor,
    options,
    getPageQuery,
    getSingleQuery,
    toStartRow,
  });

  const newEstimatedTotal = firstIssueIndex + issuesLength;

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
          return anchor.index * ITEM_SIZE;
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
          makeEven(Math.ceil(virtualizer.scrollRect?.height / ITEM_SIZE) * 3),
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

  const updateAnchor = (anchor: Anchor<TIssueRowSort>) => {
    setQueryAnchor({
      anchor,
      listContextParams,
    });
  };

  useEffect(() => {
    if (issuesEmpty || !isListContextCurrent) {
      return;
    }

    if (skipPagingLogic && pendingScrollAdjustment === 0) {
      setSkipPagingLogic(false);
      return;
    }

    // There is a pending scroll adjustment from last anchor change.
    if (pendingScrollAdjustment !== 0) {
      virtualizer.scrollToOffset(
        (virtualizer.scrollOffset ?? 0) + pendingScrollAdjustment * ITEM_SIZE,
      );

      setEstimatedTotal(estimatedTotal + pendingScrollAdjustment);

      setPendingScrollAdjustment(0);
      setSkipPagingLogic(true);

      return;
    }

    // First issue is before start of list - need to shift down
    if (firstIssueIndex < 0) {
      const placeholderRows = !atStart ? NUM_ROWS_FOR_LOADING_SKELETON : 0;
      const offset = -firstIssueIndex + placeholderRows;

      setSkipPagingLogic(true);
      setPendingScrollAdjustment(offset);
      const newAnchor = {
        ...anchor,
        index: anchor.index + offset,
      };
      updateAnchor(newAnchor);
      return;
    }

    if (atStart && firstIssueIndex > 0) {
      setPendingScrollAdjustment(-firstIssueIndex);
      updateAnchor(TOP_ANCHOR);
      return;
    }
  }, [
    firstIssueIndex,
    anchor,
    atStart,
    pendingScrollAdjustment,
    skipPagingLogic,
    issuesEmpty,
    isListContextCurrent,
    estimatedTotal,
    // virtualizer, Do not depend on virtualizer. TDZ.
  ]);

  useEffect(() => {
    if (!isListContextCurrent) {
      const scrollElement = getScrollElement();
      if (historyState) {
        if (scrollElement) {
          scrollElement.scrollTop = historyState.scrollTop;
        }
        setEstimatedTotal(historyState.estimatedTotal);
        setHasReachedStart(historyState.hasReachedStart);
        setHasReachedEnd(historyState.hasReachedEnd);
        // TODO: FIXME
        updateAnchor(historyState.anchor as Anchor<TIssueRowSort>);
      } else if (permalinkID) {
        if (scrollElement) {
          scrollElement.scrollTop = NUM_ROWS_FOR_LOADING_SKELETON * ITEM_SIZE;
        }
        setEstimatedTotal(NUM_ROWS_FOR_LOADING_SKELETON);
        setHasReachedStart(false);
        setHasReachedEnd(false);
        updateAnchor({
          id: permalinkID,
          index: NUM_ROWS_FOR_LOADING_SKELETON,
          kind: 'permalink',
        });
      } else {
        if (scrollElement) {
          scrollElement.scrollTop = 0;
        }
        setEstimatedTotal(0);
        setHasReachedStart(true);
        setHasReachedEnd(false);
        updateAnchor(TOP_ANCHOR);
      }
      setSkipPagingLogic(true);
    }
  }, [isListContextCurrent]);

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
      if (firstIssueIndex !== 0) {
        updateAnchor(TOP_ANCHOR);
        return;
      }
    }

    const updateAnchorForEdge = (
      targetIndex: number,
      type: 'forward' | 'backward',
      indexOffset: number,
    ) => {
      const index = toBoundIndex(targetIndex, firstIssueIndex, issuesLength);
      const startRow = issueAt(index);
      assert(startRow !== undefined || type === 'forward');
      updateAnchor({
        index: index + indexOffset,
        kind: type,
        startRow,
      } as Anchor<TIssueRowSort>);
    };

    const firstItem = virtualItems[0];
    const lastItem = virtualItems[virtualItems.length - 1];
    const nearPageEdgeThreshold = getNearPageEdgeThreshold(pageSize);

    const distanceFromStart = firstItem.index - firstIssueIndex;
    const distanceFromEnd = firstIssueIndex + issuesLength - lastItem.index;

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
    firstIssueIndex,
    issuesLength,
    atStart,
    atEnd,
    issueAt,
  ]);

  return {
    virtualizer,
    issueAt,
    complete,
    issuesEmpty,
    permalinkNotFound,
    estimatedTotal,
    total,
  };
}

/**
 * Clamps an index to be within the valid range of issues.
 * @param targetIndex - The desired index to clamp
 * @param firstIssueIndex - The first valid issue index
 * @param issuesLength - The number of issues available
 * @returns The clamped index within [firstIssueIndex, firstIssueIndex + issuesLength - 1]
 */
function toBoundIndex(
  targetIndex: number,
  firstIssueIndex: number,
  issuesLength: number,
): number {
  return Math.max(
    firstIssueIndex,
    Math.min(firstIssueIndex + issuesLength - 1, targetIndex),
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
  TIssueRowSort,
>(): PermalinkHistoryState<TIssueRowSort> | null {
  const maybeHistoryState = useHistoryState<unknown>();
  if (maybeHistoryState === null || typeof maybeHistoryState !== 'object') {
    return null;
  }

  if (!(zeroHistoryKey in maybeHistoryState)) {
    return null;
  }

  return maybeHistoryState[
    zeroHistoryKey
  ] as PermalinkHistoryState<TIssueRowSort>;
}

function replaceHistoryState<TIssueRowSort>(
  data: PermalinkHistoryState<TIssueRowSort>,
) {
  const historyState = history.state || {};
  historyState[zeroHistoryKey] = data;
  history.replaceState(historyState, '', document.location.href);
}
