import type {VirtualItem, Virtualizer} from '@tanstack/react-virtual';
import {useVirtualizer, type VirtualizerOptions} from '@tanstack/react-virtual';
import {useEffect, useMemo, useState} from 'react';
import {useDebouncedCallback} from 'use-debounce';
import {useHistoryState} from 'wouter/use-browser-location';
import * as zod from 'zod/mini';
import {assert} from '../../../../../packages/shared/src/asserts.ts';
import {
  issueRowSortSchema,
  type Issue,
  type ListContextParams,
} from '../../../shared/queries.ts';
import {replaceHistoryState} from '../../navigate.ts';
import {CACHE_NAV, CACHE_NONE} from '../../query-cache-policy.ts';
import {ITEM_SIZE} from './list-page.tsx';
import {useIssues} from './use-issues.tsx';

// Make sure this is even since we half it for permalink loading
const MIN_PAGE_SIZE = 100;

const NUM_ROWS_FOR_LOADING_SKELETON = 1;

type QueryAnchor = {
  readonly anchor: Anchor;
  /**
   * Associates an anchor with list query params.  This is for managing the
   * transition when query params change.  When this happens the list should
   * scroll to 0, the anchor reset to top, and estimate/total counts reset.
   * During this transition, some renders has a mix of new list query params and
   * list results and old anchor (as anchor reset is async via setState), it is
   * important to:
   * 1. avoid creating a query with the new query params but the old anchor, as
   *    that would be loading a query that is not the correct one to display,
   *    accomplished by using TOP_ANCHOR when
   *    listContextParams !== queryAnchor.listContextParams
   * 2. avoid calculating counts based on a mix of new list results and old
   *    anchor, avoided by not updating counts when
   *    listContextParams !== queryAnchor.listContextParams
   * 3. avoid updating anchor for paging based on a mix of new list results and
   *    old anchor, avoided by not doing paging updates when
   *    listContextParams !== queryAnchor.listContextParams
   */
  readonly listContextParams: ListContextParams;
};

export const anchorSchema = zod.discriminatedUnion('kind', [
  zod.readonly(
    zod.object({
      index: zod.number(),
      kind: zod.literal('forward'),
      startRow: zod.optional(issueRowSortSchema),
    }),
  ),
  zod.readonly(
    zod.object({
      index: zod.number(),
      kind: zod.literal('backward'),
      startRow: issueRowSortSchema,
    }),
  ),
  zod.readonly(
    zod.object({
      index: zod.number(),
      kind: zod.literal('permalink'),
      id: zod.string(),
    }),
  ),
]);

export type Anchor = zod.infer<typeof anchorSchema>;

const permalinkHistoryStateSchema = zod.readonly(
  zod.looseObject({
    anchor: anchorSchema,
    scrollTop: zod.number(),
    estimatedTotal: zod.number(),
    hasReachedStart: zod.boolean(),
    hasReachedEnd: zod.boolean(),
  }),
);

type PermalinkHistoryState = zod.infer<typeof permalinkHistoryStateSchema>;

export const TOP_ANCHOR = Object.freeze({
  index: 0,
  kind: 'forward',
  startRow: undefined,
});

export function useZeroVirtualizer<
  TScrollElement extends Element,
  TItemElement extends Element,
>({
  // Tanstack Virtual params
  estimateSize,
  overscan = 5, // Virtualizer defaults to 1.
  getScrollElement,

  // Zero specific params
  listContext,
  userID,
  textFilterQuery,
  textFilter,
  permalinkID,
}: {
  // Tanstack Virtual params
  estimateSize: (index: number) => number;
  overscan?: number;
  getScrollElement: VirtualizerOptions<
    TScrollElement,
    TItemElement
  >['getScrollElement'];

  // Zero specific params
  listContext: ListContextParams;
  userID: string;
  textFilterQuery: string | null;
  textFilter: string | null;
  permalinkID: string | null;
}): {
  virtualizer: Virtualizer<TScrollElement, TItemElement>;
  issueAt: (index: number) => Issue | undefined;
  complete: boolean;
  issuesEmpty: boolean;
  permalinkNotFound: boolean;
  estimatedTotal: number;
  total: number | undefined;
  virtualItems: VirtualItem[];
} {
  const [estimatedTotal, setEstimatedTotal] = useState(
    NUM_ROWS_FOR_LOADING_SKELETON,
  );
  const [hasReachedStart, setHasReachedStart] = useState(false);
  const [hasReachedEnd, setHasReachedEnd] = useState(false);
  const [skipPagingLogic, setSkipPagingLogic] = useState(false);
  const [isScrolling, setIsScrolling] = useState(false);

  // Initialize queryAnchor from history.state directly to avoid Strict Mode double-mount issues
  const [queryAnchor, setQueryAnchor] = useState<QueryAnchor>(() => {
    const {state} = history;
    const parseResult = permalinkHistoryStateSchema.safeParse(state);
    const anchor =
      parseResult.success && parseResult.data.anchor
        ? parseResult.data.anchor
        : permalinkID
          ? ({
              index: NUM_ROWS_FOR_LOADING_SKELETON,
              kind: 'permalink',
              id: permalinkID,
            } satisfies Anchor)
          : TOP_ANCHOR;
    return {
      anchor,
      listContextParams: listContext,
    };
  });

  const isListContextCurrent = useMemo(
    () => queryAnchor.listContextParams === listContext,
    [queryAnchor.listContextParams, listContext],
  );

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
      } satisfies Anchor;
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
    listContext,
    userID,
    pageSize,
    anchor,
    options:
      !isScrolling && textFilterQuery === textFilter ? CACHE_NAV : CACHE_NONE,
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

  // TODO(arv): Do we need this state?
  useEffect(() => {
    setIsScrolling(virtualizer.isScrolling);
  }, [virtualizer.isScrolling]);

  const updateHistoryState = useDebouncedCallback(() => {
    replaceHistoryState<PermalinkHistoryState>({
      anchor,
      scrollTop: virtualizer.scrollOffset ?? 0,
      estimatedTotal,
      hasReachedStart,
      hasReachedEnd,
    });
  }, 100);

  useEffect(() => {
    updateHistoryState();
  }, [
    anchor,
    virtualizer.scrollOffset,
    estimatedTotal,
    hasReachedStart,
    hasReachedEnd,
    updateHistoryState,
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

  // TODO:(arv): DRY
  // TODO(arv): Do not depend on wouter!
  const maybeHistoryState = useHistoryState<PermalinkHistoryState | null>();
  const res = permalinkHistoryStateSchema.safeParse(maybeHistoryState);
  const historyState = res.success ? res.data : null;

  const [pendingScrollAdjustment, setPendingScrollAdjustment] =
    useState<number>(0);

  const updateAnchor = (anchor: Anchor) => {
    setQueryAnchor({
      anchor,
      listContextParams: listContext,
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
        updateAnchor(historyState.anchor);
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
      } as Anchor);
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

  return useMemo(
    () => ({
      virtualizer,
      issueAt,
      issuesLength,
      complete,
      issuesEmpty,
      permalinkNotFound,
      hasReachedStart,
      hasReachedEnd,
      estimatedTotal,
      total,
      virtualItems,
    }),
    [
      virtualizer,
      issueAt,
      issuesLength,
      complete,
      issuesEmpty,
      atStart,
      atEnd,
      firstIssueIndex,
      permalinkNotFound,
      estimatedTotal,
      total,
      virtualItems,
    ],
  );
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
