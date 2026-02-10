import {useVirtualizer, type Virtualizer} from '@rocicorp/react-virtual';
import {useCallback, useEffect, useMemo, useRef, useState} from 'react';
import {
  useRows,
  type GetPageQuery,
  type GetSingleQuery,
} from '../../../../packages/zero-react/src/use-rows.js';

// Make sure this is even since we half it for permalink loading
const MIN_PAGE_SIZE = 100;

function makeEven(n: number) {
  return n % 2 === 0 ? n : n + 1;
}

export interface UseArrayVirtualizerOptions<T, TSort> {
  estimateSize: (row: T | undefined, index: number) => number;
  getScrollElement: () => HTMLElement | null;
  getPageQuery: GetPageQuery<T, TSort>;
  getSingleQuery: GetSingleQuery<T>;
  toStartRow: (row: T) => TSort;
  initialPermalinkID?: string | undefined;
  /**
   * Controlled scroll state. When the consumer provides a new value that
   * differs from what the hook last reported via `onScrollStateChange`, the
   * virtualizer will restore to that position.
   *
   * Setting `undefined` scrolls to the top.
   */
  scrollState?: ScrollRestorationState | undefined;
  /**
   * Called whenever the current scroll position changes (every render where
   * the anchor row or offset changed). The consumer can store this in React
   * state, `history.state`, or anywhere else.
   */
  onScrollStateChange?: ((state: ScrollRestorationState) => void) | undefined;
  debug?: boolean | undefined;
  overscan?: number | undefined;
}

export type ScrollRestorationState = {
  scrollAnchorID: string;
  index: number;
  scrollOffset: number;
};

export interface UseArrayVirtualizerReturn<T> {
  virtualizer: Virtualizer<HTMLElement, Element>;
  rowAt: (index: number) => T | undefined;
  complete: boolean;
  rowsEmpty: boolean;
  permalinkNotFound: boolean;
  estimatedTotal: number;
  total: number | undefined;
}

type ForwardAnchorState<TSort> = {
  kind: 'forward';
  index: number;
  startRow: TSort | undefined;
};

type BackwardAnchorState<TSort> = {
  kind: 'backward';
  index: number;
  startRow: TSort;
};

type PermalinkAnchorState = {
  kind: 'permalink';
  index: number;
  permalinkID: string;
};

type AnchorState<TSort> =
  | ForwardAnchorState<TSort>
  | BackwardAnchorState<TSort>
  | PermalinkAnchorState;

const anchorsEqual = <TSort>(a: AnchorState<TSort>, b: AnchorState<TSort>) => {
  if (a.index !== b.index) {
    return false;
  }

  if (a.kind === 'permalink' && b.kind === 'permalink') {
    return a.permalinkID === b.permalinkID;
  }

  if (a.kind === 'forward' && b.kind === 'forward') {
    return a.startRow === b.startRow;
  }

  if (a.kind === 'backward' && b.kind === 'backward') {
    return a.startRow === b.startRow;
  }

  return false;
};

const scrollStatesEqual = (
  a: ScrollRestorationState | undefined,
  b: ScrollRestorationState | undefined,
): boolean => {
  if (a === b) {
    return true;
  }
  if (!a || !b) {
    return false;
  }
  return (
    a.scrollAnchorID === b.scrollAnchorID &&
    a.index === b.index &&
    a.scrollOffset === b.scrollOffset
  );
};

// Delay after positioning before enabling auto-paging.
// Allows virtualItems to update after programmatic scroll.
const POSITIONING_SETTLE_DELAY_MS = 50;

export function useArrayVirtualizer<T, TSort>({
  estimateSize: estimateSizeCallback,
  getScrollElement,
  getPageQuery,
  getSingleQuery,
  toStartRow,
  initialPermalinkID,
  scrollState: externalScrollState,
  onScrollStateChange,
  debug = false,
  overscan = 5,
}: UseArrayVirtualizerOptions<T, TSort>): UseArrayVirtualizerReturn<T> {
  const [pageSize, setPageSize] = useState(MIN_PAGE_SIZE);
  const [anchor, setAnchor] = useState<AnchorState<TSort>>(() =>
    initialPermalinkID
      ? {
          kind: 'permalink',
          index: 0,
          permalinkID: initialPermalinkID,
        }
      : {
          kind: 'forward',
          index: 0,
          startRow: undefined,
        },
  );

  // Track min/max indices seen to calculate total counts
  const [minIndexSeen, setMinIndexSeen] = useState<number | null>(null);
  const [maxIndexSeen, setMaxIndexSeen] = useState<number | null>(null);
  const [hasReachedStart, setHasReachedStart] = useState(false);
  const [hasReachedEnd, setHasReachedEnd] = useState(false);

  const scrollInternalRef = useRef({
    pendingScroll: null as number | null,
    pendingScrollIsRelative: false,
    scrollRetryCount: 0,
    // When there is an initial permalink, start at 0 so the positioning
    // loop activates and emission is suppressed until positioning completes.
    // Otherwise use Date.now() so emission (and auto-paging) starts
    // immediately.
    positionedAt: initialPermalinkID ? 0 : Date.now(),
    lastTargetOffset: null as number | null,
    // The anchor that the merged effect has requested.  Used by the
    // positioning effect to skip stale positioning when the React state
    // update from replaceAnchor hasn't committed yet.
    expectedAnchorID: initialPermalinkID ?? (null as string | null),
  });

  // Track the last state we emitted via onScrollStateChange so we can
  // distinguish "consumer reflected back what we told them" (no-op) from
  // "consumer set a new position externally" (trigger restore).
  const lastEmittedStateRef = useRef<ScrollRestorationState | undefined>(
    undefined,
  );

  const replaceAnchor = useCallback(
    (next: AnchorState<TSort>) =>
      setAnchor(prev => (anchorsEqual(prev, next) ? prev : next)),
    [],
  );

  // Track previous values so we can detect actual changes inside a
  // single unified effect.  `prevExternalStateRef` starts as `undefined`
  // (not the initial prop) so that a mount-time value from history.state
  // is treated as an external change and triggers a restore.
  const prevExternalStateRef = useRef<ScrollRestorationState | undefined>(
    undefined,
  );
  const prevPermalinkIDRef = useRef(initialPermalinkID);

  // ---- Unified effect: external scroll state + permalink changes ----
  // External scroll-state changes take priority over URL-hash changes so
  // that back/forward (which update both simultaneously) restores the
  // offset instead of re-positioning to the top of the permalink.
  useEffect(() => {
    const externalChanged = !scrollStatesEqual(
      externalScrollState,
      prevExternalStateRef.current,
    );
    const permalinkChanged = initialPermalinkID !== prevPermalinkIDRef.current;

    prevExternalStateRef.current = externalScrollState;
    prevPermalinkIDRef.current = initialPermalinkID;

    // --- External scroll state (restore from history.state, etc.) ---
    // When both external state and permalink changed simultaneously and
    // the external state is undefined, it means we traversed to a history
    // entry where scroll state was never saved (e.g. the user navigated
    // away before the throttled save fired).  Prefer the permalink branch
    // so we position to the hash rather than scroll to top.
    if (
      externalChanged &&
      !scrollStatesEqual(externalScrollState, lastEmittedStateRef.current) &&
      !(externalScrollState === undefined && permalinkChanged)
    ) {
      if (!externalScrollState) {
        // undefined → scroll to top
        replaceAnchor({
          kind: 'forward',
          index: 0,
          startRow: undefined,
        });
        scrollInternalRef.current.positionedAt = 0;
        scrollInternalRef.current.scrollRetryCount = 0;
        scrollInternalRef.current.pendingScroll = 0;
        scrollInternalRef.current.pendingScrollIsRelative = false;
        scrollInternalRef.current.lastTargetOffset = null;
        scrollInternalRef.current.expectedAnchorID = null;
        lastEmittedStateRef.current = undefined;
        return;
      }

      replaceAnchor({
        kind: 'permalink',
        index: externalScrollState.index,
        permalinkID: externalScrollState.scrollAnchorID,
      });
      scrollInternalRef.current.positionedAt = 0;
      scrollInternalRef.current.scrollRetryCount = 0;
      scrollInternalRef.current.pendingScroll =
        externalScrollState.scrollOffset;
      scrollInternalRef.current.pendingScrollIsRelative = true;
      scrollInternalRef.current.lastTargetOffset = null;
      scrollInternalRef.current.expectedAnchorID =
        externalScrollState.scrollAnchorID;
      lastEmittedStateRef.current = externalScrollState;
      return;
    }

    // --- URL hash navigation (initialPermalinkID changed) ---
    if (!permalinkChanged) {
      return;
    }

    const nextAnchor: AnchorState<TSort> = initialPermalinkID
      ? {
          kind: 'permalink',
          index: 0,
          permalinkID: initialPermalinkID,
        }
      : {
          kind: 'forward',
          index: 0,
          startRow: undefined,
        };

    replaceAnchor(nextAnchor);
    scrollInternalRef.current.scrollRetryCount = 0;
    scrollInternalRef.current.lastTargetOffset = null;

    if (initialPermalinkID) {
      // Navigating TO a permalink — activate the positioning loop.
      scrollInternalRef.current.positionedAt = 0;
      scrollInternalRef.current.expectedAnchorID = initialPermalinkID;
      scrollInternalRef.current.pendingScroll = null;
      scrollInternalRef.current.pendingScrollIsRelative = false;
    } else {
      // Permalink cleared (hash removed).  Queue a scroll-to-top via
      // pendingScroll but do NOT reset positionedAt or expectedAnchorID.
      // When going back/forward, both hash and scrollState change at the
      // same time.  If they arrive in separate renders (Wouter fires the
      // hash change first), resetting positionedAt would reactivate the
      // positioning loop for the stale anchor.  By leaving positionedAt
      // untouched the stale anchor is ignored, and the external-state
      // branch (which fires on the next render) overwrites pendingScroll
      // with the correct restore position.
      scrollInternalRef.current.pendingScroll = 0;
      scrollInternalRef.current.pendingScrollIsRelative = false;
    }
  }, [initialPermalinkID, externalScrollState, replaceAnchor]);

  const {
    rowAt,
    rowsLength,
    complete,
    rowsEmpty,
    atStart,
    atEnd,
    firstRowIndex,
    permalinkNotFound,
  } = useRows<T, TSort>({
    pageSize,
    anchor: useMemo(() => {
      if (anchor.kind === 'permalink') {
        return {
          kind: 'permalink',
          index: anchor.index,
          id: anchor.permalinkID,
        };
      }

      if (anchor.kind === 'forward') {
        return {
          kind: 'forward',
          index: anchor.index,
          startRow: anchor.startRow,
        };
      }

      anchor.kind satisfies 'backward';
      return {
        kind: 'backward',
        index: anchor.index,
        startRow: anchor.startRow,
      };
    }, [anchor]),
    getPageQuery,
    getSingleQuery,
    toStartRow,
  });

  const endPlaceholder = atEnd ? 0 : 1;
  const startPlaceholder = atStart ? 0 : 1;

  // Track min/max indices seen so far
  useEffect(() => {
    if (rowsLength === 0) {
      return;
    }

    const currentMin = firstRowIndex;
    const currentMax = firstRowIndex + rowsLength - 1;

    if (minIndexSeen === null || currentMin < minIndexSeen) {
      setMinIndexSeen(currentMin);
    }

    if (maxIndexSeen === null || currentMax > maxIndexSeen) {
      setMaxIndexSeen(currentMax);
    }
  }, [firstRowIndex, rowsLength, minIndexSeen, maxIndexSeen]);

  // Track when we reach the boundaries
  useEffect(() => {
    if (atStart && !hasReachedStart) {
      setHasReachedStart(true);
    }
  }, [atStart, hasReachedStart]);

  useEffect(() => {
    if (atEnd && !hasReachedEnd) {
      setHasReachedEnd(true);
    }
  }, [atEnd, hasReachedEnd]);

  const placeholderForIndex = useCallback(
    (index: number) => {
      if (!atStart && index === 0) {
        return 'start';
      }

      if (!atEnd && index === startPlaceholder + rowsLength) {
        return 'end';
      }

      return null;
    },
    [atEnd, atStart, rowsLength, startPlaceholder],
  );

  // Convert virtualizer index to logical data index
  const toLogicalIndex = useCallback(
    (virtualizerIndex: number) =>
      firstRowIndex + (virtualizerIndex - startPlaceholder),
    [firstRowIndex, startPlaceholder],
  );

  const virtualizerCount = startPlaceholder + rowsLength + endPlaceholder;

  const rowAtVirtualIndex = useCallback(
    (index: number) => {
      if (index < 0) {
        return undefined;
      }

      if (placeholderForIndex(index)) {
        return undefined;
      }

      const logicalIndex = firstRowIndex + (index - startPlaceholder);
      return rowAt(logicalIndex);
    },
    [placeholderForIndex, rowAt, firstRowIndex, startPlaceholder],
  );

  const estimateSize = useCallback(
    (index: number) => {
      const row = rowAtVirtualIndex(index);
      return estimateSizeCallback(row, index);
    },
    [rowAtVirtualIndex, estimateSizeCallback],
  );

  const virtualizer = useVirtualizer({
    count: virtualizerCount,
    getScrollElement,
    estimateSize,
    getItemKey: useCallback(
      (index: number) => {
        const row = rowAtVirtualIndex(index);
        if (row && typeof row === 'object' && 'id' in row) {
          return (row as {id: string}).id;
        }
        const placeholder = placeholderForIndex(index);
        if (placeholder) {
          return `placeholder-${placeholder}-${index}`;
        }
        return `placeholder-end-${index}`;
      },
      [rowAtVirtualIndex, placeholderForIndex],
    ),
    overscan,
    debug,
  });

  // Force remeasurement when estimateSize function changes
  useEffect(() => {
    virtualizer.measure();
  }, [estimateSizeCallback, virtualizer]);

  // Automatically adjust page size based on viewport height
  useEffect(() => {
    // Make sure page size is enough to fill the scroll element at least
    // 3 times. Don't shrink page size.
    const newPageSize = virtualizer.scrollRect
      ? Math.max(
          MIN_PAGE_SIZE,
          makeEven(
            Math.ceil(
              virtualizer.scrollRect.height /
                // Use first row's estimated size as a proxy
                estimateSize(0),
            ) * 3,
          ),
        )
      : MIN_PAGE_SIZE;
    if (newPageSize > pageSize) {
      setPageSize(newPageSize);
    }
  }, [pageSize, virtualizer.scrollRect, estimateSize]);

  const virtualItems = virtualizer.getVirtualItems();

  // ---- Single unified effect: positioning + auto-paging ----
  useEffect(() => {
    const restoreScrollIfNeeded = () => {
      const state = scrollInternalRef.current;

      if (state.pendingScroll === null || anchor.kind === 'permalink') {
        return false;
      }

      // Relative restores are handled by positionPermalinkIfNeeded once
      // the anchor updates to permalink mode.  Return true to suppress
      // auto-paging while we wait for the anchor state update.
      if (state.pendingScrollIsRelative) {
        return true;
      }

      if (complete && rowsLength > 0) {
        virtualizer.scrollToOffset(state.pendingScroll);
        state.pendingScroll = null;
        state.positionedAt = Date.now();
      }

      return true;
    };

    const positionPermalinkIfNeeded = () => {
      if (anchor.kind !== 'permalink') {
        return false;
      }

      // The merged effect sets expectedAnchorID when it calls
      // replaceAnchor.  If the current anchor doesn't match, the
      // React state update hasn't committed yet — skip to avoid
      // positioning for a stale anchor.
      const state = scrollInternalRef.current;
      if (
        state.expectedAnchorID !== null &&
        anchor.permalinkID !== state.expectedAnchorID
      ) {
        return true; // suppress auto-paging while waiting
      }

      if (rowsLength === 0) {
        return true;
      }

      const targetVirtualIndex =
        anchor.index - firstRowIndex + startPlaceholder;

      if (state.positionedAt === 0 || !complete) {
        if (state.pendingScroll === null) {
          virtualizer.scrollToIndex(targetVirtualIndex, {
            align: 'start',
          });

          if (complete) {
            state.positionedAt = Date.now();
          }

          return true;
        }

        const baseOffset = virtualizer.getOffsetForIndex(
          targetVirtualIndex,
          'start',
        );

        if (state.pendingScrollIsRelative && !baseOffset) {
          state.scrollRetryCount = 0;
          return true;
        }

        let targetOffset = state.pendingScroll;

        if (state.pendingScrollIsRelative) {
          if (!baseOffset) {
            state.scrollRetryCount = 0;
            return true;
          }

          targetOffset = baseOffset[0] + state.pendingScroll;
        }
        virtualizer.scrollToOffset(targetOffset);

        const currentScrollOffset = virtualizer.scrollOffset ?? 0;

        if (Math.abs(currentScrollOffset - targetOffset) <= 1) {
          // Scroll position matches target. Verify that the
          // computed targetOffset has stabilized across effect
          // runs — this ensures item measurements have settled
          // (e.g. after a page reload clears the measurement
          // cache and estimates differ from actual sizes).
          if (
            state.lastTargetOffset !== null &&
            Math.abs(targetOffset - state.lastTargetOffset) <= 1
          ) {
            state.positionedAt = Date.now();
            state.pendingScroll = null;
            state.pendingScrollIsRelative = false;
            state.scrollRetryCount = 0;
            state.lastTargetOffset = null;
            return true;
          }
          // Save current target for comparison on next run.
          state.lastTargetOffset = targetOffset;
        } else {
          // Scroll didn't land where expected — reset stability
          // tracking so we require a fresh pair of matching runs.
          state.lastTargetOffset = null;
        }

        const maxRetries = 10;
        if (state.scrollRetryCount < maxRetries) {
          state.scrollRetryCount++;
        } else {
          state.positionedAt = Date.now();
          state.pendingScroll = null;
          state.pendingScrollIsRelative = false;
          state.scrollRetryCount = 0;
          state.lastTargetOffset = null;
        }

        return true;
      }

      return false;
    };

    const maybeAutoPage = () => {
      if (virtualItems.length === 0 || !complete) {
        return;
      }

      const state = scrollInternalRef.current;

      if (Date.now() - state.positionedAt < POSITIONING_SETTLE_DELAY_MS) {
        return;
      }

      const viewportHeight = virtualizer.scrollRect?.height ?? 0;
      const thresholdDistance = viewportHeight * 2;

      const measuredSizes = new Map<number, number>();
      for (const item of virtualItems) {
        measuredSizes.set(item.index, item.size);
      }

      const getSizeForIndex = (index: number): number => {
        const measured = measuredSizes.get(index);
        if (measured !== undefined) {
          return measured;
        }
        return estimateSize(index);
      };

      if (!atEnd) {
        let lastItem = virtualItems[virtualItems.length - 1];
        let lastRow = rowAtVirtualIndex(lastItem.index);

        if (!lastRow) {
          for (let i = virtualItems.length - 2; i >= 0; i--) {
            const item = virtualItems[i];
            const row = rowAtVirtualIndex(item.index);
            if (row) {
              lastItem = item;
              lastRow = row;
              break;
            }
          }
        }

        if (lastRow) {
          const lastLogicalIndex = toLogicalIndex(lastItem.index);
          const lastDataIndex = firstRowIndex + rowsLength - 1;

          const remainingRowCount = lastDataIndex - lastLogicalIndex;
          let estimatedDistanceToEnd = 0;

          for (let i = 0; i < remainingRowCount && i < 20; i++) {
            estimatedDistanceToEnd += getSizeForIndex(lastItem.index + i + 1);
          }

          if (remainingRowCount > 20) {
            const avgSize = estimatedDistanceToEnd / 20;
            estimatedDistanceToEnd += avgSize * (remainingRowCount - 20);
          }

          if (estimatedDistanceToEnd < thresholdDistance) {
            const newAnchorIndex = lastLogicalIndex - Math.ceil(pageSize * 0.6);
            const newAnchorRow = rowAt(newAnchorIndex);

            if (newAnchorRow && newAnchorIndex !== anchor.index) {
              replaceAnchor({
                kind: 'forward',
                index: newAnchorIndex,
                startRow: toStartRow(newAnchorRow),
              });
              return;
            }
          }
        }
      }

      if (!atStart) {
        let firstItem = virtualItems[0];
        let firstRow = rowAtVirtualIndex(firstItem.index);

        if (!firstRow) {
          for (let i = 1; i < virtualItems.length; i++) {
            const item = virtualItems[i];
            const row = rowAtVirtualIndex(item.index);
            if (row) {
              firstItem = item;
              firstRow = row;
              break;
            }
          }
        }

        if (firstRow) {
          const firstLogicalIndex = toLogicalIndex(firstItem.index);

          const precedingRowCount = firstLogicalIndex - firstRowIndex;
          let estimatedDistanceToStart = 0;

          for (let i = 0; i < precedingRowCount && i < 20; i++) {
            estimatedDistanceToStart += getSizeForIndex(
              firstItem.index - i - 1,
            );
          }

          if (precedingRowCount > 20) {
            const avgSize = estimatedDistanceToStart / 20;
            estimatedDistanceToStart += avgSize * (precedingRowCount - 20);
          }

          if (estimatedDistanceToStart < thresholdDistance) {
            const newAnchorIndex =
              firstLogicalIndex + Math.ceil(pageSize * 0.6);
            const newAnchorRow = rowAt(newAnchorIndex);

            if (newAnchorRow && newAnchorIndex !== anchor.index) {
              replaceAnchor({
                kind: 'backward',
                index: newAnchorIndex,
                startRow: toStartRow(newAnchorRow),
              });
              return;
            }
          }
        }
      }
    };

    if (restoreScrollIfNeeded()) {
      return;
    }

    if (positionPermalinkIfNeeded()) {
      return;
    }

    maybeAutoPage();
  }, [
    anchor,
    rowsLength,
    complete,
    startPlaceholder,
    firstRowIndex,
    virtualizer,
    virtualItems,
    atEnd,
    atStart,
    rowAtVirtualIndex,
    rowAt,
    toLogicalIndex,
    pageSize,
    toStartRow,
    replaceAnchor,
    estimateSize,
  ]);

  // ---- Emit scroll state changes via onScrollStateChange ----
  const onScrollStateChangeRef = useRef(onScrollStateChange);
  onScrollStateChangeRef.current = onScrollStateChange;

  // Compute current scroll state each render (cheap — just iterates
  // virtual items to find the first fully visible row).
  const currentScrollState = useMemo((): ScrollRestorationState | undefined => {
    const scrollOffset = virtualizer.scrollOffset ?? 0;

    // Find the first fully visible item (start >= scrollOffset)
    for (const item of virtualItems) {
      if (item.start < scrollOffset) {
        continue;
      }
      const row = rowAtVirtualIndex(item.index);
      if (row && typeof row === 'object' && 'id' in row) {
        return {
          scrollAnchorID: (row as {id: string}).id,
          index: toLogicalIndex(item.index),
          scrollOffset: scrollOffset - item.start,
        };
      }
    }

    return undefined;
  }, [
    virtualizer.scrollOffset,
    virtualItems,
    rowAtVirtualIndex,
    toLogicalIndex,
  ]);

  useEffect(() => {
    // Don't emit while actively positioning (positionedAt === 0).
    // For the non-permalink initial case, positionedAt is initialized to
    // Date.now() so emission starts immediately.
    if (scrollInternalRef.current.positionedAt === 0) {
      return;
    }

    if (
      currentScrollState &&
      !scrollStatesEqual(currentScrollState, lastEmittedStateRef.current)
    ) {
      lastEmittedStateRef.current = currentScrollState;
      onScrollStateChangeRef.current?.(currentScrollState);
    }
  }, [currentScrollState]);

  // Calculate totals from min/max indices seen
  const estimatedTotal =
    minIndexSeen !== null && maxIndexSeen !== null
      ? maxIndexSeen - minIndexSeen + 1
      : rowsLength;
  const total = hasReachedStart && hasReachedEnd ? estimatedTotal : undefined;

  return {
    virtualizer,
    rowAt: rowAtVirtualIndex,
    complete,
    rowsEmpty,
    permalinkNotFound,
    estimatedTotal,
    total,
  };
}
