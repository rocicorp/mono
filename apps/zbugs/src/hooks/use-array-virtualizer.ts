import { useVirtualizer, type Virtualizer } from '@rocicorp/react-virtual';
import { useCallback, useEffect, useRef, useState } from 'react';
import { assert } from 'shared/src/asserts.ts';
import {
  useRows,
  type GetPageQuery,
  type GetSingleQuery,
} from '../../../../packages/zero-react/src/use-rows.js';

export interface UseArrayVirtualizerOptions<T, TSort> {
  pageSize: number;
  placeholderHeight: number;
  estimateSize: (row: T | undefined, index: number) => number;
  getScrollElement: () => HTMLElement | null;
  getPageQuery: GetPageQuery<T, TSort>;
  getSingleQuery: GetSingleQuery<T>;
  toStartRow: (row: T) => TSort;
  initialPermalinkID?: string | undefined;
  debug?: boolean | undefined;
  overscan?: number | undefined;
}

export interface ScrollRestorationState<TSort> {
  anchorIndex: number;
  anchorKind: 'forward' | 'backward' | 'permalink';
  permalinkID?: string | undefined;
  startRow: TSort | undefined;
  scrollOffset: number;
  // For permalink restoration: ID of first visible item and offset from its start
  firstVisibleItemID?: string | undefined;
  scrollOffsetFromFirstVisible?: number | undefined;
}

export interface UseArrayVirtualizerReturn<T, TSort> {
  virtualizer: Virtualizer<HTMLElement, Element>;
  rowAt: (index: number) => T | undefined;
  rowsEmpty: boolean;
  permalinkNotFound: boolean;
  anchorState: {
    anchorIndex: number;
    anchorKind: 'forward' | 'backward' | 'permalink';
    permalinkID: string | undefined;
    startRow: TSort | undefined;
    scrollOffset: number;
    firstVisibleItemID?: string | undefined;
    scrollOffsetFromFirstVisible?: number | undefined;
  };
  restoreAnchorState: (state: {
    anchorIndex: number;
    anchorKind: 'forward' | 'backward' | 'permalink';
    permalinkID?: string | undefined;
    startRow: TSort | undefined;
    scrollOffset?: number;
    firstVisibleItemID?: string | undefined;
    scrollOffsetFromFirstVisible?: number | undefined;
  }) => void;
}

export function useArrayVirtualizer<T, TSort>({
  pageSize,
  placeholderHeight,
  estimateSize: estimateSizeCallback,
  getScrollElement,
  getPageQuery,
  getSingleQuery,
  toStartRow,
  initialPermalinkID,
  debug = false,
  overscan = 5,
}: UseArrayVirtualizerOptions<T, TSort>): UseArrayVirtualizerReturn<T, TSort> {
  const [anchorIndex, setAnchorIndex] = useState(0);
  const [anchorKind, setAnchorKind] = useState<
    'forward' | 'backward' | 'permalink'
  >(initialPermalinkID ? 'permalink' : 'forward');
  const [startRow, setStartRow] = useState<TSort | undefined>(undefined);
  const [permalinkID, setPermalinkID] = useState<string | undefined>(
    initialPermalinkID,
  );
  const [autoPagingEnabled, setAutoPagingEnabled] =
    useState<boolean>(!initialPermalinkID);

  // Track if we've positioned the permalink
  const hasPositionedPermalinkRef = useRef(false);

  // Track if we're waiting for data to complete after an anchor shift
  const waitingForCompleteRef = useRef(false);
  const hasSeenIncompleteRef = useRef(false);

  // Track scroll stability to prevent auto-paging during positioning/restoration
  const lastScrollOffsetRef = useRef<number | null>(null);
  const scrollStableCountRef = useRef(0);
  const STABLE_SCROLL_THRESHOLD = 2; // Scroll offset must be unchanged for N renders
  const PERMALINK_SCROLL_THRESHOLD = 2;
  const permalinkScrollOffsetRef = useRef<number | null>(null);
  const hasScrolledFromPermalinkRef = useRef(false);

  const resetScrollStability = useCallback(() => {
    lastScrollOffsetRef.current = null;
    scrollStableCountRef.current = 0;
  }, []);

  const resetPermalinkScrollTracking = useCallback(() => {
    permalinkScrollOffsetRef.current = null;
    hasScrolledFromPermalinkRef.current = false;
  }, []);

  // Track scroll offset to restore after state change
  const [restoreScrollOffset, setRestoreScrollOffset] = useState<number | null>(
    null,
  );

  // Track first visible item for permalink restoration
  const [restoreFirstVisibleItemID, setRestoreFirstVisibleItemID] = useState<
    string | null
  >(null);
  const [
    restoreScrollOffsetFromFirstVisible,
    setRestoreScrollOffsetFromFirstVisible,
  ] = useState<number | null>(null);

  useEffect(() => {
    if (initialPermalinkID) {
      console.log('Setting anchorKind to permalink: initial permalink ID provided', {initialPermalinkID});
      setAnchorKind('permalink');
      setAnchorIndex(0);
      setPermalinkID(initialPermalinkID);
      setStartRow(undefined);
      setAutoPagingEnabled(false);
      resetPermalinkScrollTracking();
      hasPositionedPermalinkRef.current = false;
    } else {
      // Reset to top when permalink is cleared
      console.log('Setting anchorKind to forward: permalink cleared, resetting to top');
      setAnchorKind('forward');
      setAnchorIndex(0);
      setPermalinkID(undefined);
      setStartRow(undefined);
      setAutoPagingEnabled(false); // Will be enabled once scroll is stable
      resetScrollStability();
      resetPermalinkScrollTracking();
      hasPositionedPermalinkRef.current = false;
    }
  }, [initialPermalinkID, resetPermalinkScrollTracking, resetScrollStability]);

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
    anchor:
      anchorKind === 'permalink' && permalinkID
        ? {
            kind: 'permalink',
            index: anchorIndex,
            id: permalinkID,
          }
        : anchorKind === 'forward'
          ? {
              kind: 'forward',
              index: anchorIndex,
              startRow,
            }
          : anchorKind === 'backward' && startRow
            ? {
                kind: 'backward',
                index: anchorIndex,
                startRow,
              }
            : {
                kind: 'forward',
                index: anchorIndex,
                startRow,
              },
    getPageQuery,
    getSingleQuery,
    toStartRow,
  });

  const endPlaceholder = atEnd ? 0 : 1;
  const startPlaceholder = atStart ? 0 : 1;

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

      // Start placeholder
      if (!atStart && index === 0) {
        return undefined;
      }
      // End placeholder
      if (!atEnd && index === virtualizerCount - 1) {
        return undefined;
      }
      // Map virtualizer index to logical index, accounting for start placeholder
      const logicalIndex = firstRowIndex + (index - startPlaceholder);
      return rowAt(logicalIndex);
    },
    [rowAt, firstRowIndex, virtualizerCount, atEnd, atStart, startPlaceholder],
  );

  const estimateSize = useCallback(
    (index: number) => {
      const row = rowAtVirtualIndex(index);
      if (!row) {
        return placeholderHeight;
      }
      return estimateSizeCallback(row, index);
    },
    [rowAtVirtualIndex, placeholderHeight, estimateSizeCallback],
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
        // For placeholders, use a unique key based on position
        if (!atStart && index === 0) {
          return `placeholder-start`;
        }
        return `placeholder-end-${index}`;
      },
      [rowAtVirtualIndex, atStart],
    ),
    overscan,
    debug,
  });

  // Helper to check if scroll position is stable (defined after virtualizer)
  const isScrollStable = useCallback(() => {
    const currentOffset = virtualizer.scrollOffset ?? 0;
    
    if (lastScrollOffsetRef.current === null) {
      lastScrollOffsetRef.current = currentOffset;
      scrollStableCountRef.current = 0;
      return false;
    }
    
    if (Math.abs(currentOffset - lastScrollOffsetRef.current) < 1) {
      scrollStableCountRef.current++;
    } else {
      scrollStableCountRef.current = 0;
    }
    
    lastScrollOffsetRef.current = currentOffset;
    return scrollStableCountRef.current >= STABLE_SCROLL_THRESHOLD;
  }, [virtualizer]);

  // Force remeasurement when estimateSize function changes (e.g., heightMode changes)
  useEffect(() => {
    virtualizer.measure();
  }, [estimateSizeCallback, virtualizer]);

  // Scroll to top when resetting from permalink to forward anchor
  useEffect(() => {
    if (
      anchorKind === 'forward' &&
      anchorIndex === 0 &&
      !permalinkID &&
      restoreScrollOffset !== null
    ) {
      console.log('Resetting scroll to top'); // Debug log
      virtualizer.scrollToOffset(restoreScrollOffset);
      setRestoreScrollOffset(null);
    }
  }, [anchorKind, anchorIndex, permalinkID, virtualizer, restoreScrollOffset]);

  const virtualItems = virtualizer.getVirtualItems();

  // Track when the user scrolls away from the permalink position.
  useEffect(() => {
    if (anchorKind !== 'permalink') {
      resetPermalinkScrollTracking();
      return;
    }

    if (
      permalinkScrollOffsetRef.current === null &&
      complete &&
      isScrollStable()
    ) {
      permalinkScrollOffsetRef.current = virtualizer.scrollOffset ?? 0;
      hasScrolledFromPermalinkRef.current = false;
    }

    if (
      !autoPagingEnabled &&
      permalinkScrollOffsetRef.current !== null &&
      !hasScrolledFromPermalinkRef.current
    ) {
      const delta = Math.abs(
        (virtualizer.scrollOffset ?? 0) - permalinkScrollOffsetRef.current,
      );
      if (delta > PERMALINK_SCROLL_THRESHOLD) {
        hasScrolledFromPermalinkRef.current = true;
        setAutoPagingEnabled(true);
      }
    }
  }, [
    anchorKind,
    autoPagingEnabled,
    complete,
    isScrollStable,
    resetPermalinkScrollTracking,
    virtualItems,
    virtualizer,
  ]);

  // Handle permalink positioning and enable auto-paging when ready
  useEffect(() => {
    // Reset positioning flag when switching modes
    if (anchorKind !== 'permalink' && restoreScrollOffset === null) {
      hasPositionedPermalinkRef.current = false;
      if (!autoPagingEnabled && isScrollStable()) {
        console.log('Enabling auto-paging: scroll is stable', {
          anchorKind,
          anchorIndex,
          permalinkID,
          startRow,
        }); // Debug log
        setAutoPagingEnabled(true);
      }
      return;
    }

    if (rowsLength === 0) {
      return;
    }

    // Restore scroll offset when data is complete
    if (
      anchorKind !== 'permalink' &&
      restoreScrollOffset !== null &&
      complete &&
      !hasPositionedPermalinkRef.current
    ) {
      assert(
        anchorKind !== 'permalink',
        'Do not scroll to offset with permalink',
      );
      console.log('Restoring scroll offset using scrollToOffset:', {
        restoreScrollOffset,
        'hasPositionedPermalinkRef.current': hasPositionedPermalinkRef.current,
        complete,
        permalinkID,
        anchorKind,
        anchorIndex,
        startRow,
      }); // Debug log

      virtualizer.scrollToOffset(restoreScrollOffset);
      setRestoreScrollOffset(null);
      hasPositionedPermalinkRef.current = true;
      return;
    }

    // Permalink row is loaded - ensure it's at the correct scroll position
    if (
      (!hasPositionedPermalinkRef.current || !complete) &&
      anchorIndex !== null
    ) {
      assert(
        anchorKind === 'permalink',
        'Only restore by index with permalink',
      );
      const permalinkLogicalIndex = anchorIndex;
      const targetVirtualIndex =
        permalinkLogicalIndex - firstRowIndex + startPlaceholder;

      console.log('Positioning permalink at virtual index:', {
        targetVirtualIndex,
        complete,
      }); // Debug log

      virtualizer.scrollToIndex(targetVirtualIndex, {
        align: 'start',
      });

      if (complete) {
        hasPositionedPermalinkRef.current = true;
      }
    }

    // Once complete and scroll is stable, enable auto-paging (non-permalink only)
    if (
      anchorKind !== 'permalink' &&
      complete &&
      !autoPagingEnabled &&
      isScrollStable()
    ) {
      console.log('Enabling auto-paging: data complete and scroll stable');
      setAutoPagingEnabled(true);
    }
  }, [
    anchorKind,
    rowsLength,
    complete,
    autoPagingEnabled,
    anchorIndex,
    startPlaceholder,
    firstRowIndex,
    virtualizer,
    restoreScrollOffset,
    restoreFirstVisibleItemID,
    restoreScrollOffsetFromFirstVisible,
    rowAt,
    isScrollStable,
  ]);

  // Auto-shift anchor forward when scrolling near the end of the data window
  useEffect(() => {
    if (!autoPagingEnabled || virtualItems.length === 0 || atEnd) {
      return;
    }

    if (anchorKind === 'permalink' && !hasScrolledFromPermalinkRef.current) {
      return;
    }

    if (waitingForCompleteRef.current && !complete) {
      hasSeenIncompleteRef.current = true;
      return;
    }

    if (
      waitingForCompleteRef.current &&
      complete &&
      hasSeenIncompleteRef.current
    ) {
      waitingForCompleteRef.current = false;
      hasSeenIncompleteRef.current = false;
    }

    // Find last non-placeholder item
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

      if (!lastRow) {
        return;
      }
    }

    const lastLogicalIndex = toLogicalIndex(lastItem.index);
    const lastDataIndex = firstRowIndex + rowsLength - 1;
    const distanceFromEnd = lastDataIndex - lastLogicalIndex;
    const nearPageEdgeThreshold = Math.ceil(pageSize * 0.1);

    if (distanceFromEnd <= nearPageEdgeThreshold) {
      const newAnchorIndex = lastLogicalIndex - Math.ceil(pageSize * 0.6);
      const newAnchorRow = rowAt(newAnchorIndex);

      if (newAnchorRow && newAnchorIndex !== anchorIndex) {
        waitingForCompleteRef.current = true;
        console.log('Setting anchorKind to forward: auto-paging forward near end of window', {newAnchorIndex, distanceFromEnd, nearPageEdgeThreshold});
        setAnchorKind('forward');
        setAnchorIndex(newAnchorIndex);
        setStartRow(toStartRow(newAnchorRow));
        // Keep auto-paging enabled during continuous user scrolls
      }
    }
  }, [
    virtualItems,
    atEnd,
    firstRowIndex,
    rowsLength,
    toLogicalIndex,
    rowAt,
    anchorIndex,
    complete,
    autoPagingEnabled,
    rowAtVirtualIndex,
    pageSize,
    toStartRow,
  ]);

  // Auto-shift anchor backward when scrolling near the start of the data window
  useEffect(() => {
    if (!autoPagingEnabled || virtualItems.length === 0 || atStart) {
      return;
    }

    if (anchorKind === 'permalink' && !hasScrolledFromPermalinkRef.current) {
      return;
    }

    if (waitingForCompleteRef.current && !complete) {
      hasSeenIncompleteRef.current = true;
      return;
    }

    if (
      waitingForCompleteRef.current &&
      complete &&
      hasSeenIncompleteRef.current
    ) {
      waitingForCompleteRef.current = false;
      hasSeenIncompleteRef.current = false;
    }

    // Find first non-placeholder item
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

      if (!firstRow) {
        return;
      }
    }

    const firstLogicalIndex = toLogicalIndex(firstItem.index);
    const distanceFromStart = firstLogicalIndex - firstRowIndex;
    const nearPageEdgeThreshold = Math.ceil(pageSize * 0.1);

    if (distanceFromStart <= nearPageEdgeThreshold) {
      const newAnchorIndex = firstLogicalIndex + Math.ceil(pageSize * 0.6);
      const newAnchorRow = rowAt(newAnchorIndex);

      if (newAnchorRow && newAnchorIndex !== anchorIndex) {
        waitingForCompleteRef.current = true;
        console.log('Setting anchorKind to backward: auto-paging backward near start of window', {newAnchorIndex, distanceFromStart, nearPageEdgeThreshold});
        setAnchorKind('backward');
        setAnchorIndex(newAnchorIndex);
        setStartRow(toStartRow(newAnchorRow));
        // Keep auto-paging enabled during continuous user scrolls
        // TODO(arv): Remove
        // setPermalinkID(undefined);
      }
    }
  }, [
    virtualItems,
    atStart,
    firstRowIndex,
    rowsLength,
    toLogicalIndex,
    rowAt,
    anchorIndex,
    complete,
    autoPagingEnabled,
    anchorKind,
    rowAtVirtualIndex,
    pageSize,
    toStartRow,
  ]);

  const restoreAnchorState = useCallback(
    (state: {
      anchorIndex: number;
      anchorKind: 'forward' | 'backward' | 'permalink';
      permalinkID?: string | undefined;
      startRow: TSort | undefined;
      scrollOffset?: number;
      firstVisibleItemID?: string | undefined;
      scrollOffsetFromFirstVisible?: number | undefined;
    }) => {
      console.log('Setting anchorKind via restoreAnchorState:', {anchorKind: state.anchorKind, anchorIndex: state.anchorIndex, permalinkID: state.permalinkID});
      setAnchorIndex(state.anchorIndex);
      setAnchorKind(state.anchorKind);
      setPermalinkID(state.permalinkID);
      setStartRow(state.startRow);
      setAutoPagingEnabled(false);
      hasPositionedPermalinkRef.current = false;
      setRestoreScrollOffset(state.scrollOffset ?? null);
      setRestoreFirstVisibleItemID(state.firstVisibleItemID ?? null);
      setRestoreScrollOffsetFromFirstVisible(
        state.scrollOffsetFromFirstVisible ?? null,
      );
    },
    [],
  );

  // Convert permalink to forward page for state capture
  const captureAnchorState = useCallback(() => {
    if (anchorKind === 'permalink') {
      const scrollOffset = virtualizer.scrollOffset ?? 0;

      // Find the first visible (non-placeholder) item
      let firstVisibleItem = virtualItems[0];
      let firstVisibleRow = rowAtVirtualIndex(firstVisibleItem?.index);

      if (!firstVisibleRow && virtualItems.length > 0) {
        for (let i = 1; i < virtualItems.length; i++) {
          const item = virtualItems[i];
          const row = rowAtVirtualIndex(item.index);
          if (row) {
            firstVisibleItem = item;
            firstVisibleRow = row;
            break;
          }
        }
      }

      if (!firstVisibleRow || !firstVisibleItem) {
        // Fallback if no visible row found
        return {
          anchorIndex,
          anchorKind,
          permalinkID,
          startRow,
          scrollOffset,
        };
      }

      // Extract the ID from the first visible row to use as permalink
      const firstVisibleItemID =
        typeof firstVisibleRow === 'object' &&
        firstVisibleRow !== null &&
        'id' in firstVisibleRow
          ? String(firstVisibleRow.id)
          : undefined;

      if (!firstVisibleItemID) {
        // Fallback if we can't get an ID
        return {
          anchorIndex,
          anchorKind,
          permalinkID,
          startRow,
          scrollOffset,
        };
      }

      const scrollOffsetFromFirstVisible = Math.max(
        0,
        scrollOffset - firstVisibleItem.start,
      );

      // Keep as permalink anchor but use first visible row's ID
      return {
        anchorIndex: 0,
        anchorKind: 'permalink' as const,
        permalinkID: firstVisibleItemID,
        startRow: undefined,
        scrollOffset: scrollOffsetFromFirstVisible,
        firstVisibleItemID,
        scrollOffsetFromFirstVisible,
      };
    }

    // Return current state as-is
    return {
      anchorIndex,
      anchorKind,
      permalinkID,
      startRow,
      scrollOffset: virtualizer.scrollOffset ?? 0,
    };
  }, [
    anchorKind,
    anchorIndex,
    permalinkID,
    startRow,
    virtualizer,
    virtualItems,
    rowAtVirtualIndex,
  ]);

  return {
    virtualizer,
    rowAt: rowAtVirtualIndex,
    rowsEmpty,
    permalinkNotFound,
    anchorState: captureAnchorState(),
    restoreAnchorState,
  };
}
