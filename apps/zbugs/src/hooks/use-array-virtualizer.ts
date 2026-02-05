import {useVirtualizer, type Virtualizer} from '@rocicorp/react-virtual';
import {useCallback, useEffect, useRef, useState} from 'react';
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
  };
  restoreAnchorState: (state: {
    anchorIndex: number;
    anchorKind: 'forward' | 'backward' | 'permalink';
    permalinkID?: string | undefined;
    startRow: TSort | undefined;
    scrollOffset?: number;
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

  // Track scroll offset to restore after state change
  const [restoreScrollOffset, setRestoreScrollOffset] = useState<number | null>(
    null,
  );

  useEffect(() => {
    if (initialPermalinkID) {
      setAnchorKind('permalink');
      setAnchorIndex(0);
      setPermalinkID(initialPermalinkID);
      setStartRow(undefined);
      setAutoPagingEnabled(false);
      hasPositionedPermalinkRef.current = false;
    } else {
      // Reset to top when permalink is cleared
      setAnchorKind('forward');
      setAnchorIndex(0);
      setPermalinkID(undefined);
      setStartRow(undefined);
      setAutoPagingEnabled(true);
      hasPositionedPermalinkRef.current = false;
    }
  }, [initialPermalinkID]);

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
      virtualizer.scrollToOffset(restoreScrollOffset);
      setRestoreScrollOffset(null);
    }
  }, [anchorKind, anchorIndex, permalinkID, virtualizer, restoreScrollOffset]);

  const virtualItems = virtualizer.getVirtualItems();

  // Handle permalink positioning and enable auto-paging when ready
  useEffect(() => {
    // Reset positioning flag when switching modes
    if (anchorKind !== 'permalink' && restoreScrollOffset === null) {
      hasPositionedPermalinkRef.current = false;
      if (!autoPagingEnabled) {
        setAutoPagingEnabled(true);
      }
      return;
    }

    if (rowsLength === 0) {
      return;
    }

    // Restore scroll offset when data is complete
    if (restoreScrollOffset !== null && complete) {
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
      const permalinkLogicalIndex = anchorIndex;
      const targetVirtualIndex =
        permalinkLogicalIndex - firstRowIndex + startPlaceholder;

      virtualizer.scrollToIndex(targetVirtualIndex, {
        align: 'start',
      });

      if (complete) {
        hasPositionedPermalinkRef.current = true;
      }
    }

    // Once complete, enable auto-paging
    if (complete && !autoPagingEnabled) {
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
  ]);

  // Auto-shift anchor forward when scrolling near the end of the data window
  useEffect(() => {
    if (!autoPagingEnabled || virtualItems.length === 0 || atEnd) {
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
        setAnchorKind('forward');
        setAnchorIndex(newAnchorIndex);
        setStartRow(toStartRow(newAnchorRow));
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
        setAnchorKind('backward');
        setAnchorIndex(newAnchorIndex);
        setStartRow(toStartRow(newAnchorRow));
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
    }) => {
      setAnchorIndex(state.anchorIndex);
      setAnchorKind(state.anchorKind);
      setPermalinkID(state.permalinkID);
      setStartRow(state.startRow);
      setAutoPagingEnabled(false);
      hasPositionedPermalinkRef.current = false;
      setRestoreScrollOffset(state.scrollOffset ?? null);
    },
    [],
  );

  return {
    virtualizer,
    rowAt: rowAtVirtualIndex,
    rowsEmpty,
    permalinkNotFound,
    anchorState: {
      anchorIndex,
      anchorKind,
      permalinkID,
      startRow,
      scrollOffset: virtualizer.scrollOffset ?? 0,
    },
    restoreAnchorState,
  };
}
