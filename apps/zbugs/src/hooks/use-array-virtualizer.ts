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

export interface ScrollRestorationState<TSort> {
  anchorIndex: number;
  anchorKind: 'forward' | 'backward' | 'permalink';
  permalinkID?: string | undefined;
  startRow: TSort | undefined;
  scrollOffset: number;
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

// Delay after positioning before enabling auto-paging.
// Allows virtualItems to update after programmatic scroll.
const POSITIONING_SETTLE_DELAY_MS = 500;

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

  // Ref: has the permalink/restore scroll been positioned?
  const positionedRef = useRef(false);

  // Ref: pending scroll offset to restore (set by restoreAnchorState)
  const pendingScrollRef = useRef<number | null>(null);

  // Ref: prevents rapid-fire anchor shifts while waiting for data
  const waitingForDataRef = useRef(false);

  // Ref: timestamp when positioning completed (for settling delay)
  const positionedAtRef = useRef<number>(0);

  useEffect(() => {
    if (initialPermalinkID) {
      setAnchorKind('permalink');
      setAnchorIndex(0);
      setPermalinkID(initialPermalinkID);
      setStartRow(undefined);
      positionedRef.current = false;
      positionedAtRef.current = 0;
    } else {
      setAnchorKind('forward');
      setAnchorIndex(0);
      setPermalinkID(undefined);
      setStartRow(undefined);
      positionedRef.current = false;
      positionedAtRef.current = 0;
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

  // Force remeasurement when estimateSize function changes
  useEffect(() => {
    virtualizer.measure();
  }, [estimateSizeCallback, virtualizer]);

  const virtualItems = virtualizer.getVirtualItems();

  // ---- Single unified effect: positioning + auto-paging ----
  useEffect(() => {
    // Phase 0: Scroll restoration (from restoreAnchorState)
    if (pendingScrollRef.current !== null && anchorKind !== 'permalink') {
      if (complete && rowsLength > 0) {
        virtualizer.scrollToOffset(pendingScrollRef.current);
        pendingScrollRef.current = null;
        positionedRef.current = true;
        positionedAtRef.current = Date.now();
      }
      // Don't auto-page until restoration is done
      return;
    }

    // Phase 1: Permalink positioning
    if (anchorKind === 'permalink') {
      if (rowsLength === 0) {
        return;
      }

      if (!positionedRef.current || !complete) {
        const targetVirtualIndex =
          anchorIndex - firstRowIndex + startPlaceholder;

        virtualizer.scrollToIndex(targetVirtualIndex, {
          align: 'start',
        });

        if (complete) {
          positionedRef.current = true;
          positionedAtRef.current = Date.now();
        }

        // Don't auto-page until positioning is done — virtualItems
        // still reflect the pre-scroll position this render.
        return;
      }

      // Positioned and complete — fall through to Phase 2 for auto-paging.
    }

    // Phase 2: Auto-paging (forward/backward modes only)
    if (virtualItems.length === 0 || !complete) {
      return;
    }

    // Don't auto-page for 500ms after positioning completes.
    // virtualItems take time to update after scrollToIndex(), so we need
    // a settling period to avoid checking boundaries with stale positions.
    if (Date.now() - positionedAtRef.current < POSITIONING_SETTLE_DELAY_MS) {
      return;
    }

    // Clear the waiting flag once data has loaded
    if (waitingForDataRef.current) {
      waitingForDataRef.current = false;
    }

    const viewportHeight = virtualizer.scrollRect?.height ?? 0;
    const thresholdDistance = viewportHeight * 2;

    // Build map of measured sizes from virtualItems
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

    console.log('[AutoPage]', {
      anchorKind,
      anchorIndex,
      firstRowIndex,
      rowsLength,
      positionedRef: positionedRef.current,
      isScrolling: virtualizer.isScrolling,
      viewportHeight,
      thresholdDistance,
      firstVisible: virtualItems[0]?.index,
      lastVisible: virtualItems[virtualItems.length - 1]?.index,
    });

    // Check forward boundary (near end of data window)
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

        // Calculate actual distance: sum measured/estimated sizes of remaining items
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

          if (newAnchorRow && newAnchorIndex !== anchorIndex) {
            waitingForDataRef.current = true;
            setAnchorKind('forward');
            setAnchorIndex(newAnchorIndex);
            setStartRow(toStartRow(newAnchorRow));
            return;
          }
        }
      }
    }

    // Check backward boundary (near start of data window)
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

        // Calculate actual distance: sum measured/estimated sizes of preceding items
        const precedingRowCount = firstLogicalIndex - firstRowIndex;
        let estimatedDistanceToStart = 0;

        for (let i = 0; i < precedingRowCount && i < 20; i++) {
          estimatedDistanceToStart += getSizeForIndex(firstItem.index - i - 1);
        }

        if (precedingRowCount > 20) {
          const avgSize = estimatedDistanceToStart / 20;
          estimatedDistanceToStart += avgSize * (precedingRowCount - 20);
        }

        if (estimatedDistanceToStart < thresholdDistance) {
          const newAnchorIndex = firstLogicalIndex + Math.ceil(pageSize * 0.6);
          const newAnchorRow = rowAt(newAnchorIndex);

          if (newAnchorRow && newAnchorIndex !== anchorIndex) {
            waitingForDataRef.current = true;
            setAnchorKind('backward');
            setAnchorIndex(newAnchorIndex);
            setStartRow(toStartRow(newAnchorRow));
            return;
          }
        }
      }
    }
  }, [
    anchorKind,
    anchorIndex,
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
      setAnchorIndex(state.anchorIndex);
      setAnchorKind(state.anchorKind);
      setPermalinkID(state.permalinkID);
      setStartRow(state.startRow);
      positionedRef.current = false;
      positionedAtRef.current = 0;
      pendingScrollRef.current = state.scrollOffset ?? null;
    },
    [],
  );

  // Capture current anchor state for external save/restore
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
        return {
          anchorIndex,
          anchorKind,
          permalinkID,
          startRow,
          scrollOffset,
        };
      }

      const firstVisibleItemID =
        typeof firstVisibleRow === 'object' &&
        firstVisibleRow !== null &&
        'id' in firstVisibleRow
          ? String(firstVisibleRow.id)
          : undefined;

      if (!firstVisibleItemID) {
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
