import { useVirtualizer, type Virtualizer } from '@rocicorp/react-virtual';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
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

type AnchorKind = 'forward' | 'backward' | 'permalink';

type AnchorState<TSort> = {
  kind: AnchorKind;
  index: number;
  startRow: TSort | undefined;
  permalinkID?: string | undefined;
};

const anchorsEqual = <TSort>(a: AnchorState<TSort>, b: AnchorState<TSort>) =>
  a.kind === b.kind &&
  a.index === b.index &&
  a.permalinkID === b.permalinkID &&
  a.startRow === b.startRow;

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
  const [anchor, setAnchor] = useState<AnchorState<TSort>>(() =>
    initialPermalinkID
      ? {
          kind: 'permalink',
          index: 0,
          startRow: undefined,
          permalinkID: initialPermalinkID,
        }
      : {
          kind: 'forward',
          index: 0,
          startRow: undefined,
          permalinkID: undefined,
        },
  );

  // Counter to force effect re-run when restoring same state
  const [restoreTrigger, setRestoreTrigger] = useState(0);

  // Ref: has the permalink/restore scroll been positioned?
  const positionedRef = useRef(false);

  // Ref: pending scroll offset to restore (set by restoreAnchorState)
  const pendingScrollRef = useRef<number | null>(null);
  const pendingScrollIsRelativeRef = useRef(false);

  // Ref: prevents rapid-fire anchor shifts while waiting for data
  const waitingForDataRef = useRef(false);

  // Ref: timestamp when positioning completed (for settling delay)
  const positionedAtRef = useRef<number>(0);

  // Ref: retry count for custom scroll loop
  const scrollRetryCountRef = useRef(0);

  const replaceAnchor = useCallback(
    (next: AnchorState<TSort>) =>
      setAnchor(prev => (anchorsEqual(prev, next) ? prev : next)),
    [],
  );

  useEffect(() => {
    const nextAnchor = initialPermalinkID
      ? {
          kind: 'permalink' as AnchorKind,
          index: 0,
          startRow: undefined,
          permalinkID: initialPermalinkID,
        }
      : {
          kind: 'forward' as AnchorKind,
          index: 0,
          startRow: undefined,
          permalinkID: undefined,
        };

    replaceAnchor(nextAnchor);
    positionedRef.current = false;
    positionedAtRef.current = 0;
    scrollRetryCountRef.current = 0;
  }, [initialPermalinkID, replaceAnchor]);

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
      if (anchor.kind === 'permalink' && anchor.permalinkID) {
        return {
          kind: 'permalink' as const,
          index: anchor.index,
          id: anchor.permalinkID,
        };
      }

      if (anchor.kind === 'forward') {
        return {
          kind: 'forward' as const,
          index: anchor.index,
          startRow: anchor.startRow,
        };
      }

      if (anchor.kind === 'backward' && anchor.startRow) {
        return {
          kind: 'backward' as const,
          index: anchor.index,
          startRow: anchor.startRow,
        };
      }

      return {
        kind: 'forward' as const,
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

  const virtualItems = virtualizer.getVirtualItems();

  // ---- Single unified effect: positioning + auto-paging ----
  useEffect(() => {
    const restoreScrollIfNeeded = () => {
      if (pendingScrollRef.current === null || anchor.kind === 'permalink') {
        return false;
      }

      if (complete && rowsLength > 0) {
        virtualizer.scrollToOffset(pendingScrollRef.current);
        pendingScrollRef.current = null;
        positionedRef.current = true;
        positionedAtRef.current = Date.now();
      }

      return true;
    };

    const positionPermalinkIfNeeded = () => {
      if (anchor.kind !== 'permalink') {
        return false;
      }

      if (rowsLength === 0) {
        return true;
      }

      const targetVirtualIndex =
        anchor.index - firstRowIndex + startPlaceholder;

      if (!positionedRef.current || !complete) {
        if (pendingScrollRef.current === null) {
          virtualizer.scrollToIndex(targetVirtualIndex, {
            align: 'start',
          });

          if (complete) {
            positionedRef.current = true;
            positionedAtRef.current = Date.now();
          }

          return true;
        }

        const baseOffset = virtualizer.getOffsetForIndex(
          targetVirtualIndex,
          'start',
        );

        if (pendingScrollIsRelativeRef.current && !baseOffset) {
          scrollRetryCountRef.current = 0;
          return true;
        }

        const targetOffset = pendingScrollIsRelativeRef.current
          ? baseOffset![0] + pendingScrollRef.current
          : pendingScrollRef.current;
        virtualizer.scrollToOffset(targetOffset);

        const currentScrollOffset = virtualizer.scrollOffset ?? 0;

        if (Math.abs(currentScrollOffset - targetOffset) <= 1) {
          positionedRef.current = true;
          positionedAtRef.current = Date.now();
          pendingScrollRef.current = null;
          pendingScrollIsRelativeRef.current = false;
          scrollRetryCountRef.current = 0;
          return true;
        }

        const maxRetries = 10;
        if (scrollRetryCountRef.current < maxRetries) {
          scrollRetryCountRef.current++;
        } else {
          positionedRef.current = true;
          positionedAtRef.current = Date.now();
          pendingScrollRef.current = null;
          pendingScrollIsRelativeRef.current = false;
          scrollRetryCountRef.current = 0;
        }

        return true;
      }

      return false;
    };

    const maybeAutoPage = () => {
      if (virtualItems.length === 0 || !complete) {
        return;
      }

      if (Date.now() - positionedAtRef.current < POSITIONING_SETTLE_DELAY_MS) {
        return;
      }

      if (waitingForDataRef.current) {
        waitingForDataRef.current = false;
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
              waitingForDataRef.current = true;
              replaceAnchor({
                kind: 'forward',
                index: newAnchorIndex,
                startRow: toStartRow(newAnchorRow),
                permalinkID: anchor.permalinkID,
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
              waitingForDataRef.current = true;
              replaceAnchor({
                kind: 'backward',
                index: newAnchorIndex,
                startRow: toStartRow(newAnchorRow),
                permalinkID: anchor.permalinkID,
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
    restoreTrigger,
    replaceAnchor,
  ]);

  const restoreAnchorState = useCallback(
    (state: {
      anchorIndex: number;
      anchorKind: 'forward' | 'backward' | 'permalink';
      permalinkID?: string | undefined;
      startRow: TSort | undefined;
      scrollOffset?: number;
    }) => {
      replaceAnchor({
        kind: state.anchorKind,
        index: state.anchorIndex,
        startRow: state.startRow,
        permalinkID: state.permalinkID,
      });
      positionedRef.current = false;
      positionedAtRef.current = 0;
      scrollRetryCountRef.current = 0;
      pendingScrollRef.current = state.scrollOffset ?? null;
      pendingScrollIsRelativeRef.current = state.anchorKind === 'permalink';
      // Increment trigger to force re-render even if state values are identical
      setRestoreTrigger(prev => prev + 1);
    },
    [replaceAnchor],
  );

  // Capture current anchor state for external save/restore
  const captureAnchorState = useCallback(() => {
    const scrollOffset = virtualizer.scrollOffset ?? 0;

    if (anchor.kind === 'permalink') {
      const targetVirtualIndex =
        anchor.index - firstRowIndex + startPlaceholder;
      const offsetInfo = virtualizer.getOffsetForIndex(
        targetVirtualIndex,
        'start',
      );
      const itemStart = offsetInfo ? offsetInfo[0] : scrollOffset;

      return {
        anchorIndex: anchor.index,
        anchorKind: anchor.kind,
        permalinkID: anchor.permalinkID,
        startRow: anchor.startRow,
        scrollOffset: scrollOffset - itemStart,
      };
    }

    return {
      anchorIndex: anchor.index,
      anchorKind: anchor.kind,
      permalinkID: anchor.permalinkID,
      startRow: anchor.startRow,
      scrollOffset,
    };
  }, [
    anchor.index,
    anchor.kind,
    anchor.permalinkID,
    anchor.startRow,
    firstRowIndex,
    startPlaceholder,
    virtualizer,
    virtualizer.scrollOffset,
  ]);

  const anchorState = captureAnchorState();

  return {
    virtualizer,
    rowAt: rowAtVirtualIndex,
    rowsEmpty,
    permalinkNotFound,
    anchorState,
    restoreAnchorState,
  };
}
