import {useVirtualizer, type Virtualizer} from '@rocicorp/react-virtual';
import {useCallback, useEffect, useMemo, useRef, useState} from 'react';
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

export type ScrollRestorationState<TSort> = {
  anchor: AnchorState<TSort>;
  scrollOffset: number;
};

export interface UseArrayVirtualizerReturn<T, TSort> {
  virtualizer: Virtualizer<HTMLElement, Element>;
  rowAt: (index: number) => T | undefined;
  rowsEmpty: boolean;
  permalinkNotFound: boolean;
  scrollState: ScrollRestorationState<TSort>;
  restoreScrollState: (state: ScrollRestorationState<TSort>) => void;
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

// Delay after positioning before enabling auto-paging.
// Allows virtualItems to update after programmatic scroll.
const POSITIONING_SETTLE_DELAY_MS = 50;

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
          permalinkID: initialPermalinkID,
        }
      : {
          kind: 'forward',
          index: 0,
          startRow: undefined,
        },
  );

  // Counter to force effect re-run when restoring same state
  const [restoreTrigger, setRestoreTrigger] = useState(0);

  const scrollStateRef = useRef({
    pendingScroll: null as number | null,
    pendingScrollIsRelative: false,
    scrollRetryCount: 0,
    positionedAt: 0,
  });

  const replaceAnchor = useCallback(
    (next: AnchorState<TSort>) =>
      setAnchor(prev => (anchorsEqual(prev, next) ? prev : next)),
    [],
  );

  useEffect(() => {
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
    scrollStateRef.current.positionedAt = 0;
    scrollStateRef.current.scrollRetryCount = 0;
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
      const state = scrollStateRef.current;

      if (state.pendingScroll === null || anchor.kind === 'permalink') {
        return false;
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

      if (rowsLength === 0) {
        return true;
      }

      const targetVirtualIndex =
        anchor.index - firstRowIndex + startPlaceholder;

      const state = scrollStateRef.current;

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
          state.positionedAt = Date.now();
          state.pendingScroll = null;
          state.pendingScrollIsRelative = false;
          state.scrollRetryCount = 0;
          return true;
        }

        const maxRetries = 10;
        if (state.scrollRetryCount < maxRetries) {
          state.scrollRetryCount++;
        } else {
          state.positionedAt = Date.now();
          state.pendingScroll = null;
          state.pendingScrollIsRelative = false;
          state.scrollRetryCount = 0;
        }

        return true;
      }

      return false;
    };

    const maybeAutoPage = () => {
      if (virtualItems.length === 0 || !complete) {
        return;
      }

      const state = scrollStateRef.current;

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
    restoreTrigger,
    replaceAnchor,
  ]);

  const restoreAnchorState = useCallback(
    (state: ScrollRestorationState<TSort>) => {
      replaceAnchor(state.anchor);
      scrollStateRef.current.positionedAt = 0;
      scrollStateRef.current.scrollRetryCount = 0;
      scrollStateRef.current.pendingScroll = state.scrollOffset;
      scrollStateRef.current.pendingScrollIsRelative =
        state.anchor.kind === 'permalink';
      // Increment trigger to force re-render even if state values are identical
      setRestoreTrigger(prev => prev + 1);
    },
    [replaceAnchor],
  );

  // Capture current anchor state for external save/restore
  const captureAnchorState = useCallback((): ScrollRestorationState<TSort> => {
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
        anchor,
        scrollOffset: scrollOffset - itemStart,
      };
    }

    return {
      anchor,
      scrollOffset,
    };
  }, [
    anchor,
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
    scrollState: anchorState,
    restoreScrollState: restoreAnchorState,
  };
}
