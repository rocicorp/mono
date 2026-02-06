import {useVirtualizer, type Virtualizer} from '@rocicorp/react-virtual';
import {useCallback, useEffect, useMemo, useRef, useState} from 'react';
import {
  useRows,
  type Anchor,
  type GetPageQuery,
  type GetSingleQuery,
} from '../../../../packages/zero-react/src/use-rows.js';

export interface UseArrayVirtualizerOptions<T, TSort, TListContextParams> {
  getScrollElement: () => HTMLElement | null;

  debug?: boolean | undefined;
  overscan?: number | undefined;

  pageSize: number;
  placeholderHeight: number;
  estimateRowSize: (row: T | undefined, index: number) => number;
  listContextParams: TListContextParams;
  getPageQuery: GetPageQuery<T, TSort>;
  getSingleQuery: GetSingleQuery<T>;
  toStartRow: (row: T) => TSort;
  permalinkID?: string | undefined | null;
  scrollState?: ScrollRestorationState<TSort> | null | undefined;
  onScrollStateChange?: (state: ScrollRestorationState<TSort>) => void;
  /** Parameters that define the list's query context (filters, sort order, etc.) */
}

export type ScrollRestorationState<TSort> = {
  anchor: AnchorState<TSort>;
  scrollOffset: number;
};

export interface UseArrayVirtualizerReturn<T> {
  virtualizer: Virtualizer<HTMLElement, Element>;
  rowAt: (index: number) => T | undefined;
  rowsEmpty: boolean;
  permalinkNotFound: boolean;
}

type AnchorState<TSort> = Anchor<TSort>;

const anchorsEqual = <TSort>(a: AnchorState<TSort>, b: AnchorState<TSort>) => {
  if (a.index !== b.index) {
    return false;
  }

  if (a.kind === 'permalink' && b.kind === 'permalink') {
    return a.id === b.id;
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

export function useArrayVirtualizer<T, TSort, TListContextParams>({
  listContextParams,
  pageSize,
  placeholderHeight,
  estimateRowSize: estimateSizeCallback,
  getScrollElement,
  getPageQuery,
  getSingleQuery,
  toStartRow,
  permalinkID,
  scrollState,
  onScrollStateChange,
  debug = false,
  overscan = 5,
}: UseArrayVirtualizerOptions<
  T,
  TSort,
  TListContextParams
>): UseArrayVirtualizerReturn<T> {
  const [anchor, setAnchor] = useState<AnchorState<TSort>>(() =>
    scrollState
      ? scrollState.anchor
      : permalinkID
        ? {
            kind: 'permalink',
            index: 0,
            id: permalinkID,
          }
        : {
            kind: 'forward',
            index: 0,
            startRow: undefined,
          },
  );

  // Counter to force effect re-run when restoring same state
  // const [restoreTrigger, setRestoreTrigger] = useState(0);

  const scrollStateRef = useRef({
    pendingScroll: scrollState?.scrollOffset ?? null,
    pendingScrollIsRelative: scrollState?.anchor.kind === 'permalink',
    scrollRetryCount: 0,
    positionedAt: 0,
    listContext: listContextParams,
  });

  const isListContextCurrent =
    scrollStateRef.current.listContext === listContextParams;

  console.log('useArrayVirtualizer: render', {
    permalinkID,
    anchor,
    scrollState,
    'scrollStateRef.current': scrollStateRef.current,
    isListContextCurrent,
  });

  const replaceAnchor = useCallback(
    (next: AnchorState<TSort>) =>
      setAnchor(prev => (anchorsEqual(prev, next) ? prev : next)),
    [],
  );

  useEffect(() => {
    // if (!isListContextCurrent) {
    //   console.log(
    //     'List context changed, resetting anchor to initial state',
    //     listContextParams,
    //   );
    //   return;
    // }
    const nextAnchor: AnchorState<TSort> = scrollState
      ? scrollState.anchor
      : permalinkID
        ? {
            kind: 'permalink',
            index: 0,
            id: permalinkID,
          }
        : {
            kind: 'forward',
            index: 0,
            startRow: undefined,
          };

    replaceAnchor(nextAnchor);
    scrollStateRef.current.positionedAt = 0;
    scrollStateRef.current.scrollRetryCount = 0;
  }, [permalinkID, replaceAnchor]);

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
          id: anchor.id,
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

  useEffect(() => {
    console.log('HERE', {
      permalinkID,
      scrollState,
      isListContextCurrent,
    });
  }, [permalinkID, scrollState, isListContextCurrent]);

  // Force remeasurement when estimateSize function changes
  useEffect(() => {
    if (isListContextCurrent) {
      virtualizer.measure();
    }
  }, [estimateSizeCallback, virtualizer, isListContextCurrent]);

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

    console.log('useArrayVirtualizer: effect', {
      anchor,
      rowsLength,
      complete,
      startPlaceholder,
      firstRowIndex,
      virtualItems,
      atEnd,
      atStart,
      rowAtVirtualIndex,
      rowAt,
      toLogicalIndex,
      pageSize,
      toStartRow,
      replaceAnchor,
      isListContextCurrent,
      scrollStateRef,
      scrollState,
    });

    if (!isListContextCurrent) {
      console.log(
        'List context not current, need to restore position after context updates',
      );

      return;
    }

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
    isListContextCurrent,
  ]);

  // Capture current scroll state
  const captureCurrentState = useCallback((): ScrollRestorationState<TSort> => {
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

  useEffect(() => {
    if (isListContextCurrent && onScrollStateChange) {
      const id = setTimeout(() => {
        const state = captureCurrentState();
        onScrollStateChange(state);
      }, 100);

      return () => clearTimeout(id);
    }
    return;
  }, [
    captureCurrentState,
    onScrollStateChange,
    isListContextCurrent,
    complete,
    scrollStateRef.current.scrollRetryCount,
  ]);

  return {
    virtualizer,
    rowAt: rowAtVirtualIndex,
    rowsEmpty,
    permalinkNotFound,
  };
}
