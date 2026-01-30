import {useCallback, useEffect, useLayoutEffect, useRef, type Key} from 'react';

/**
 * Interface for virtualizer methods used by this hook.
 * Compatible with @tanstack/react-virtual.
 *
 * Note: If using @rocicorp/react-virtual, this hook is NOT needed
 * as stability is built into the virtualizer itself.
 *
 * @deprecated When using @rocicorp/react-virtual, use virtualizer.measureElement directly.
 */
interface VirtualizerLike<TItemElement extends Element> {
  getVirtualItems(): ReadonlyArray<{
    index: number;
    key: Key;
    start: number;
  }>;
  measureElement(node: TItemElement | null): void;
  scrollOffset: number | null | undefined;
  options: {
    getItemKey?: ((index: number) => Key) | undefined;
  };
  // Internal properties needed for stability - may not exist on all virtualizers
  shouldAdjustScrollPositionOnItemSizeChange?: unknown;
  elementsCache?: Map<Key, TItemElement>;
  measurementsCache?: ReadonlyArray<{key: Key; start: number}>;
  scrollToOffset?(offset: number): void;
}

/**
 * Snapshot of scroll position anchored to a specific item.
 * Used to restore scroll position after mutations that would shift content.
 */
type ScrollAnchor = {
  /** Unique key identifying the anchor item */
  key: Key;
  /** Index of the anchor item at the time of capture */
  index: number;
  /** Pixel offset from viewport top to anchor item's top edge */
  pixelOffset: number;
  /** Index where the splice/mutation occurred */
  spliceIndex: number;
  /** Number of corrections applied so far */
  correctionCount: number;
};

export type UseStableScrollOptions<
  TScrollElement extends HTMLElement,
  TItemElement extends Element,
> = {
  /** The Tanstack virtualizer instance (or compatible fork) */
  virtualizer: VirtualizerLike<TItemElement>;
  /** Function that returns the scroll container element */
  getScrollElement: () => TScrollElement | null;
  /** Whether to enable debug logging */
  debug?: boolean;
};

/**
 * Function that captures the current scroll anchor before a mutation.
 * Call this BEFORE updating the data that the virtualizer renders.
 *
 * @param spliceIndex - The index where items will be inserted/deleted
 * @returns A function to clear the anchor without restoring (abandon pending restoration)
 */
export type CaptureAnchor = (spliceIndex: number) => () => void;

/**
 * Result object from useStableScroll hook.
 */
export type UseStableScrollResult<TItemElement extends Element> = {
  /**
   * Captures the current scroll anchor before a mutation.
   * Call this BEFORE updating the data that the virtualizer renders.
   *
   * @param spliceIndex - The index where items will be inserted/deleted
   * @returns A function to clear the anchor without restoring
   */
  captureAnchor: CaptureAnchor;

  /**
   * Wrapped measureElement that handles resize-above-viewport adjustments.
   * Use this as the ref callback for your virtualized items.
   */
  measureElement: (node: TItemElement | null) => void;
};

/**
 * Hook that provides scroll position stability across data mutations and resizes.
 *
 * When items are inserted or deleted above the viewport, the visible content
 * would normally shift. This hook captures an "anchor" item before mutations
 * and automatically restores its position after React renders.
 *
 * It also handles resize-above-viewport: when an item above the viewport
 * changes size, the scroll position is adjusted to keep visible content stable.
 *
 * Usage:
 * 1. Use `measureElement` as the ref callback for your virtualized items
 * 2. Call `captureAnchor(spliceIndex)` BEFORE updating your data
 * 3. Update your data (setState, etc.)
 * 4. The hook automatically restores scroll position in useLayoutEffect/useEffect
 *
 * @example
 * ```tsx
 * const {captureAnchor, measureElement} = useStableScroll({
 *   virtualizer,
 *   getScrollElement,
 * });
 *
 * const handleInsert = (index: number, items: Item[]) => {
 *   captureAnchor(index);
 *   setData(prev => {
 *     const next = [...prev];
 *     next.splice(index, 0, ...items);
 *     return next;
 *   });
 * };
 *
 * // In render:
 * <div ref={measureElement} data-key={row.id}>...</div>
 * ```
 */
export function useStableScroll<
  TScrollElement extends HTMLElement,
  TItemElement extends Element,
>({
  virtualizer,
  getScrollElement,
  debug = false,
}: UseStableScrollOptions<
  TScrollElement,
  TItemElement
>): UseStableScrollResult<TItemElement> {
  const anchorRef = useRef<ScrollAnchor | null>(null);

  // Size cache for resize-above-viewport handling: data-key -> measured height
  const sizeCacheRef = useRef(new Map<string, number>());

  // Track elements we're observing for resize
  const resizeObserverRef = useRef<ResizeObserver | null>(null);
  const observedElementsRef = useRef(new Map<Element, string>());

  // Disable Tanstack's automatic scroll adjustments - we handle everything ourselves
  // This property exists on @tanstack/react-virtual but may not on our fork
  if ('shouldAdjustScrollPositionOnItemSizeChange' in virtualizer) {
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    (virtualizer as any).shouldAdjustScrollPositionOnItemSizeChange = () =>
      false;
  }

  const log = useCallback(
    (message: string) => {
      if (debug) {
        // oxlint-disable-next-line no-console
        console.log(`[STABLE_SCROLL] ${message}`);
      }
    },
    [debug],
  );

  // Handle resize observations - adjust scroll when items above viewport resize
  const handleResize = useCallback(
    (entries: ResizeObserverEntry[]) => {
      const scrollElement = getScrollElement();
      if (!scrollElement) {
        return;
      }

      const scrollRect = scrollElement.getBoundingClientRect();

      for (const entry of entries) {
        const node = entry.target;
        const key = observedElementsRef.current.get(node);
        if (!key) {
          continue;
        }

        const oldSize = sizeCacheRef.current.get(key);
        const newSize = entry.contentRect.height;

        // Only process if we have a previous size and it changed
        if (oldSize === undefined || oldSize === newSize) {
          // Update cache even for first observation
          if (oldSize === undefined) {
            sizeCacheRef.current.set(key, newSize);
          }
          continue;
        }

        // Get position of this element relative to viewport
        const nodeRect = node.getBoundingClientRect();
        const itemTopRelativeToViewport = nodeRect.top - scrollRect.top;

        // If item's BOTTOM is above the viewport top (item is entirely above),
        // OR if item's TOP is above viewport top (item is partially above),
        // we need to adjust scroll.
        // Using bottom < 0 would only catch fully-above items.
        // Using top < 0 catches both partially and fully above items.
        // For scroll stability, we want to adjust when item's top edge is above viewport.
        if (itemTopRelativeToViewport < 0) {
          const delta = newSize - oldSize;
          log(
            `Resize above viewport: key=${key}, oldSize=${oldSize}, newSize=${newSize}, delta=${delta}`,
          );
          scrollElement.scrollTop = scrollElement.scrollTop + delta;
        }

        // Update cache
        sizeCacheRef.current.set(key, newSize);
      }
    },
    [getScrollElement, log],
  );

  // Initialize ResizeObserver
  useEffect(() => {
    resizeObserverRef.current = new ResizeObserver(handleResize);
    return () => {
      resizeObserverRef.current?.disconnect();
      resizeObserverRef.current = null;
      observedElementsRef.current.clear();
    };
  }, [handleResize]);

  const clearAnchor = useCallback(() => {
    anchorRef.current = null;
  }, []);

  const captureAnchor = useCallback(
    (spliceIndex: number): (() => void) => {
      const scrollElement = getScrollElement();
      if (!scrollElement) {
        return clearAnchor;
      }

      const scrollOffset = virtualizer.scrollOffset ?? 0;
      const virtualItems = virtualizer.getVirtualItems();

      // Find the first item whose start is >= scrollOffset (first fully/partially visible)
      let anchorItem = virtualItems.find(item => item.start >= scrollOffset);
      if (!anchorItem && virtualItems.length > 0) {
        anchorItem = virtualItems[0];
      }

      if (!anchorItem) {
        return clearAnchor;
      }

      const {key} = anchorItem;

      // Use Tanstack's elementsCache for O(1) element lookup (if available)
      const anchorElement = virtualizer.elementsCache?.get(key) as
        | Element
        | undefined;

      let pixelOffset: number;
      if (anchorElement?.isConnected) {
        // Use actual DOM position to avoid sub-pixel drift
        const anchorRect = anchorElement.getBoundingClientRect();
        const scrollRect = scrollElement.getBoundingClientRect();
        pixelOffset = anchorRect.top - scrollRect.top;
      } else {
        // Fallback to Tanstack's computed position
        pixelOffset = anchorItem.start - (scrollOffset ?? 0);
      }

      const anchor: ScrollAnchor = {
        key,
        index: anchorItem.index,
        pixelOffset,
        spliceIndex,
        correctionCount: 0,
      };

      anchorRef.current = anchor;

      log(
        `Captured: key=${key}, index=${anchorItem.index}, pixelOffset=${pixelOffset.toFixed(1)}, spliceIndex=${spliceIndex}`,
      );

      return clearAnchor;
    },
    [virtualizer, getScrollElement, log, clearAnchor],
  );

  // Core correction logic - returns true if a correction was made
  const correctScrollPosition = useCallback((): boolean => {
    const anchor = anchorRef.current;
    if (!anchor) {
      return false;
    }

    const scrollElement = getScrollElement();
    if (!scrollElement) {
      return false;
    }

    // Try DOM-based correction first (more accurate)
    const anchorElement = virtualizer.elementsCache?.get(anchor.key) as
      | Element
      | undefined;

    if (anchorElement?.isConnected) {
      const anchorRect = anchorElement.getBoundingClientRect();
      const scrollRect = scrollElement.getBoundingClientRect();
      const actualPixelOffset = anchorRect.top - scrollRect.top;
      const currentScroll = scrollElement.scrollTop;

      const error = actualPixelOffset - anchor.pixelOffset;

      if (Math.abs(error) > 1) {
        const targetScroll = currentScroll + error;
        log(
          `DOM correction #${anchor.correctionCount + 1}: error=${error.toFixed(1)}, scroll ${currentScroll.toFixed(1)} -> ${targetScroll.toFixed(1)}`,
        );
        scrollElement.scrollTop = targetScroll;
        anchor.correctionCount++;
        return true;
      }

      log(`Stable after ${anchor.correctionCount} corrections`);
      return false;
    }

    // Fallback: use Tanstack's measurementsCache if element not in DOM
    const measurements = virtualizer.measurementsCache;
    if (!measurements) {
      return false;
    }
    const anchorMeasurement = measurements.find(m => m.key === anchor.key);

    if (anchorMeasurement) {
      const newStart = anchorMeasurement.start;
      const targetScroll = newStart - anchor.pixelOffset;
      const currentScroll = virtualizer.scrollOffset ?? 0;
      const error = targetScroll - currentScroll;

      if (Math.abs(error) > 1) {
        log(
          `Cache correction #${anchor.correctionCount + 1}: error=${error.toFixed(1)}, scroll ${currentScroll.toFixed(1)} -> ${targetScroll.toFixed(1)}`,
        );
        virtualizer.scrollToOffset?.(targetScroll);
        anchor.correctionCount++;
        return true;
      }
    }

    return false;
  }, [virtualizer, getScrollElement, log]);

  // Initial correction - runs after React renders but before paint
  useLayoutEffect(() => {
    const anchor = anchorRef.current;
    if (!anchor) {
      return;
    }

    // If splice happened AFTER anchor, no adjustment needed
    if (anchor.spliceIndex > anchor.index) {
      log(
        `Splice after anchor (${anchor.spliceIndex} > ${anchor.index}), skipping`,
      );
      anchorRef.current = null;
      return;
    }

    // Do initial correction
    correctScrollPosition();
  });

  // Follow-up corrections after paint and ResizeObserver fires
  useEffect(() => {
    const anchor = anchorRef.current;
    if (!anchor) {
      return;
    }

    // Skip if splice was after anchor
    if (anchor.spliceIndex > anchor.index) {
      anchorRef.current = null;
      return;
    }

    let rafId: number;
    let attempts = 0;
    const maxAttempts = 10;

    const attemptCorrection = () => {
      attempts++;
      const madeCorrection = correctScrollPosition();

      if (madeCorrection && attempts < maxAttempts) {
        // Wait for next frame in case ResizeObserver fires again
        rafId = requestAnimationFrame(attemptCorrection);
      } else {
        log(`Complete after ${anchor.correctionCount} total corrections`);
        anchorRef.current = null;
      }
    };

    // Start on next frame to let ResizeObserver fire
    rafId = requestAnimationFrame(attemptCorrection);

    return () => {
      cancelAnimationFrame(rafId);
    };
  });

  // Wrapped measureElement that observes elements for resize
  const measureElement = useCallback(
    (node: TItemElement | null) => {
      if (!node) {
        virtualizer.measureElement(node);
        return;
      }

      const key = node.getAttribute('data-key');
      if (!key) {
        virtualizer.measureElement(node);
        return;
      }

      // Observe this element for resize
      if (resizeObserverRef.current && !observedElementsRef.current.has(node)) {
        observedElementsRef.current.set(node, key);
        resizeObserverRef.current.observe(node);
      }

      // Let Tanstack measure the element
      virtualizer.measureElement(node);

      // Initialize size cache if not already set
      if (!sizeCacheRef.current.has(key)) {
        const rect = node.getBoundingClientRect();
        sizeCacheRef.current.set(key, rect.height);
      }
    },
    [virtualizer],
  );

  return {captureAnchor, measureElement};
}
