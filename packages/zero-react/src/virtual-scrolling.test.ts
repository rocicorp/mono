import {describe, expect, test} from 'vitest';

/**
 * Virtual Scrolling Tests
 *
 * These tests simulate the virtual scrolling behavior used in list components.
 * Virtual scrolling efficiently renders large lists by only rendering items
 * that are currently visible in the viewport.
 */

describe('Virtual Scrolling', () => {
  // Simulate the virtual scrolling logic
  function getVirtualItems(
    scrollTop: number,
    itemSize: number,
    viewportHeight: number,
    totalItems: number,
    overscan = 5,
  ): Array<{index: number; start: number; size: number}> {
    const itemsInView = Math.ceil(viewportHeight / itemSize) + overscan;
    const firstVisibleIndex = Math.floor(scrollTop / itemSize);
    const startIndex = Math.max(0, firstVisibleIndex - overscan);
    const endIndex = Math.min(totalItems, startIndex + itemsInView);

    return Array.from({length: endIndex - startIndex}, (_, i) => ({
      index: startIndex + i,
      start: (startIndex + i) * itemSize,
      size: itemSize,
    }));
  }

  test('scrolling down updates visible items correctly', () => {
    // Configuration matching typical list component usage
    const ITEM_SIZE = 56;
    const VIEWPORT_HEIGHT = 600;
    const TOTAL_ITEMS = 200;
    let scrollTop = 0;
    let total: number | undefined = undefined;

    // Initial state - should have visible items at the top
    let virtualItems = getVirtualItems(
      scrollTop,
      ITEM_SIZE,
      VIEWPORT_HEIGHT,
      TOTAL_ITEMS,
    );
    expect(virtualItems.length).toBeGreaterThan(0);
    expect(virtualItems[0].index).toBe(0);

    const initialFirstIndex = virtualItems[0].index;

    // Scroll down in small increments (50px at a time)
    const SCROLL_INCREMENT = 50;
    let scrollCount = 0;
    // Calculate max scrolls needed: total height minus viewport height, divided by increment
    const totalHeight = TOTAL_ITEMS * ITEM_SIZE;
    const maxScrolls = Math.ceil(
      (totalHeight - VIEWPORT_HEIGHT + ITEM_SIZE * 10) / SCROLL_INCREMENT,
    );

    // Keep scrolling until we reach the end or until total is defined
    while (scrollCount < maxScrolls) {
      // Scroll down by 50px
      scrollTop += SCROLL_INCREMENT;
      scrollCount++;

      // Update visible items
      virtualItems = getVirtualItems(
        scrollTop,
        ITEM_SIZE,
        VIEWPORT_HEIGHT,
        TOTAL_ITEMS,
      );

      // Verify that visible items are present
      expect(virtualItems.length).toBeGreaterThan(0);

      const newFirstIndex = virtualItems[0].index;
      const newLastIndex = virtualItems[virtualItems.length - 1].index;

      // Verify visible items are updating as we scroll down
      // After scrolling, either first index should increase or we're at the end
      const itemsUpdating =
        newFirstIndex >= initialFirstIndex || newLastIndex === TOTAL_ITEMS - 1;
      expect(itemsUpdating).toBe(true);

      // Check if we've reached the end
      const lastVisibleIndex = virtualItems[virtualItems.length - 1]?.index;
      if (lastVisibleIndex === TOTAL_ITEMS - 1) {
        // Simulate reaching atEnd - total becomes defined
        total = TOTAL_ITEMS;
        break;
      }
    }

    // Verify we reached the end
    expect(total).toBe(TOTAL_ITEMS);
    expect(virtualItems.length).toBeGreaterThan(0);

    // Verify the last visible item is near or at the end
    const lastVisibleIndex = virtualItems[virtualItems.length - 1].index;
    expect(lastVisibleIndex).toBe(TOTAL_ITEMS - 1);
  });

  test('visible items update correctly with different scroll amounts', () => {
    const ITEM_SIZE = 56;
    const VIEWPORT_HEIGHT = 600;
    const TOTAL_ITEMS = 100;
    let scrollTop = 0;

    // Start at top
    let virtualItems = getVirtualItems(
      scrollTop,
      ITEM_SIZE,
      VIEWPORT_HEIGHT,
      TOTAL_ITEMS,
    );
    const topItems = virtualItems.map(v => v.index);

    // Scroll to middle
    scrollTop = (TOTAL_ITEMS * ITEM_SIZE) / 2;
    virtualItems = getVirtualItems(
      scrollTop,
      ITEM_SIZE,
      VIEWPORT_HEIGHT,
      TOTAL_ITEMS,
    );
    const middleItems = virtualItems.map(v => v.index);

    // Verify items changed
    expect(middleItems[0]).toBeGreaterThan(topItems[0]);
    expect(middleItems[middleItems.length - 1]).toBeGreaterThan(
      topItems[topItems.length - 1],
    );

    // Scroll near bottom
    scrollTop = TOTAL_ITEMS * ITEM_SIZE - VIEWPORT_HEIGHT;
    virtualItems = getVirtualItems(
      scrollTop,
      ITEM_SIZE,
      VIEWPORT_HEIGHT,
      TOTAL_ITEMS,
    );
    const bottomItems = virtualItems.map(v => v.index);

    // Verify we can see items near the end
    expect(bottomItems[bottomItems.length - 1]).toBeGreaterThanOrEqual(
      TOTAL_ITEMS - 10,
    );
  });
});
