import {expect, test, type Page} from '@playwright/test';

const BASE_URL = process.env.URL ?? 'http://localhost:5173';

// Tolerance in pixels for position comparisons.
const POSITION_TOLERANCE = 2;

// Wait for the initial data to load and virtual items to render.
async function waitForRows(page: Page) {
  await page.waitForSelector('[data-row-id]', {timeout: 30000});
}

type VisibleRow = {
  rowId: string;
  index: number;
  // Position of the row's top edge relative to the scroll container's viewport top.
  viewportRelativeTop: number;
};

// Get all rows that are fully or partially visible inside the scroll viewport.
// Returns them sorted by their position (top to bottom).
function getVisibleRows(page: Page): Promise<VisibleRow[]> {
  return page.evaluate(() => {
    // The scroll container has overflow: auto and position: relative.
    const container = document.querySelector(
      'div[style*="overflow: auto"][style*="position: relative"]',
    ) as HTMLElement | null;
    if (!container) {
      return [];
    }
    const viewportHeight = container.clientHeight;

    const rows: VisibleRow[] = [];
    const elements = container.querySelectorAll('[data-row-id]');
    for (const el of elements) {
      const rect = el.getBoundingClientRect();
      const containerRect = container.getBoundingClientRect();
      const relativeTop = rect.top - containerRect.top;
      const relativeBottom = rect.bottom - containerRect.top;

      // Include rows that overlap the viewport.
      if (relativeBottom > 0 && relativeTop < viewportHeight) {
        rows.push({
          rowId: el.getAttribute('data-row-id')!,
          index: parseInt(el.getAttribute('data-index')!, 10),
          viewportRelativeTop: relativeTop,
        });
      }
    }

    rows.sort((a, b) => a.viewportRelativeTop - b.viewportRelativeTop);
    return rows;
  });
}

// Wait for scroll position to stabilize (no change for duration ms).
async function waitForScrollStable(page: Page, duration = 300) {
  await page.evaluate(async dur => {
    const container = document.querySelector(
      'div[style*="overflow: auto"][style*="position: relative"]',
    ) as HTMLElement | null;
    if (!container) {
      return;
    }

    let lastTop = container.scrollTop;
    let stableTime = 0;
    const interval = 50;

    await new Promise<void>(resolve => {
      const timer = setInterval(() => {
        const current = container.scrollTop;
        if (Math.abs(current - lastTop) < 1) {
          stableTime += interval;
          if (stableTime >= dur) {
            clearInterval(timer);
            resolve();
          }
        } else {
          stableTime = 0;
          lastTop = current;
        }
      }, interval);
    });
  }, duration);
}

// Scroll the container to a specific scrollTop value.
async function scrollTo(page: Page, offset: number) {
  await page.evaluate(scrollTop => {
    const container = document.querySelector(
      'div[style*="overflow: auto"][style*="position: relative"]',
    ) as HTMLElement | null;
    if (container) {
      container.scrollTop = scrollTop;
    }
  }, offset);
}

// Get the current scrollTop.
function getScrollTop(page: Page): Promise<number> {
  return page.evaluate(() => {
    const container = document.querySelector(
      'div[style*="overflow: auto"][style*="position: relative"]',
    ) as HTMLElement | null;
    return container ? container.scrollTop : 0;
  });
}

test.describe('Scroll Restoration', () => {
  test('capture and restore preserves visible items and positions', async ({
    page,
  }) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Scroll down to get a non-trivial position with data loaded.
    await scrollTo(page, 3000);
    await waitForScrollStable(page, 500);
    // Wait for any auto-paging and re-rendering to settle.
    await page.waitForTimeout(500);

    // Record visible rows before capture.
    const beforeRows = await getVisibleRows(page);
    expect(beforeRows.length).toBeGreaterThan(0);

    // Capture the scroll state.
    await page.click('[data-testid="capture-btn"]');
    const capturedText = await page.inputValue('[data-testid="restore-input"]');
    expect(capturedText).toBeTruthy();

    // Verify the captured state has the expected structure.
    const capturedState = JSON.parse(capturedText!);
    expect(capturedState).toHaveProperty('permalinkID');
    expect(capturedState).toHaveProperty('index');
    expect(capturedState).toHaveProperty('scrollOffset');
    expect(typeof capturedState.permalinkID).toBe('string');
    expect(capturedState.permalinkID.length).toBeGreaterThan(0);

    // Scroll far away to a completely different position.
    await scrollTo(page, 0);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);

    // Verify we're now at a different position.
    const midRows = await getVisibleRows(page);
    // The sets should be different (we scrolled to top).
    const midIds = new Set(midRows.map(r => r.rowId));
    const beforeIds = new Set(beforeRows.map(r => r.rowId));
    // At least some rows should be different if we scrolled far enough.
    const allSame = [...beforeIds].every(id => midIds.has(id));
    // This is just a sanity check — if the list is very short this might
    // still be true, so we don't hard-fail on it.
    if (beforeRows[0].viewportRelativeTop !== midRows[0]?.viewportRelativeTop) {
      expect(allSame).toBe(false);
    }

    // Restore the scroll state.
    // The textarea should already have the captured state from the capture
    // button (which auto-fills it).
    await page.click('[data-testid="restore-btn"]');

    // Wait for the restore to complete: positioning + settle.
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    // Second stability check — the positioning retry loop may take a few frames.
    await waitForScrollStable(page, 300);

    // Record visible rows after restore.
    const afterRows = await getVisibleRows(page);
    expect(afterRows.length).toBeGreaterThan(0);

    // Verify: same row IDs visible.
    const afterIds = new Set(afterRows.map(r => r.rowId));
    for (const row of beforeRows) {
      expect(
        afterIds.has(row.rowId),
        `Row ${row.rowId} (index ${row.index}) was visible before but not after restore`,
      ).toBe(true);
    }

    // Verify: each row's viewport-relative position matches within tolerance.
    const afterByRowId = new Map(afterRows.map(r => [r.rowId, r]));
    for (const beforeRow of beforeRows) {
      const afterRow = afterByRowId.get(beforeRow.rowId);
      if (!afterRow) {
        continue;
      }
      expect(
        Math.abs(afterRow.viewportRelativeTop - beforeRow.viewportRelativeTop),
        `Row ${beforeRow.rowId}: position shifted by ${afterRow.viewportRelativeTop - beforeRow.viewportRelativeTop}px (before=${beforeRow.viewportRelativeTop}, after=${afterRow.viewportRelativeTop})`,
      ).toBeLessThanOrEqual(POSITION_TOLERANCE);
    }
  });

  test('capture and restore works after scrolling further down', async ({
    page,
  }) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Scroll down significantly to trigger auto-paging.
    await scrollTo(page, 6000);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);

    // Might need more data loaded — scroll a bit more to trigger paging.
    await scrollTo(page, 8000);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);

    const beforeRows = await getVisibleRows(page);
    expect(beforeRows.length).toBeGreaterThan(0);

    await page.click('[data-testid="capture-btn"]');
    const capturedText = await page.inputValue('[data-testid="restore-input"]');
    expect(capturedText).toBeTruthy();

    // Scroll to a very different location.
    await scrollTo(page, 1000);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);

    // Restore.
    await page.click('[data-testid="restore-btn"]');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    const afterRows = await getVisibleRows(page);
    expect(afterRows.length).toBeGreaterThan(0);

    // Verify same rows visible with same positions.
    const afterByRowId = new Map(afterRows.map(r => [r.rowId, r]));
    for (const beforeRow of beforeRows) {
      const afterRow = afterByRowId.get(beforeRow.rowId);
      expect(
        afterRow,
        `Row ${beforeRow.rowId} was visible before but not after restore`,
      ).toBeTruthy();
      if (afterRow) {
        expect(
          Math.abs(
            afterRow.viewportRelativeTop - beforeRow.viewportRelativeTop,
          ),
          `Row ${beforeRow.rowId}: position shifted by ${afterRow.viewportRelativeTop - beforeRow.viewportRelativeTop}px`,
        ).toBeLessThanOrEqual(POSITION_TOLERANCE);
      }
    }
  });

  test('restore with undefined state scrolls to top', async ({page}) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Scroll down.
    await scrollTo(page, 3000);
    await waitForScrollStable(page, 500);

    const scrollBefore = await getScrollTop(page);
    expect(scrollBefore).toBeGreaterThan(1000);

    // Clear the restore input and type "null" to restore undefined state.
    // We need to use the app's restore mechanism. Since restoreScrollState
    // accepts undefined, we'll inject the call directly.
    await page.evaluate(() => {
      // The textarea + restore button flow parses JSON, and JSON.parse('null')
      // returns null which is falsy like undefined.
      const textarea = document.querySelector(
        '[data-testid="restore-input"]',
      ) as HTMLTextAreaElement;
      if (textarea) {
        // Set value to 'null' which is valid JSON
        const nativeInputValueSetter = Object.getOwnPropertyDescriptor(
          window.HTMLTextAreaElement.prototype,
          'value',
        )!.set!;
        nativeInputValueSetter.call(textarea, 'null');
        textarea.dispatchEvent(new Event('input', {bubbles: true}));
        textarea.dispatchEvent(new Event('change', {bubbles: true}));
      }
    });

    await page.click('[data-testid="restore-btn"]');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);

    const scrollAfter = await getScrollTop(page);
    // Should have scrolled to top (or very near).
    expect(scrollAfter).toBeLessThan(50);
  });

  test('capture, reload, then restore preserves positions without measurement cache', async ({
    page,
  }) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Scroll down to a non-trivial position.
    await scrollTo(page, 3000);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);

    // Record visible rows before capture.
    const beforeRows = await getVisibleRows(page);
    expect(beforeRows.length).toBeGreaterThan(0);

    // Capture the scroll state.
    await page.click('[data-testid="capture-btn"]');
    const capturedText = await page.inputValue('[data-testid="restore-input"]');
    expect(capturedText).toBeTruthy();

    // Reload the page — this clears the virtualizer's measurement cache.
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Fill the restore input with the previously captured state and restore.
    await page.fill('[data-testid="restore-input"]', capturedText!);
    await page.click('[data-testid="restore-btn"]');

    // Wait for the restore to complete — needs more time since data
    // must be fetched fresh and measurements rebuilt.
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);
    await waitForScrollStable(page, 500);

    // Record visible rows after restore.
    const afterRows = await getVisibleRows(page);
    expect(afterRows.length).toBeGreaterThan(0);

    // Verify: same row IDs visible.
    const afterIds = new Set(afterRows.map(r => r.rowId));
    for (const row of beforeRows) {
      expect(
        afterIds.has(row.rowId),
        `Row ${row.rowId} (index ${row.index}) was visible before reload but not after restore`,
      ).toBe(true);
    }

    // Verify: each row's viewport-relative position matches within tolerance.
    const afterByRowId = new Map(afterRows.map(r => [r.rowId, r]));
    for (const beforeRow of beforeRows) {
      const afterRow = afterByRowId.get(beforeRow.rowId);
      if (!afterRow) {
        continue;
      }
      expect(
        Math.abs(afterRow.viewportRelativeTop - beforeRow.viewportRelativeTop),
        `Row ${beforeRow.rowId}: position shifted by ${afterRow.viewportRelativeTop - beforeRow.viewportRelativeTop}px (before=${beforeRow.viewportRelativeTop}, after=${afterRow.viewportRelativeTop})`,
      ).toBeLessThanOrEqual(POSITION_TOLERANCE);
    }
  });
});

// Helper: set the permalink ID in the input and submit.
async function setPermalink(page: Page, id: string) {
  await page.fill('[data-testid="permalink-input"]', id);
  await page.click('[data-testid="permalink-go-btn"]');
}

// Helper: clear the permalink.
async function clearPermalink(page: Page) {
  await page.click('[data-testid="permalink-clear-btn"]');
}

// Helper: get the viewport-relative position of a specific row by its ID.
// Returns undefined if the row is not in the DOM or not visible.
function getRowPosition(
  page: Page,
  rowId: string,
): Promise<{viewportRelativeTop: number} | undefined> {
  return page.evaluate(id => {
    const container = document.querySelector(
      'div[style*="overflow: auto"][style*="position: relative"]',
    ) as HTMLElement | null;
    if (!container) {
      return undefined;
    }
    const el = container.querySelector(`[data-row-id="${id}"]`);
    if (!el) {
      return undefined;
    }
    const containerRect = container.getBoundingClientRect();
    const rect = el.getBoundingClientRect();
    return {viewportRelativeTop: rect.top - containerRect.top};
  }, rowId);
}

test.describe('Initial Permalink Positioning', () => {
  test('initialPermalinkID positions target row at top of viewport', async ({
    page,
  }) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Scroll down to reveal rows that aren't near the top of the data set.
    await scrollTo(page, 3000);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);

    // Pick a visible row to use as our permalink target.
    const rows = await getVisibleRows(page);
    expect(rows.length).toBeGreaterThan(2);
    const targetRow = rows[2]; // Use the 3rd visible row.

    // Now set that row as the permalink — this changes initialPermalinkID
    // which triggers data re-fetching centered on the target and positions
    // it at the top of the viewport.
    await setPermalink(page, targetRow.rowId);

    // Wait for the permalink positioning to complete.
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    // Verify: the target row is now at the top of the viewport.
    const pos = await getRowPosition(page, targetRow.rowId);
    expect(pos, `Target row ${targetRow.rowId} should be visible`).toBeTruthy();
    expect(
      Math.abs(pos!.viewportRelativeTop),
      `Target row should be at the top of the viewport, but was at ${pos!.viewportRelativeTop}px`,
    ).toBeLessThanOrEqual(POSITION_TOLERANCE);
  });

  test('changing initialPermalinkID repositions to the new target', async ({
    page,
  }) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Scroll down, pick a row, set permalink.
    await scrollTo(page, 2000);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);
    const rowsA = await getVisibleRows(page);
    expect(rowsA.length).toBeGreaterThan(0);
    const targetA = rowsA[0];

    await setPermalink(page, targetA.rowId);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    // Verify row A is at the top.
    const posA = await getRowPosition(page, targetA.rowId);
    expect(posA, `Row A (${targetA.rowId}) should be visible`).toBeTruthy();
    expect(
      Math.abs(posA!.viewportRelativeTop),
      `Row A should be at the top, but was at ${posA!.viewportRelativeTop}px`,
    ).toBeLessThanOrEqual(POSITION_TOLERANCE);

    // Clear permalink, scroll to a different area, then pick a different row.
    await clearPermalink(page);
    await page.waitForTimeout(200);
    await scrollTo(page, 5000);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);

    const rowsB = await getVisibleRows(page);
    expect(rowsB.length).toBeGreaterThan(0);
    // Find a row that's different from targetA.
    const targetB = rowsB.find(r => r.rowId !== targetA.rowId) ?? rowsB[0];
    expect(targetB.rowId).not.toBe(targetA.rowId);

    // Set the new permalink.
    await setPermalink(page, targetB.rowId);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    // Verify row B is now at the top.
    const posB = await getRowPosition(page, targetB.rowId);
    expect(posB, `Row B (${targetB.rowId}) should be visible`).toBeTruthy();
    expect(
      Math.abs(posB!.viewportRelativeTop),
      `Row B should be at the top, but was at ${posB!.viewportRelativeTop}px`,
    ).toBeLessThanOrEqual(POSITION_TOLERANCE);

    // Verify row A is NOT at the top anymore (it may or may not be visible
    // depending on how far apart A and B are).
    const posAAfter = await getRowPosition(page, targetA.rowId);
    if (posAAfter) {
      expect(
        Math.abs(posAAfter.viewportRelativeTop),
        `Row A should no longer be at the top`,
      ).toBeGreaterThan(POSITION_TOLERANCE);
    }
  });

  test('initialPermalinkID works with shortID', async ({page}) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Get a shortID from a visible row. The app renders shortIDs in the UI.
    // We'll extract one from the rendered DOM.
    const shortID = await page.evaluate(() => {
      const rows = document.querySelectorAll('[data-row-id]');
      for (const row of rows) {
        // Look for the shortID display — it's rendered as "#<number>" in a
        // span or text node inside the row.
        const text = row.textContent ?? '';
        const match = text.match(/#(\d+)/);
        if (match) {
          return match[1];
        }
      }
      return null;
    });
    expect(shortID, 'Should find a shortID in the rendered rows').toBeTruthy();

    // Scroll away from the top.
    await scrollTo(page, 4000);
    await waitForScrollStable(page, 500);

    // Set the shortID as the permalink.
    await setPermalink(page, shortID!);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    // The row with that shortID should now be at the top of the viewport.
    // We need to find it by its UUID since data-row-id uses UUIDs.
    const topRows = await getVisibleRows(page);
    expect(topRows.length).toBeGreaterThan(0);
    // The first visible row should be the permalink target. Its
    // viewportRelativeTop should be near 0.
    expect(
      Math.abs(topRows[0].viewportRelativeTop),
      `First visible row should be at the top, but was at ${topRows[0].viewportRelativeTop}px`,
    ).toBeLessThanOrEqual(POSITION_TOLERANCE);

    // Verify this row is actually the one with the matching shortID by
    // checking the text content.
    const matchesShortID = await page.evaluate(
      ({rowId, sid}) => {
        const el = document.querySelector(`[data-row-id="${rowId}"]`);
        return el?.textContent?.includes(`#${sid}`) ?? false;
      },
      {rowId: topRows[0].rowId, sid: shortID!},
    );
    expect(matchesShortID, `Top row should contain shortID #${shortID}`).toBe(
      true,
    );
  });
});
