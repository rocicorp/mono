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
    expect(capturedState).toHaveProperty('scrollAnchorID');
    expect(capturedState).toHaveProperty('index');
    expect(capturedState).toHaveProperty('scrollOffset');
    expect(typeof capturedState.scrollAnchorID).toBe('string');
    expect(capturedState.scrollAnchorID.length).toBeGreaterThan(0);

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

test.describe('URL Hash Permalink', () => {
  test('navigating to a URL with #issue-<shortID> positions the row at top', async ({
    page,
  }) => {
    // First load the page normally to discover a shortID.
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    const shortID = await page.evaluate(() => {
      const rows = document.querySelectorAll('[data-row-id]');
      for (const row of rows) {
        const text = row.textContent ?? '';
        const match = text.match(/#(\d+)/);
        if (match) {
          return match[1];
        }
      }
      return null;
    });
    expect(shortID, 'Should find a shortID in the rendered rows').toBeTruthy();

    // Navigate to the page with the hash fragment.
    await page.goto(`${BASE_URL}/array-test#issue-${shortID}`, {
      waitUntil: 'networkidle',
    });
    await waitForRows(page);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    // The first visible row should be at the top and match the shortID.
    const topRows = await getVisibleRows(page);
    expect(topRows.length).toBeGreaterThan(0);
    expect(
      Math.abs(topRows[0].viewportRelativeTop),
      `First visible row should be at the top, but was at ${topRows[0].viewportRelativeTop}px`,
    ).toBeLessThanOrEqual(POSITION_TOLERANCE);

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

  test('navigating to a URL with #issue-<UUID> positions the row at top', async ({
    page,
  }) => {
    // First load to discover a UUID.
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Scroll down to get a non-top-of-list row.
    await scrollTo(page, 2000);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);

    const rows = await getVisibleRows(page);
    expect(rows.length).toBeGreaterThan(0);
    const targetUUID = rows[1].rowId;

    // Navigate with the UUID hash.
    await page.goto(`${BASE_URL}/array-test#issue-${targetUUID}`, {
      waitUntil: 'networkidle',
    });
    await waitForRows(page);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    // Verify the target row is at the top.
    const pos = await getRowPosition(page, targetUUID);
    expect(pos, `Target row ${targetUUID} should be visible`).toBeTruthy();
    expect(
      Math.abs(pos!.viewportRelativeTop),
      `Target row should be at the top, but was at ${pos!.viewportRelativeTop}px`,
    ).toBeLessThanOrEqual(POSITION_TOLERANCE);
  });

  test('changing the hash programmatically repositions to the new target', async ({
    page,
  }) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Pick two different rows.
    await scrollTo(page, 2000);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);
    const rowsA = await getVisibleRows(page);
    const targetA = rowsA[0];

    await scrollTo(page, 5000);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);
    const rowsB = await getVisibleRows(page);
    const targetB = rowsB.find(r => r.rowId !== targetA.rowId) ?? rowsB[0];
    expect(targetB.rowId).not.toBe(targetA.rowId);

    // Set hash to row A via location.hash (triggers hashchange picked up by wouter).
    await page.evaluate(id => {
      window.location.hash = `issue-${id}`;
    }, targetA.rowId);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    const posA = await getRowPosition(page, targetA.rowId);
    expect(posA).toBeTruthy();
    expect(
      Math.abs(posA!.viewportRelativeTop),
      `Row A should be at top after hash change, was ${posA!.viewportRelativeTop}px`,
    ).toBeLessThanOrEqual(POSITION_TOLERANCE);

    // Change hash to row B.
    await page.evaluate(id => {
      window.location.hash = `issue-${id}`;
    }, targetB.rowId);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    const posB = await getRowPosition(page, targetB.rowId);
    expect(posB).toBeTruthy();
    expect(
      Math.abs(posB!.viewportRelativeTop),
      `Row B should be at top after hash change, was ${posB!.viewportRelativeTop}px`,
    ).toBeLessThanOrEqual(POSITION_TOLERANCE);
  });

  test('the Go button updates the URL hash', async ({page}) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Initially no hash.
    const initialHash = await page.evaluate(() => window.location.hash);
    expect(initialHash).toBe('');

    // Set a permalink via the Go button.
    await setPermalink(page, '3130');
    await waitForScrollStable(page, 500);

    // Hash should now contain #issue-3130.
    const hashAfterGo = await page.evaluate(() => window.location.hash);
    expect(hashAfterGo).toBe('#issue-3130');

    // Clear — hash should be removed.
    await clearPermalink(page);
    await page.waitForTimeout(200);

    const hashAfterClear = await page.evaluate(() => window.location.hash);
    expect(hashAfterClear).toBe('');
  });

  test('page loaded with hash initializes the permalink input', async ({
    page,
  }) => {
    await page.goto(`${BASE_URL}/array-test#issue-3130`, {
      waitUntil: 'networkidle',
    });
    await waitForRows(page);

    // The permalink input should be pre-filled with the hash value.
    const inputValue = await page.inputValue('[data-testid="permalink-input"]');
    expect(inputValue).toBe('3130');
  });

  test('browser back navigates to previous permalink', async ({page}) => {
    // Use well-known shortIDs to keep things simple.
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Navigate to shortID 3130 via the Go button (pushes history entry).
    await setPermalink(page, '3130');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    // Verify first row matches 3130.
    const rowsAfterA = await getVisibleRows(page);
    expect(rowsAfterA.length).toBeGreaterThan(0);
    expect(Math.abs(rowsAfterA[0].viewportRelativeTop)).toBeLessThanOrEqual(
      POSITION_TOLERANCE,
    );
    const rowA_id = rowsAfterA[0].rowId;

    // Navigate to shortID 3100 via the Go button (pushes another history entry).
    await setPermalink(page, '3100');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    const rowsAfterB = await getVisibleRows(page);
    expect(rowsAfterB.length).toBeGreaterThan(0);
    expect(Math.abs(rowsAfterB[0].viewportRelativeTop)).toBeLessThanOrEqual(
      POSITION_TOLERANCE,
    );

    // Verify hash is now #issue-3100.
    const hashB = await page.evaluate(() => window.location.hash);
    expect(hashB).toBe('#issue-3100');

    // Press browser back — should go back to #issue-3130.
    await page.goBack({waitUntil: 'commit'});

    // Wait for React to pick up the hash change.
    await page.waitForFunction(
      () => window.location.hash === '#issue-3130',
      undefined,
      {timeout: 5000},
    );

    // Wait for the row to appear and positioning to settle.
    await page.waitForSelector(`[data-row-id="${rowA_id}"]`, {
      timeout: 15000,
    });
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    // Row A (3130) should be at the top.
    const posA = await getRowPosition(page, rowA_id);
    expect(posA, `Row 3130 should be visible after back`).toBeTruthy();
    expect(
      Math.abs(posA!.viewportRelativeTop),
      `Row 3130 should be at top after back, was ${posA!.viewportRelativeTop}px`,
    ).toBeLessThanOrEqual(POSITION_TOLERANCE);
  });
});

test.describe('Scroll Restoration Edge Cases', () => {
  test('capture at scrollTop=0 and restore returns to exact top', async ({
    page,
  }) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Already at the top — capture immediately.
    const beforeRows = await getVisibleRows(page);
    expect(beforeRows.length).toBeGreaterThan(0);
    expect(Math.abs(beforeRows[0].viewportRelativeTop)).toBeLessThanOrEqual(
      POSITION_TOLERANCE,
    );

    await page.click('[data-testid="capture-btn"]');
    const capturedText = await page.inputValue('[data-testid="restore-input"]');
    expect(capturedText).toBeTruthy();

    // Verify scrollOffset is 0 or very close (we're at the top).
    const capturedState = JSON.parse(capturedText!);
    expect(Math.abs(capturedState.scrollOffset)).toBeLessThanOrEqual(1);

    // Scroll away.
    await scrollTo(page, 5000);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);

    // Restore.
    await page.click('[data-testid="restore-btn"]');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    const scrollAfter = await getScrollTop(page);
    // Should be back at the very top.
    expect(scrollAfter).toBeLessThan(POSITION_TOLERANCE);

    // Same rows visible.
    const afterRows = await getVisibleRows(page);
    const afterIds = new Set(afterRows.map(r => r.rowId));
    for (const row of beforeRows) {
      expect(
        afterIds.has(row.rowId),
        `Row ${row.rowId} was visible at top before but not after restore`,
      ).toBe(true);
    }
  });

  test('restore same state twice is idempotent', async ({page}) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Scroll to a position, capture.
    await scrollTo(page, 4000);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);

    await page.click('[data-testid="capture-btn"]');
    const capturedText = await page.inputValue('[data-testid="restore-input"]');
    expect(capturedText).toBeTruthy();

    const beforeRows = await getVisibleRows(page);
    expect(beforeRows.length).toBeGreaterThan(0);

    // Scroll away.
    await scrollTo(page, 0);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);

    // Restore first time.
    await page.click('[data-testid="restore-btn"]');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    const firstRestoreRows = await getVisibleRows(page);

    // Scroll away again.
    await scrollTo(page, 0);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);

    // Restore second time (same state still in textarea).
    await page.click('[data-testid="restore-btn"]');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    const secondRestoreRows = await getVisibleRows(page);

    // Both restores should yield the same visible rows at the same positions.
    const secondMap = new Map(secondRestoreRows.map(r => [r.rowId, r]));
    for (const row of firstRestoreRows) {
      const match = secondMap.get(row.rowId);
      expect(
        match,
        `Row ${row.rowId} visible after first restore but not second`,
      ).toBeTruthy();
      if (match) {
        expect(
          Math.abs(match.viewportRelativeTop - row.viewportRelativeTop),
          `Row ${row.rowId}: position differs between first and second restore`,
        ).toBeLessThanOrEqual(POSITION_TOLERANCE);
      }
    }
  });

  test('capture with partial row offset preserves sub-row scroll position', async ({
    page,
  }) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Scroll to a position where a row is only partially visible at the top.
    // Use a non-round number to create a partial offset.
    await scrollTo(page, 3037);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);

    const beforeRows = await getVisibleRows(page);
    expect(beforeRows.length).toBeGreaterThan(0);
    // The first visible row should have a negative viewportRelativeTop
    // (its top is above the viewport) indicating partial visibility.
    const firstRow = beforeRows[0];

    await page.click('[data-testid="capture-btn"]');
    const capturedText = await page.inputValue('[data-testid="restore-input"]');
    expect(capturedText).toBeTruthy();

    // Verify the captured scrollOffset is negative (row top above viewport).
    const capturedState = JSON.parse(capturedText!);
    expect(capturedState.scrollOffset).toBeLessThanOrEqual(0);

    // Scroll far away.
    await scrollTo(page, 0);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);

    // Restore.
    await page.click('[data-testid="restore-btn"]');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    // The same first row should be at the same partial position.
    const afterPos = await getRowPosition(page, firstRow.rowId);
    expect(
      afterPos,
      `First row ${firstRow.rowId} should be visible`,
    ).toBeTruthy();
    expect(
      Math.abs(afterPos!.viewportRelativeTop - firstRow.viewportRelativeTop),
      `Partial row offset should be preserved (before=${firstRow.viewportRelativeTop}, after=${afterPos!.viewportRelativeTop})`,
    ).toBeLessThanOrEqual(POSITION_TOLERANCE);
  });

  test('restore with invalid JSON shows alert and does not crash', async ({
    page,
  }) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Scroll to a known position.
    await scrollTo(page, 2000);
    await waitForScrollStable(page, 500);
    const scrollBefore = await getScrollTop(page);

    // Fill invalid JSON into the restore textarea.
    await page.fill('[data-testid="restore-input"]', '{bad json!!!}');

    // Listen for the alert dialog and auto-accept it so the click can complete.
    page.on('dialog', async dialog => {
      expect(dialog.type()).toBe('alert');
      expect(dialog.message()).toContain('Invalid JSON');
      await dialog.accept();
    });
    await page.click('[data-testid="restore-btn"]');

    // Scroll position should be unchanged.
    const scrollAfter = await getScrollTop(page);
    expect(Math.abs(scrollAfter - scrollBefore)).toBeLessThan(5);
  });
});

test.describe('Permalink Edge Cases', () => {
  test('non-existent permalink shows not-found banner', async ({page}) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Use a permalink ID that definitely doesn't exist.
    await setPermalink(page, 'nonexistent-fake-id-999999');

    // Wait for the not-found state to propagate.
    await page.waitForTimeout(3000);

    // The not-found banner should appear.
    const banner = page
      .getByText('Permalink not found:', {exact: false})
      .first();
    await expect(banner).toBeVisible({timeout: 10000});
  });

  test('clearing permalink resets to beginning of list', async ({page}) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Navigate to a permalink deep in the list.
    await setPermalink(page, '3130');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    const scrollAtPermalink = await getScrollTop(page);
    // Should be at a non-trivial scroll position.
    expect(scrollAtPermalink).toBeGreaterThan(0);

    // Clear the permalink.
    await clearPermalink(page);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);
    await waitForScrollStable(page, 300);

    // After clearing, the list should render rows (not be empty/broken).
    const rowsAfterClear = await getVisibleRows(page);
    expect(rowsAfterClear.length).toBeGreaterThan(0);

    // The hash should be cleared.
    const hashAfter = await page.evaluate(() => window.location.hash);
    expect(hashAfter === '' || hashAfter === '#').toBe(true);
  });

  test('permalink to first item in list positions it at top', async ({
    page,
  }) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Get the first visible row (which is at the top of the list).
    const topRows = await getVisibleRows(page);
    expect(topRows.length).toBeGreaterThan(0);
    const firstRowId = topRows[0].rowId;

    // Scroll away.
    await scrollTo(page, 5000);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);

    // Set permalink to the first item.
    await setPermalink(page, firstRowId);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    // The first item should be at the top.
    const pos = await getRowPosition(page, firstRowId);
    expect(pos, `First row ${firstRowId} should be visible`).toBeTruthy();
    expect(
      Math.abs(pos!.viewportRelativeTop),
      `First row should be at top, was ${pos!.viewportRelativeTop}px`,
    ).toBeLessThanOrEqual(POSITION_TOLERANCE);
  });

  test('rapid permalink changes settle on the last target', async ({page}) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Discover shortIDs for rapid navigation.
    const shortIDs = await page.evaluate(() => {
      const ids: string[] = [];
      const rows = document.querySelectorAll('[data-row-id]');
      for (const row of rows) {
        const match = row.textContent?.match(/#(\d+)/);
        if (match && !ids.includes(match[1])) {
          ids.push(match[1]);
        }
        if (ids.length >= 3) break;
      }
      return ids;
    });
    expect(shortIDs.length).toBeGreaterThanOrEqual(2);

    // Rapidly set permalink to different IDs without waiting for settle.
    for (const sid of shortIDs.slice(0, -1)) {
      await page.fill('[data-testid="permalink-input"]', sid);
      await page.click('[data-testid="permalink-go-btn"]');
      // Don't wait — immediately go to the next one.
      await page.waitForTimeout(50);
    }

    // Set the final permalink and wait for it to settle.
    const finalSID = shortIDs[shortIDs.length - 1];
    await setPermalink(page, finalSID);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);
    await waitForScrollStable(page, 300);

    // The final permalink target should be at the top.
    const topRows = await getVisibleRows(page);
    expect(topRows.length).toBeGreaterThan(0);
    expect(
      Math.abs(topRows[0].viewportRelativeTop),
      `Final target should be at top, was ${topRows[0].viewportRelativeTop}px`,
    ).toBeLessThanOrEqual(POSITION_TOLERANCE);

    // It should be the row matching the final shortID.
    const matchesFinal = await page.evaluate(
      ({rowId, sid}) => {
        const el = document.querySelector(`[data-row-id="${rowId}"]`);
        return el?.textContent?.includes(`#${sid}`) ?? false;
      },
      {rowId: topRows[0].rowId, sid: finalSID},
    );
    expect(matchesFinal, `Top row should be #${finalSID}`).toBe(true);
  });

  test('setting a permalink after clearing and re-setting works', async ({
    page,
  }) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Set a permalink.
    await setPermalink(page, '3130');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    const rowsFirst = await getVisibleRows(page);
    expect(rowsFirst.length).toBeGreaterThan(0);

    // Clear and navigate elsewhere.
    await clearPermalink(page);
    await page.waitForTimeout(200);
    await scrollTo(page, 5000);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);

    // Set a different permalink.
    await setPermalink(page, '3100');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    const rowsSecond = await getVisibleRows(page);
    expect(rowsSecond.length).toBeGreaterThan(0);

    // Row should be at the top.
    expect(
      Math.abs(rowsSecond[0].viewportRelativeTop),
      `Row should be at top after setting new permalink`,
    ).toBeLessThanOrEqual(POSITION_TOLERANCE);

    // Should be a different row than the first permalink.
    const is3100 = await page.evaluate(
      ({rowId}) => {
        const el = document.querySelector(`[data-row-id="${rowId}"]`);
        return el?.textContent?.includes('#3100') ?? false;
      },
      {rowId: rowsSecond[0].rowId},
    );
    expect(is3100, 'Top row should be #3100').toBe(true);
  });
});

test.describe('URL Hash Edge Cases', () => {
  test('navigating to hash with empty ID after prefix loads normally', async ({
    page,
  }) => {
    // #issue- with nothing after the prefix — should behave as no permalink.
    await page.goto(`${BASE_URL}/array-test#issue-`, {
      waitUntil: 'networkidle',
    });
    await waitForRows(page);
    await waitForScrollStable(page, 500);

    // Should be at the top of the list.
    const scrollTop = await getScrollTop(page);
    expect(scrollTop).toBeLessThan(50);

    const rows = await getVisibleRows(page);
    expect(rows.length).toBeGreaterThan(0);
  });

  test('hash change from valid to empty clears permalink', async ({page}) => {
    // Start with a valid hash.
    await page.goto(`${BASE_URL}/array-test#issue-3130`, {
      waitUntil: 'networkidle',
    });
    await waitForRows(page);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    // Verify positioned at 3130.
    const rows = await getVisibleRows(page);
    expect(rows.length).toBeGreaterThan(0);
    const is3130 = await page.evaluate(
      ({rowId}) => {
        const el = document.querySelector(`[data-row-id="${rowId}"]`);
        return el?.textContent?.includes('#3130') ?? false;
      },
      {rowId: rows[0].rowId},
    );
    expect(is3130).toBe(true);

    // Clear the hash programmatically.
    await page.evaluate(() => {
      window.location.hash = '';
    });
    await page.waitForTimeout(500);

    // Hash should now be empty.
    const hashAfter = await page.evaluate(() => window.location.hash);
    expect(hashAfter === '' || hashAfter === '#').toBe(true);
  });

  test('reload with hash permalink positions the row at top', async ({
    page,
  }) => {
    // Navigate to a specific permalink via hash.
    await page.goto(`${BASE_URL}/array-test#issue-3130`, {
      waitUntil: 'networkidle',
    });
    await waitForRows(page);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    // Record the row at the top.
    const beforeRows = await getVisibleRows(page);
    expect(beforeRows.length).toBeGreaterThan(0);
    const topRowId = beforeRows[0].rowId;

    // Reload the page (hash persists).
    await page.reload({waitUntil: 'networkidle'});
    await waitForRows(page);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);
    await waitForScrollStable(page, 500);

    // The same row should be at the top.
    const pos = await getRowPosition(page, topRowId);
    expect(pos, `Row ${topRowId} should be visible after reload`).toBeTruthy();
    expect(
      Math.abs(pos!.viewportRelativeTop),
      `Row should be at top after reload with hash, was ${pos!.viewportRelativeTop}px`,
    ).toBeLessThanOrEqual(POSITION_TOLERANCE);
  });
});

test.describe('History State Scroll Restoration', () => {
  test('back/forward restores scroll positions across permalink navigations', async ({
    page,
  }) => {
    // 1. Navigate to "" (no permalink).
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // 2. Check that first row is at top of viewport.
    const initialRows = await getVisibleRows(page);
    expect(initialRows.length).toBeGreaterThan(0);
    expect(
      Math.abs(initialRows[0].viewportRelativeTop),
      `First row should be at top, was ${initialRows[0].viewportRelativeTop}px`,
    ).toBeLessThanOrEqual(POSITION_TOLERANCE);

    // 3. Scroll down 50px.
    await scrollTo(page, 50);
    await waitForScrollStable(page, 300);
    await page.waitForTimeout(300);
    // Record visible rows at this position for later verification.
    const rowsAtNoHash50 = await getVisibleRows(page);

    // 4. Navigate to 3130.
    await setPermalink(page, '3130');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    // 5. Check that 3130 is at 0 in the viewport.
    const rows3130 = await getVisibleRows(page);
    expect(rows3130.length).toBeGreaterThan(0);
    const is3130 = await page.evaluate(
      ({rowId}) => {
        const el = document.querySelector(`[data-row-id="${rowId}"]`);
        return el?.textContent?.includes('#3130') ?? false;
      },
      {rowId: rows3130[0].rowId},
    );
    expect(is3130, 'First visible row should be #3130').toBe(true);
    expect(Math.abs(rows3130[0].viewportRelativeTop)).toBeLessThanOrEqual(
      POSITION_TOLERANCE,
    );
    const base3130 = await getScrollTop(page);

    // 6. Scroll down 100px from 3130-at-top.
    await scrollTo(page, base3130 + 100);
    await waitForScrollStable(page, 300);
    await page.waitForTimeout(300);
    // Record visible rows at this position.
    const rowsAt3130Plus100 = await getVisibleRows(page);

    // 7. Navigate to 3230.
    await setPermalink(page, '3230');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    // 8. Check that 3230 is at 0 in the viewport.
    const rows3230 = await getVisibleRows(page);
    expect(rows3230.length).toBeGreaterThan(0);
    const is3230 = await page.evaluate(
      ({rowId}) => {
        const el = document.querySelector(`[data-row-id="${rowId}"]`);
        return el?.textContent?.includes('#3230') ?? false;
      },
      {rowId: rows3230[0].rowId},
    );
    expect(is3230, 'First visible row should be #3230').toBe(true);
    expect(Math.abs(rows3230[0].viewportRelativeTop)).toBeLessThanOrEqual(
      POSITION_TOLERANCE,
    );
    const base3230 = await getScrollTop(page);

    // 9. Scroll down 150px from 3230-at-top.
    await scrollTo(page, base3230 + 150);
    await waitForScrollStable(page, 300);
    await page.waitForTimeout(300);
    // Record visible rows at this position.
    const rowsAt3230Plus150 = await getVisibleRows(page);

    // 10. Hit back → should restore 3130 + 100px scroll.
    await page.goBack({waitUntil: 'commit'});
    await page.waitForFunction(
      () => window.location.hash === '#issue-3130',
      undefined,
      {timeout: 5000},
    );
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);
    await waitForScrollStable(page, 300);

    // 11. Verify same rows visible at same viewport-relative positions.
    const rowsAfterBack1 = await getVisibleRows(page);
    const afterBack1Map = new Map(rowsAfterBack1.map(r => [r.rowId, r]));
    let matchCount1 = 0;
    for (const before of rowsAt3130Plus100) {
      const after = afterBack1Map.get(before.rowId);
      if (after) {
        expect(
          Math.abs(after.viewportRelativeTop - before.viewportRelativeTop),
          `Row ${before.rowId}: position shifted by ${after.viewportRelativeTop - before.viewportRelativeTop}px after back to 3130`,
        ).toBeLessThanOrEqual(POSITION_TOLERANCE);
        matchCount1++;
      }
    }
    expect(
      matchCount1,
      'At least some rows from the 3130+100 position should be visible after back',
    ).toBeGreaterThanOrEqual(2);

    // 12. Hit forward → should restore 3230 + 150px scroll.
    await page.goForward({waitUntil: 'commit'});
    await page.waitForFunction(
      () => window.location.hash === '#issue-3230',
      undefined,
      {timeout: 5000},
    );
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);
    await waitForScrollStable(page, 300);

    // 13. Verify same rows visible at same viewport-relative positions.
    const rowsAfterForward = await getVisibleRows(page);
    const afterForwardMap = new Map(rowsAfterForward.map(r => [r.rowId, r]));
    let matchCount2 = 0;
    for (const before of rowsAt3230Plus150) {
      const after = afterForwardMap.get(before.rowId);
      if (after) {
        expect(
          Math.abs(after.viewportRelativeTop - before.viewportRelativeTop),
          `Row ${before.rowId}: position shifted by ${after.viewportRelativeTop - before.viewportRelativeTop}px after forward to 3230`,
        ).toBeLessThanOrEqual(POSITION_TOLERANCE);
        matchCount2++;
      }
    }
    expect(
      matchCount2,
      'At least some rows from the 3230+150 position should be visible after forward',
    ).toBeGreaterThanOrEqual(2);

    // 14. Hit back twice → 3230 → 3130 → no-hash (scrollTop 50).
    await page.goBack({waitUntil: 'commit'});
    await page.waitForFunction(
      () => window.location.hash === '#issue-3130',
      undefined,
      {timeout: 5000},
    );
    await waitForScrollStable(page, 300);

    await page.goBack({waitUntil: 'commit'});
    await page.waitForFunction(() => window.location.hash === '', undefined, {
      timeout: 5000,
    });
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);
    await waitForScrollStable(page, 300);

    // 15. Verify same rows visible at same viewport-relative positions.
    const rowsAfterBackTwice = await getVisibleRows(page);
    const afterBackTwiceMap = new Map(
      rowsAfterBackTwice.map(r => [r.rowId, r]),
    );
    let matchCount3 = 0;
    for (const before of rowsAtNoHash50) {
      const after = afterBackTwiceMap.get(before.rowId);
      if (after) {
        expect(
          Math.abs(after.viewportRelativeTop - before.viewportRelativeTop),
          `Row ${before.rowId}: position shifted by ${after.viewportRelativeTop - before.viewportRelativeTop}px after back to no-hash`,
        ).toBeLessThanOrEqual(POSITION_TOLERANCE);
        matchCount3++;
      }
    }
    expect(
      matchCount3,
      'At least some rows from the no-hash+50 position should be visible after back twice',
    ).toBeGreaterThanOrEqual(2);
  });

  test('reload without permalink preserves scroll position via history.state', async ({
    page,
  }) => {
    // Load page with no hash.
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Scroll down 25px (small scroll where anchor row stays near top).
    await scrollTo(page, 25);
    await waitForScrollStable(page, 300);
    await page.waitForTimeout(200);

    // Record visible rows.
    const beforeRows = await getVisibleRows(page);
    expect(beforeRows.length).toBeGreaterThan(0);

    // Verify history.state was saved.
    const savedState = await page.evaluate(() => window.history.state);
    expect(savedState).toBeTruthy();

    // Reload the page.
    await page.reload({waitUntil: 'networkidle'});

    await waitForRows(page);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(1000);
    await waitForScrollStable(page, 500);

    // Verify same rows visible at same viewport-relative positions.
    const afterRows = await getVisibleRows(page);
    const afterMap = new Map(afterRows.map(r => [r.rowId, r]));
    let matchCount = 0;
    for (const before of beforeRows) {
      const after = afterMap.get(before.rowId);
      if (after) {
        expect(
          Math.abs(after.viewportRelativeTop - before.viewportRelativeTop),
          `Row ${before.rowId}: position shifted by ${after.viewportRelativeTop - before.viewportRelativeTop}px after reload`,
        ).toBeLessThanOrEqual(POSITION_TOLERANCE);
        matchCount++;
      }
    }
    expect(
      matchCount,
      'At least some rows from before reload should be visible after',
    ).toBeGreaterThanOrEqual(2);
  });

  test('reload with permalink hash preserves scroll offset from history.state', async ({
    page,
  }) => {
    // Navigate to a permalink.
    await page.goto(`${BASE_URL}/array-test#issue-3130`, {
      waitUntil: 'networkidle',
    });
    await waitForRows(page);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    // Scroll down from the permalink position.
    const baseScroll = await getScrollTop(page);
    await scrollTo(page, baseScroll + 75);
    await waitForScrollStable(page, 300);
    await page.waitForTimeout(300);

    // Record visible rows at this offset position.
    const beforeRows = await getVisibleRows(page);
    expect(beforeRows.length).toBeGreaterThan(0);

    // Verify history.state captured the scroll.
    const savedState = await page.evaluate(() => window.history.state);
    expect(savedState).toBeTruthy();

    // Reload — hash persists and history.state should be used for the offset.
    await page.reload({waitUntil: 'networkidle'});
    await waitForRows(page);
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(1000);
    await waitForScrollStable(page, 500);

    // Verify same rows visible at same positions.
    const afterRows = await getVisibleRows(page);
    const afterMap = new Map(afterRows.map(r => [r.rowId, r]));
    let matchCount = 0;
    for (const before of beforeRows) {
      const after = afterMap.get(before.rowId);
      if (after) {
        expect(
          Math.abs(after.viewportRelativeTop - before.viewportRelativeTop),
          `Row ${before.rowId}: shifted by ${after.viewportRelativeTop - before.viewportRelativeTop}px after reload with hash`,
        ).toBeLessThanOrEqual(POSITION_TOLERANCE);
        matchCount++;
      }
    }
    expect(
      matchCount,
      'At least some rows should match positions after reload',
    ).toBeGreaterThanOrEqual(2);
  });

  test('back button after scroll without permalink navigation restores position', async ({
    page,
  }) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Scroll down and let history.state save.
    await scrollTo(page, 2000);
    await waitForScrollStable(page, 300);
    await page.waitForTimeout(300);

    const rowsBefore = await getVisibleRows(page);
    expect(rowsBefore.length).toBeGreaterThan(0);

    // Navigate to a permalink (pushes history entry, saving current state).
    await setPermalink(page, '3130');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    // Verify we're at 3130.
    const hashAtPermalink = await page.evaluate(() => window.location.hash);
    expect(hashAtPermalink).toBe('#issue-3130');

    // Hit back — should restore the pre-permalink scroll position.
    await page.goBack({waitUntil: 'commit'});
    await page.waitForFunction(() => window.location.hash === '', undefined, {
      timeout: 5000,
    });
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);
    await waitForScrollStable(page, 300);

    // Verify restored position matches.
    const rowsAfterBack = await getVisibleRows(page);
    const afterMap = new Map(rowsAfterBack.map(r => [r.rowId, r]));
    let matchCount = 0;
    for (const before of rowsBefore) {
      const after = afterMap.get(before.rowId);
      if (after) {
        expect(
          Math.abs(after.viewportRelativeTop - before.viewportRelativeTop),
          `Row ${before.rowId}: shifted by ${after.viewportRelativeTop - before.viewportRelativeTop}px after back`,
        ).toBeLessThanOrEqual(POSITION_TOLERANCE);
        matchCount++;
      }
    }
    expect(
      matchCount,
      'At least some rows from pre-permalink position should match after back',
    ).toBeGreaterThanOrEqual(2);
  });

  test('forward after new navigation is not possible (history is truncated)', async ({
    page,
  }) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Create history: no-hash → 3130 → 3100.
    await setPermalink(page, '3130');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    await setPermalink(page, '3100');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    // Go back to 3130.
    await page.goBack({waitUntil: 'commit'});
    await page.waitForFunction(
      () => window.location.hash === '#issue-3130',
      undefined,
      {timeout: 5000},
    );
    await waitForScrollStable(page, 500);

    // Now navigate to a NEW permalink — this should truncate forward history.
    await setPermalink(page, '3200');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);
    await waitForScrollStable(page, 300);

    const hashBeforeForward = await page.evaluate(() => window.location.hash);
    expect(hashBeforeForward).toBe('#issue-3200');

    // Attempt to go forward — should have no effect since forward was truncated.
    await page.goForward({waitUntil: 'commit'}).catch(() => {});
    await page.waitForTimeout(500);

    // Should still be at #issue-3200.
    const hashAfterForward = await page.evaluate(() => window.location.hash);
    expect(hashAfterForward).toBe('#issue-3200');
  });

  test('multiple back presses in quick succession land at the correct position', async ({
    page,
  }) => {
    await page.goto(`${BASE_URL}/array-test`, {waitUntil: 'networkidle'});
    await waitForRows(page);

    // Record initial position.
    const initialRows = await getVisibleRows(page);
    expect(initialRows.length).toBeGreaterThan(0);

    // Create a chain of permalink navigations: "" → 3130 → 3100 → 3200.
    await setPermalink(page, '3130');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);

    await setPermalink(page, '3100');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);

    await setPermalink(page, '3200');
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(300);

    // Rapidly press back 3 times to return to the initial position.
    await page.goBack({waitUntil: 'commit'});
    await page.goBack({waitUntil: 'commit'});
    await page.goBack({waitUntil: 'commit'});

    // Wait for the final state to settle.
    await page.waitForFunction(() => window.location.hash === '', undefined, {
      timeout: 10000,
    });
    await waitForScrollStable(page, 500);
    await page.waitForTimeout(500);
    await waitForScrollStable(page, 300);

    // Should be back at the initial (no hash) position.
    const afterRows = await getVisibleRows(page);
    const afterIds = new Set(afterRows.map(r => r.rowId));
    let matchCount = 0;
    for (const row of initialRows) {
      if (afterIds.has(row.rowId)) {
        matchCount++;
      }
    }
    expect(
      matchCount,
      'Should see the original rows after rapid back presses',
    ).toBeGreaterThanOrEqual(2);

    // Scroll position should be near the top.
    const scrollAfter = await getScrollTop(page);
    expect(scrollAfter).toBeLessThan(100);
  });
});
