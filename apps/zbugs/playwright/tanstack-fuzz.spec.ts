// oxlint-disable no-explicit-any
// oxlint-disable require-await
// oxlint-disable no-console

/**
 * Tanstack Virtualizer Fuzz Tests
 *
 * These tests verify that scroll position is preserved when splicing rows
 * above the viewport in a virtualized list.
 *
 * Running the tests:
 *   cd apps/zbugs
 *   npx playwright test tanstack-fuzz.spec.ts
 *
 * Environment variables:
 *   URL           - Base URL (default: http://localhost:5173)
 *   NUM_FUZZ_TESTS - Number of random test cases (default: 50)
 *   SEED          - Random seed for reproducibility (default: current timestamp)
 *
 * Examples:
 *   # Run with default settings
 *   npx playwright test tanstack-fuzz.spec.ts
 *
 *   # Run 100 tests with a specific seed for reproducibility
 *   NUM_FUZZ_TESTS=100 SEED=12345 npx playwright test tanstack-fuzz.spec.ts
 *
 *   # Run against a different URL
 *   URL=http://localhost:3000 npx playwright test tanstack-fuzz.spec.ts
 */

import {expect, test, type Page} from '@playwright/test';

const BASE_URL = process.env.URL ?? 'http://localhost:5173';
const NUM_FUZZ_TESTS = parseInt(process.env.NUM_FUZZ_TESTS ?? '50');
const SEED = parseInt(process.env.SEED ?? Date.now().toString());

// Position tolerance in pixels - scrollTop only accepts integers, so 1px is the minimum achievable accuracy
const POSITION_TOLERANCE = 1;

// ============================================================================
// Seeded Random Number Generator
// ============================================================================

class SeededRandom {
  private seed: number;

  constructor(seed: number) {
    this.seed = seed;
  }

  next(): number {
    const x = Math.sin(this.seed++) * 10000;
    return x - Math.floor(x);
  }

  nextInt(min: number, max: number): number {
    return Math.floor(this.next() * (max - min + 1)) + min;
  }
}

// ============================================================================
// Test Case Types
// ============================================================================

interface TestCase {
  initialRows: number;
  scrollOffset: number;
  spliceIndex: number;
  deleteCount: number;
  insertCount: number;
  referenceRow: number;
}

interface TestResult {
  case: number;
  test: TestCase;
  passed: boolean;
  error?: string;
  actualPosition?: number;
  expectedPosition?: number;
}

// ============================================================================
// Page Helpers
// ============================================================================

/**
 * Gets the position of a row (by its "Row N" content) relative to the container.
 */
async function getRowPosition(
  page: Page,
  rowNumber: number,
): Promise<number | null> {
  return page.evaluate(refRow => {
    const scrollDivs = Array.from(document.querySelectorAll('div'));
    const listContainer = scrollDivs.find(
      d =>
        (d as HTMLElement).style.overflow === 'auto' &&
        (d as HTMLElement).style.position === 'relative',
    ) as HTMLElement;

    if (!listContainer) return null;

    const items = Array.from(document.querySelectorAll('[data-index]'));
    const refItem = items.find(el =>
      el.textContent?.includes(`Row ${refRow}`),
    ) as HTMLElement;

    if (!refItem) return null;

    const rect = refItem.getBoundingClientRect();
    const containerRect = listContainer.getBoundingClientRect();
    return rect.top - containerRect.top;
  }, rowNumber);
}

/**
 * Gets the current scrollTop of the list container.
 */
async function getScrollTop(page: Page): Promise<number> {
  return page.evaluate(() => {
    const scrollDivs = Array.from(document.querySelectorAll('div'));
    const listContainer = scrollDivs.find(
      d =>
        (d as HTMLElement).style.overflow === 'auto' &&
        (d as HTMLElement).style.position === 'relative',
    ) as HTMLElement;
    return listContainer?.scrollTop ?? 0;
  });
}

/**
 * Scrolls to a specific row index using the virtualizer.
 */
async function scrollToRow(page: Page, rowIndex: number): Promise<void> {
  await page.evaluate(refRow => {
    if ((window as any).virtualizer) {
      (window as any).virtualizer.scrollToIndex(refRow, {
        align: 'start',
        behavior: 'auto',
      });
    }
  }, rowIndex);
  await page.waitForTimeout(200);
}

/**
 * Resets the list to 1000 rows.
 */
async function resetList(page: Page): Promise<void> {
  await page.click('button:has-text("Reset")');
  await page.waitForTimeout(200);
}

/**
 * Performs a splice operation via the UI.
 */
async function performSplice(
  page: Page,
  index: number,
  deleteCount: number,
  insertCount: number,
  waitMs = 500,
): Promise<void> {
  await page
    .locator('text=Index:')
    .locator('..')
    .locator('input')
    .fill(index.toString());
  await page
    .locator('text=Delete Count:')
    .locator('..')
    .locator('input')
    .fill(deleteCount.toString());
  await page
    .locator('text=Insert Count:')
    .locator('..')
    .locator('input')
    .fill(insertCount.toString());
  await page.waitForTimeout(50);
  await page.click('button:has-text("Splice")');
  await page.waitForTimeout(waitMs);
}

// ============================================================================
// Test Case Generation
// ============================================================================

function generateTestCase(rng: SeededRandom): TestCase {
  // Always use 1000 rows to simplify
  const initialRows = 1000;

  // Pick a reference row to scroll to (between row 50 and 900)
  const referenceRow = rng.nextInt(50, 900);

  // Splice WELL BEFORE the reference row (at least 30 rows before)
  const spliceIndex = rng.nextInt(0, referenceRow - 30);

  // Delete count: 0 to 20 rows (but not getting close to reference row)
  const maxDelete = Math.min(20, referenceRow - spliceIndex - 20);
  const deleteCount = rng.nextInt(0, maxDelete);

  // Insert count: 0 to 150 rows
  const insertCount = rng.nextInt(0, 150);

  // scrollOffset will be set by scrollToIndex, so just use 0 as placeholder
  const scrollOffset = 0;

  return {
    initialRows,
    scrollOffset,
    spliceIndex,
    deleteCount,
    insertCount,
    referenceRow,
  };
}

// ============================================================================
// Tests
// ============================================================================

test.describe('Tanstack Virtualizer Fuzz Tests', () => {
  test.setTimeout(120000);

  test.beforeEach(async ({page}) => {
    // Capture console logs from the browser
    page.on('console', msg => {
      const text = msg.text();
      if (
        text.includes('[SPLICE]') ||
        text.includes('[CORRECTION]') ||
        text.includes('[shouldAdjust]') ||
        text.includes('[ANCHOR]') ||
        text.includes('[RESTORE]')
      ) {
        console.log(`[Browser Console] ${text}`);
      }
    });

    await page.goto(`${BASE_URL}/tanstack-test`);

    // Disable browser scroll restoration
    await page.evaluate(() => {
      if ('scrollRestoration' in history) {
        history.scrollRestoration = 'manual';
      }
    });

    await page.waitForSelector('button:has-text("Reset")');
  });

  test('fuzz test - random splices should preserve scroll position', async ({
    page,
  }) => {
    const rng = new SeededRandom(SEED);
    const results: TestResult[] = [];

    console.log(`\n🎲 Starting fuzz test with seed: ${SEED}`);
    console.log(`Running ${NUM_FUZZ_TESTS} test cases...\n`);

    for (let i = 0; i < NUM_FUZZ_TESTS; i++) {
      const testCase = generateTestCase(rng);

      console.log(
        `Test ${i + 1}/${NUM_FUZZ_TESTS}: rows=${testCase.initialRows} ` +
          `splice(${testCase.spliceIndex},${testCase.deleteCount},${testCase.insertCount}) ref=Row${testCase.referenceRow}`,
      );

      try {
        await resetList(page);
        await scrollToRow(page, testCase.referenceRow);

        const beforePosition = await getRowPosition(
          page,
          testCase.referenceRow,
        );
        if (beforePosition === null) {
          throw new Error(
            `Row ${testCase.referenceRow} not found before splice`,
          );
        }

        await performSplice(
          page,
          testCase.spliceIndex,
          testCase.deleteCount,
          testCase.insertCount,
        );

        const afterPosition = await getRowPosition(page, testCase.referenceRow);
        if (afterPosition === null) {
          console.log(
            `  ⚠️  Row ${testCase.referenceRow} not found after splice`,
          );
          results.push({
            case: i + 1,
            test: testCase,
            passed: false,
            error: `Row ${testCase.referenceRow} not found after splice`,
          });
          continue;
        }

        const positionDiff = Math.abs(afterPosition - beforePosition);
        const passed = positionDiff <= POSITION_TOLERANCE;

        if (passed) {
          console.log(
            `  ✅ PASS: position ${afterPosition}px (expected ${beforePosition}px, diff ${positionDiff}px)`,
          );
        } else {
          console.log(
            `  ❌ FAIL: position ${afterPosition}px (expected ${beforePosition}px, diff ${positionDiff}px)`,
          );
        }

        results.push({
          case: i + 1,
          test: testCase,
          passed,
          actualPosition: afterPosition,
          expectedPosition: beforePosition,
        });
      } catch (error) {
        console.log(`  ❌ ERROR: ${error}`);
        results.push({
          case: i + 1,
          test: testCase,
          passed: false,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }

    // Summary
    const passed = results.filter(r => r.passed).length;
    const failed = results.filter(r => !r.passed).length;
    const successRate = ((passed / results.length) * 100).toFixed(1);

    console.log(`\n${'='.repeat(60)}`);
    console.log(`📊 FUZZ TEST SUMMARY`);
    console.log(`${'='.repeat(60)}`);
    console.log(`Seed: ${SEED}`);
    console.log(`Total tests: ${results.length}`);
    console.log(`Passed: ${passed} (${successRate}%)`);
    console.log(`Failed: ${failed}`);
    console.log(`${'='.repeat(60)}\n`);

    if (failed > 0) {
      console.log('Failed test cases:');
      results
        .filter(r => !r.passed)
        .forEach(r => {
          console.log(
            `  Case ${r.case}: splice(${r.test.spliceIndex},${r.test.deleteCount},${r.test.insertCount}) ` +
              `ref=Row${r.test.referenceRow} - ` +
              `${r.error || `${r.actualPosition}px vs ${r.expectedPosition}px`}`,
          );
        });
    }

    expect(passed / results.length).toBeGreaterThanOrEqual(1.0);
  });

  test('repeated splices should not accumulate drift', async ({page}) => {
    console.log('\n🔄 Testing repeated splices for drift accumulation...\n');

    const referenceRow = 50;
    await resetList(page);
    await scrollToRow(page, referenceRow);

    const initialPosition = await getRowPosition(page, referenceRow);
    expect(initialPosition).not.toBeNull();
    console.log(
      `Initial position of Row ${referenceRow}: ${initialPosition}px`,
    );

    const numSplices = 20;
    for (let i = 0; i < numSplices; i++) {
      await performSplice(page, 0, 1, 2, 300);

      const position = await getRowPosition(page, referenceRow);
      if (position === null) {
        console.log(`  Splice ${i + 1}: Row ${referenceRow} not found!`);
        continue;
      }

      const drift = position - initialPosition!;
      console.log(
        `  Splice ${i + 1}: position=${position}px, drift=${drift}px`,
      );
    }

    const finalPosition = await getRowPosition(page, referenceRow);
    const totalDrift = Math.abs(finalPosition! - initialPosition!);
    console.log(`\nFinal drift after ${numSplices} splices: ${totalDrift}px`);

    expect(totalDrift).toBeLessThanOrEqual(POSITION_TOLERANCE);
  });

  test('delete-only splices should not cause drift', async ({page}) => {
    console.log('\n🗑️ Testing delete-only splices...\n');

    const referenceRow = 100;
    await resetList(page);
    await scrollToRow(page, referenceRow);

    const initialPosition = await getRowPosition(page, referenceRow);
    expect(initialPosition).not.toBeNull();
    console.log(
      `Initial position of Row ${referenceRow}: ${initialPosition}px`,
    );

    for (let i = 0; i < 5; i++) {
      await performSplice(page, 0, 10, 0, 300);

      const position = await getRowPosition(page, referenceRow);
      if (position === null) {
        console.log(`  Delete ${i + 1}: Row ${referenceRow} not found!`);
        continue;
      }

      const drift = Math.abs(position - initialPosition!);
      console.log(
        `  Delete ${i + 1}: position=${position}px, drift=${drift}px`,
      );
      expect(drift).toBeLessThanOrEqual(POSITION_TOLERANCE);
    }
  });

  test('insert-only splices should not cause drift', async ({page}) => {
    console.log('\n➕ Testing insert-only splices...\n');

    const referenceRow = 100;
    await resetList(page);
    await scrollToRow(page, referenceRow);

    const initialPosition = await getRowPosition(page, referenceRow);
    expect(initialPosition).not.toBeNull();
    console.log(
      `Initial position of Row ${referenceRow}: ${initialPosition}px`,
    );

    for (let i = 0; i < 5; i++) {
      await performSplice(page, 0, 0, 50, 500);

      const position = await getRowPosition(page, referenceRow);
      if (position === null) {
        console.log(`  Insert ${i + 1}: Row ${referenceRow} not found!`);
        continue;
      }

      const drift = Math.abs(position - initialPosition!);
      console.log(
        `  Insert ${i + 1}: position=${position}px, drift=${drift}px`,
      );
      expect(drift).toBeLessThanOrEqual(POSITION_TOLERANCE);
    }
  });

  test('splice AFTER anchor should not move viewport', async ({page}) => {
    console.log('\n⬇️ Testing splice after anchor (should be no-op)...\n');

    const referenceRow = 100;
    await resetList(page);
    await scrollToRow(page, referenceRow);

    const initialScrollTop = await getScrollTop(page);
    console.log(`Initial scrollTop: ${initialScrollTop}px`);

    // Splice at index 500 (well after the anchor at ~100)
    await performSplice(page, 500, 10, 50, 300);

    const finalScrollTop = await getScrollTop(page);
    const scrollDelta = Math.abs(finalScrollTop - initialScrollTop);
    console.log(
      `Final scrollTop: ${finalScrollTop}px, delta: ${scrollDelta}px`,
    );

    expect(scrollDelta).toBeLessThanOrEqual(POSITION_TOLERANCE);
  });

  test('splice at scroll position 0 should work correctly', async ({page}) => {
    console.log('\n🔝 Testing splice at scroll position 0...\n');

    await resetList(page);
    // Stay at the top (don't scroll)

    const initialPosition = await getRowPosition(page, 0);
    console.log(`Initial position of Row 0: ${initialPosition}px`);

    await performSplice(page, 0, 0, 10, 500);

    const finalPosition = await getRowPosition(page, 0);
    console.log(`Final position of Row 0: ${finalPosition}px`);

    if (initialPosition !== null && finalPosition !== null) {
      const drift = Math.abs(finalPosition - initialPosition);
      console.log(`Drift: ${drift}px`);
      expect(drift).toBeLessThanOrEqual(POSITION_TOLERANCE);
    }
  });

  test('large batch insert should stabilize within maxAttempts', async ({
    page,
  }) => {
    console.log('\n📦 Testing large batch insert stabilization...\n');

    const referenceRow = 200;
    await resetList(page);
    await scrollToRow(page, referenceRow);

    const initialPosition = await getRowPosition(page, referenceRow);
    expect(initialPosition).not.toBeNull();
    console.log(
      `Initial position of Row ${referenceRow}: ${initialPosition}px`,
    );

    // Insert 200 rows - requires multiple correction passes
    await performSplice(page, 0, 0, 200, 1000);

    const finalPosition = await getRowPosition(page, referenceRow);
    console.log(`Final position: ${finalPosition}px`);

    if (initialPosition !== null && finalPosition !== null) {
      const drift = Math.abs(finalPosition - initialPosition);
      console.log(`Drift: ${drift}px`);
      expect(drift).toBeLessThanOrEqual(POSITION_TOLERANCE);
    }
  });

  test('rapid successive splices should not cause issues', async ({page}) => {
    console.log('\n⚡ Testing rapid successive splices...\n');

    const referenceRow = 100;
    await resetList(page);
    await scrollToRow(page, referenceRow);

    const initialPosition = await getRowPosition(page, referenceRow);
    expect(initialPosition).not.toBeNull();
    console.log(
      `Initial position of Row ${referenceRow}: ${initialPosition}px`,
    );

    // Rapid fire 10 splices with minimal wait
    for (let i = 0; i < 10; i++) {
      await performSplice(page, 0, 1, 1, 50);
    }

    // Wait for things to settle
    await page.waitForTimeout(500);

    const finalPosition = await getRowPosition(page, referenceRow);
    console.log(`Final position: ${finalPosition}px`);

    if (initialPosition !== null && finalPosition !== null) {
      const drift = Math.abs(finalPosition - initialPosition);
      console.log(`Drift after rapid splices: ${drift}px`);
      expect(drift).toBeLessThanOrEqual(POSITION_TOLERANCE);
    }
  });

  test('resize row above viewport should preserve scroll position', async ({
    page,
  }) => {
    console.log('\n📏 Testing resize row above viewport...\n');

    const referenceRow = 100;
    await resetList(page);
    await scrollToRow(page, referenceRow);

    const initialPosition = await getRowPosition(page, referenceRow);
    expect(initialPosition).not.toBeNull();
    console.log(
      `Initial position of Row ${referenceRow}: ${initialPosition}px`,
    );

    // Resize a row above the viewport (row 10)
    const resizeRowIndex = 10;
    console.log(`Resizing Row ${resizeRowIndex} from multiplier 1 to 20...`);

    // First resize to small, then to large
    await page.evaluate(
      ({rowIndex, multiplier}) => {
        // oxlint-disable-next-line no-explicit-any
        (window as any).resizeRow(rowIndex, multiplier);
      },
      {rowIndex: resizeRowIndex, multiplier: 20},
    );
    await page.waitForTimeout(300);

    const afterPosition = await getRowPosition(page, referenceRow);
    console.log(`Position after resize: ${afterPosition}px`);

    if (initialPosition !== null && afterPosition !== null) {
      const drift = Math.abs(afterPosition - initialPosition);
      console.log(`Drift: ${drift}px`);
      expect(drift).toBeLessThanOrEqual(POSITION_TOLERANCE);
    }
  });

  test('resize row in overscan above viewport should preserve scroll position', async ({
    page,
  }) => {
    console.log(
      '\n📏 Testing resize row in overscan (above viewport) should preserve scroll position...\n',
    );

    // Scroll to row 10, with overscan=5 the visible items include rows ~5-15
    // We'll resize one of the overscan rows above the viewport
    const referenceRow = 10;
    await resetList(page);
    await scrollToRow(page, referenceRow);
    await page.waitForTimeout(200);

    // Find the first visible row position
    const initialPosition = await getRowPosition(page, referenceRow);
    expect(initialPosition).not.toBeNull();
    console.log(
      `Initial position of Row ${referenceRow}: ${initialPosition}px`,
    );

    // Get the virtual items to see what's in overscan
    const virtualItemsInfo = await page.evaluate(() => {
      // oxlint-disable-next-line no-explicit-any
      const v = (window as any).virtualizer;
      if (!v) return null;
      const items = v.getVirtualItems();
      return items.map(
        (item: {index: number; start: number; size: number}) => ({
          index: item.index,
          start: item.start,
          size: item.size,
        }),
      );
    });
    console.log('Virtual items:', JSON.stringify(virtualItemsInfo));

    // Resize a row that's in overscan above the viewport
    // With overscan=5 and scrolled to row 10, row 5-9 should be in overscan above
    const resizeRowIndex = 7;
    console.log(
      `Resizing Row ${resizeRowIndex} (in overscan above viewport) from multiplier 1 to 30...`,
    );

    await page.evaluate(
      ({rowIndex, multiplier}) => {
        // oxlint-disable-next-line no-explicit-any
        (window as any).resizeRow(rowIndex, multiplier);
      },
      {rowIndex: resizeRowIndex, multiplier: 30},
    );
    await page.waitForTimeout(500);

    const afterPosition = await getRowPosition(page, referenceRow);
    console.log(`Position after resize: ${afterPosition}px`);

    if (initialPosition !== null && afterPosition !== null) {
      const drift = Math.abs(afterPosition - initialPosition);
      console.log(`Drift: ${drift}px`);
      expect(drift).toBeLessThanOrEqual(POSITION_TOLERANCE);
    }
  });

  test('multiple resizes above viewport should not accumulate drift', async ({
    page,
  }) => {
    console.log(
      '\n🔄 Testing multiple resizes above viewport for drift accumulation...\n',
    );

    const referenceRow = 50;
    await resetList(page);
    await scrollToRow(page, referenceRow);

    const initialPosition = await getRowPosition(page, referenceRow);
    expect(initialPosition).not.toBeNull();
    console.log(
      `Initial position of Row ${referenceRow}: ${initialPosition}px`,
    );

    // Resize multiple rows above the viewport
    for (let i = 0; i < 5; i++) {
      const resizeRowIndex = i * 5; // Rows 0, 5, 10, 15, 20
      const multiplier = 10 + i * 5; // Multipliers 10, 15, 20, 25, 30

      console.log(
        `  Resize ${i + 1}: Row ${resizeRowIndex} to multiplier ${multiplier}`,
      );
      await page.evaluate(
        ({rowIndex, mult}) => {
          // oxlint-disable-next-line no-explicit-any
          (window as any).resizeRow(rowIndex, mult);
        },
        {rowIndex: resizeRowIndex, mult: multiplier},
      );
      await page.waitForTimeout(300);

      const position = await getRowPosition(page, referenceRow);
      if (position !== null && initialPosition !== null) {
        const drift = Math.abs(position - initialPosition);
        console.log(`    Position: ${position}px, drift: ${drift}px`);
      }
    }

    const finalPosition = await getRowPosition(page, referenceRow);
    if (initialPosition !== null && finalPosition !== null) {
      const totalDrift = Math.abs(finalPosition - initialPosition);
      console.log(`\nTotal drift after 5 resizes: ${totalDrift}px`);
      expect(totalDrift).toBeLessThanOrEqual(POSITION_TOLERANCE);
    }
  });

  test('resize row below viewport should not affect scroll position', async ({
    page,
  }) => {
    console.log(
      '\n⬇️ Testing resize row below viewport (should be no-op)...\n',
    );

    const referenceRow = 100;
    await resetList(page);
    await scrollToRow(page, referenceRow);

    const initialPosition = await getRowPosition(page, referenceRow);
    expect(initialPosition).not.toBeNull();
    console.log(
      `Initial position of Row ${referenceRow}: ${initialPosition}px`,
    );

    // Resize a row below the viewport (row 500)
    const resizeRowIndex = 500;
    console.log(
      `Resizing Row ${resizeRowIndex} (below viewport) to multiplier 30...`,
    );

    await page.evaluate(
      ({rowIndex, multiplier}) => {
        // oxlint-disable-next-line no-explicit-any
        (window as any).resizeRow(rowIndex, multiplier);
      },
      {rowIndex: resizeRowIndex, multiplier: 30},
    );
    await page.waitForTimeout(300);

    const afterPosition = await getRowPosition(page, referenceRow);
    console.log(`Position after resize: ${afterPosition}px`);

    if (initialPosition !== null && afterPosition !== null) {
      const drift = Math.abs(afterPosition - initialPosition);
      console.log(`Drift: ${drift}px`);
      expect(drift).toBeLessThanOrEqual(POSITION_TOLERANCE);
    }
  });
});
