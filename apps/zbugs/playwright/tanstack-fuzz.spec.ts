import {expect, test} from '@playwright/test';

const BASE_URL = process.env.URL ?? 'http://localhost:5173';
const NUM_FUZZ_TESTS = parseInt(process.env.NUM_FUZZ_TESTS ?? '50');
const SEED = parseInt(process.env.SEED ?? Date.now().toString());

// Simple seeded random number generator
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

interface TestCase {
  initialRows: number;
  scrollOffset: number;
  spliceIndex: number;
  deleteCount: number;
  insertCount: number;
  referenceRow: number;
}

function generateTestCase(rng: SeededRandom, caseNum: number): TestCase {
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

test.describe('Tanstack Virtualizer Fuzz Tests', () => {
  test.setTimeout(120000);

  test.beforeEach(async ({page}) => {
    // Capture console logs from the browser
    page.on('console', msg => {
      const text = msg.text();
      if (
        text.includes('[SPLICE]') ||
        text.includes('[CORRECTION]') ||
        text.includes('[shouldAdjust]')
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

  test('fuzz test', async ({page}) => {
    const testSeed = SEED;
    const rng = new SeededRandom(testSeed);
    const results: Array<{
      case: number;
      test: TestCase;
      passed: boolean;
      error?: string;
      actualPosition?: number;
      expectedPosition?: number;
    }> = [];

    console.log(`\n🎲 Starting fuzz test with seed: ${testSeed}`);
    console.log(`Running ${NUM_FUZZ_TESTS} test cases...\n`);

    for (let i = 0; i < NUM_FUZZ_TESTS; i++) {
      const testCase = generateTestCase(rng, i);

      console.log(
        `Test ${i + 1}/${NUM_FUZZ_TESTS}: rows=${testCase.initialRows} ` +
          `splice(${testCase.spliceIndex},${testCase.deleteCount},${testCase.insertCount}) ref=Row${testCase.referenceRow}`,
      );

      try {
        // Reset to 1000 rows
        await page.click('button:has-text("Reset")');
        await page.waitForTimeout(200);

        // Scroll to the reference row using scrollToIndex
        await page.evaluate(refRow => {
          const scrollDivs = Array.from(document.querySelectorAll('div'));
          const listContainer = scrollDivs.find(
            d =>
              (d as HTMLElement).style.overflow === 'auto' &&
              (d as HTMLElement).style.position === 'relative',
          ) as HTMLElement;

          if (listContainer && (window as any).virtualizer) {
            (window as any).virtualizer.scrollToIndex(refRow, {
              align: 'start',
              behavior: 'auto',
            });
          }
        }, testCase.referenceRow);

        await page.waitForTimeout(200);

        // Get reference row position before splice
        const beforePosition = await page.evaluate(refRow => {
          const scrollDivs = Array.from(document.querySelectorAll('div'));
          const listContainer = scrollDivs.find(
            d =>
              (d as HTMLElement).style.overflow === 'auto' &&
              (d as HTMLElement).style.position === 'relative',
          ) as HTMLElement;

          if (!listContainer) return {error: 'No container'};

          const items = Array.from(document.querySelectorAll('[data-index]'));
          const refItem = items.find(el =>
            el.textContent?.includes(`Row ${refRow}`),
          ) as HTMLElement;

          if (!refItem) return {error: `Row ${refRow} not found`};

          const rect = refItem.getBoundingClientRect();
          const containerRect = listContainer.getBoundingClientRect();
          const positionInViewport = rect.top - containerRect.top;

          return {
            position: Math.round(positionInViewport),
            scrollTop: listContainer.scrollTop,
          };
        }, testCase.referenceRow);

        if ('error' in beforePosition) {
          throw new Error(beforePosition.error);
        }

        const expectedPosition = beforePosition.position;

        // Perform splice
        await page
          .locator('text=Index:')
          .locator('..')
          .locator('input')
          .fill(testCase.spliceIndex.toString());
        await page
          .locator('text=Delete Count:')
          .locator('..')
          .locator('input')
          .fill(testCase.deleteCount.toString());
        await page
          .locator('text=Insert Count:')
          .locator('..')
          .locator('input')
          .fill(testCase.insertCount.toString());
        await page.waitForTimeout(50);

        await page.click('button:has-text("Splice")');

        await page.waitForTimeout(500);

        // Get reference row position after splice
        const afterPosition = await page.evaluate(refRow => {
          const scrollDivs = Array.from(document.querySelectorAll('div'));
          const listContainer = scrollDivs.find(
            d =>
              (d as HTMLElement).style.overflow === 'auto' &&
              (d as HTMLElement).style.position === 'relative',
          ) as HTMLElement;

          if (!listContainer) return {error: 'No container'};

          const items = Array.from(document.querySelectorAll('[data-index]'));
          const refItem = items.find(el =>
            el.textContent?.includes(`Row ${refRow}`),
          ) as HTMLElement;

          if (!refItem) {
            return {
              error: `Row ${refRow} not found after splice`,
              totalRows: items.length,
              scrollTop: listContainer.scrollTop,
            };
          }

          const rect = refItem.getBoundingClientRect();
          const containerRect = listContainer.getBoundingClientRect();
          const positionInViewport = rect.top - containerRect.top;

          return {
            position: Math.round(positionInViewport),
            scrollTop: listContainer.scrollTop,
            index: parseInt(refItem.getAttribute('data-index') || '-1'),
          };
        }, testCase.referenceRow);

        if ('error' in afterPosition) {
          // Reference row might have been deleted or moved out of viewport
          console.log(`  ⚠️  ${afterPosition.error}`);
          results.push({
            case: i + 1,
            test: testCase,
            passed: false,
            error: afterPosition.error,
          });
          continue;
        }

        const actualPosition = afterPosition.position;
        const positionDiff = Math.abs(actualPosition - expectedPosition);

        const scrollDiff = afterPosition.scrollTop - beforePosition.scrollTop;

        // Allow up to 5px tolerance for rounding errors
        const passed = positionDiff <= 5;

        if (passed) {
          console.log(
            `  ✅ PASS: position ${actualPosition}px (expected ${expectedPosition}px, diff ${positionDiff}px)`,
          );
        } else {
          console.log(
            `  ❌ FAIL: position ${actualPosition}px (expected ${expectedPosition}px, diff ${positionDiff}px)`,
          );
          console.log(
            `    scrollTop: ${beforePosition.scrollTop}px → ${afterPosition.scrollTop}px (delta ${scrollDiff}px)`,
          );
        }

        results.push({
          case: i + 1,
          test: testCase,
          passed,
          actualPosition,
          expectedPosition,
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
    console.log(`Seed: ${testSeed}`);
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
            `  Case ${r.case}: rows=${r.test.initialRows} scroll=${r.test.scrollOffset} ` +
              `splice(${r.test.spliceIndex},${r.test.deleteCount},${r.test.insertCount}) ` +
              `${r.error || `position ${r.actualPosition}px vs ${r.expectedPosition}px`}`,
          );
        });
    }

    // Fail the test if success rate is below 95%
    expect(passed / results.length).toBeGreaterThanOrEqual(0.95);
  });
});
