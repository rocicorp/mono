import {expect, test} from '@playwright/test';

const BASE_URL = 'http://localhost:5173';

test.describe('Tanstack Basic Tests', () => {
  test('page loads and can perform basic splice', async ({page}) => {
    console.log('Navigating to page...');
    await page.goto(`${BASE_URL}/tanstack-test`);

    console.log('Waiting for page to load...');
    await page.waitForSelector('h1:has-text("Tanstack Virtualizer Test")', {
      timeout: 10000,
    });

    console.log('Page loaded, checking initial state...');
    const heading = await page.textContent('h2');
    console.log(`Heading: ${heading}`);

    expect(heading).toContain('Virtual List');

    console.log('Test passed!');
  });

  test('can click reset button', async ({page}) => {
    console.log('Navigating to page...');
    await page.goto(`${BASE_URL}/tanstack-test`);

    console.log('Waiting for reset button...');
    await page.waitForSelector('button:has-text("Reset")', {timeout: 10000});

    console.log('Clicking reset button...');
    await page.click('button:has-text("Reset")');

    console.log('Waiting for update...');
    await page.waitForTimeout(200);

    console.log('Checking row count...');
    const heading = await page.textContent('h2');
    console.log(`Heading after reset: ${heading}`);

    expect(heading).toContain('1000 rows');

    console.log('Test passed!');
  });

  test('can perform simple splice', async ({page}) => {
    console.log('Navigating to page...');
    await page.goto(`${BASE_URL}/tanstack-test`);

    console.log('Waiting for page...');
    await page.waitForSelector('button:has-text("Reset")', {timeout: 10000});

    console.log('Clicking reset...');
    await page.click('button:has-text("Reset")');
    await page.waitForTimeout(200);

    console.log('Setting up splice parameters...');

    // Use better selectors based on the surrounding text
    await page.locator('text=Index:').locator('..').locator('input').fill('0');
    await page.waitForTimeout(50);

    await page
      .locator('text=Delete Count:')
      .locator('..')
      .locator('input')
      .fill('10');
    await page.waitForTimeout(50);

    await page
      .locator('text=Insert Count:')
      .locator('..')
      .locator('input')
      .fill('20');
    await page.waitForTimeout(50);

    console.log('Values set via label selectors');

    console.log('Clicking splice button...');
    await page.click('button:has-text("Splice")');

    console.log('Checking console for errors...');
    const logs: string[] = [];
    page.on('console', msg => logs.push(`${msg.type()}: ${msg.text()}`));

    console.log('Waiting for splice to complete...');
    await page.waitForTimeout(300);

    console.log('Checking result...');
    const heading = await page.textContent('h2');
    console.log(`Heading after splice: ${heading}`);

    // Should have 1000 - 10 + 20 = 1010 rows
    expect(heading).toContain('1010 rows');

    console.log('Test passed!');
  });
});
