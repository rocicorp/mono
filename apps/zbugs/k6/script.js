import {browser} from 'k6/browser';
import {sleep} from 'k6';

export const options = {
  cloud: {
    // Project: Default project
    projectID: 3716849,
    // Test runs with the same name groups test runs together.
    name: 'Test (26/09/2024-22:10:04)',
  },
  scenarios: {
    ui: {
      executor: 'shared-iterations',
      vus: 10,
      iterations: 10,
      options: {
        browser: {
          type: 'chromium',
        },
      },
    },
  },
  thresholds: {
    checks: ['rate==1.0'],
  },
};

export default async function () {
  const context = await browser.newContext();
  const page = await context.newPage();

  try {
    await page.goto('https://zbugs.vercel.app/?keepalive=1');
    await page.locator('input[name="_vercel_password"]').type('zql');

    console.info('entered password');

    await page.locator('button.submit').click();
    await page.waitForSelector('.issue-list .row');

    console.info('got through');

    const delay = 0.2;

    for (let i = 0; i < 10; i++) {
      // We disconnect the socket after a certain amount of time in the
      // background, so bring to foreground periodically.
      await page.bringToFront();
      console.log('bringing to front');

      await sleep(delay);
      await page.locator('.nav-item:nth-child(2)').click();

      await sleep(delay);
      await page.locator('.nav-item:nth-child(3)').click();

      await sleep(delay);
      await page.locator('.nav-item:nth-child(1)').click();

      await sleep(delay);
      await page.locator('.add-filter').click();

      await sleep(delay);
      await page
        .locator('.add-filter-modal .filter-modal-item:nth-child(1) button')
        .click();

      await sleep(delay);
      await page
        .locator(
          `.add-filter-modal .filter-modal-item:nth-child(1) .dropdown .item:nth-child(${
            Math.floor(Math.random() * 13) + 1
          })`,
        )
        .click();

      await sleep(delay);
      await page.locator('.add-filter').click();

      await sleep(delay);
      await page
        .locator('.add-filter-modal .filter-modal-item:nth-child(2) button')
        .click();

      await sleep(delay);
      await page
        .locator(
          `.add-filter-modal .filter-modal-item:nth-child(2) .dropdown .item:nth-child(${
            Math.floor(Math.random() * 13) + 1
          })`,
        )
        .click();

      await sleep(delay);
      await page.locator('.list-view-filter-container .pill.user').click();

      await sleep(delay);
      await page.locator('.list-view-filter-container .pill.label').click();

      await sleep(delay);
      await page
        .locator(
          `.issue-list .row:nth-child(${Math.floor(
            Math.random() * 10 + 1,
          )}) .issue-title`,
        )
        .click();

      await sleep(delay);
      await page
        .locator('.issue-sidebar .sidebar-item:first-child .selector button')
        .click();

      await sleep(delay);
      await page
        .locator(
          '.issue-sidebar .sidebar-item:first-child .selector .dropdown button:nth-child(2)',
        )
        .click();

      await sleep(delay);
      await page
        .locator('.issue-sidebar .sidebar-item:first-child .selector button')
        .click();

      await sleep(delay);
      await page
        .locator(
          '.issue-sidebar .sidebar-item:first-child .selector .dropdown button:nth-child(2)',
        )
        .click();

      await sleep(delay);
      await page.mainFrame().evaluate('() => window.history.back()');
    }
  } finally {
    await page.close();
  }
}
