import {test} from '@playwright/test';

test('loadtest', async ({page}) => {
  const testID = Math.random().toString(36).substring(2, 8);

  await page.goto('https://zbugs.vercel.app/');
  await page.getByLabel('VISITOR PASSWORD').click();
  await page.getByLabel('VISITOR PASSWORD').fill('zql');
  await page.getByLabel('VISITOR PASSWORD').press('Enter');

  const delay = Math.random() * 5000;
  console.log(testID, `Delaying for ${delay}ms to create jitter`);
  await page.waitForTimeout(delay);

  const cgID = await page.evaluate('window.z.clientGroupID');
  for (let i = 0; i < 10; i++) {
    console.log(cgID, `Iteration: ${i}`);
    await page.waitForSelector('.issue-list .row');
    console.log(cgID, 'Filtering by open');
    await page.locator('.nav-item', {hasText: 'Open'}).click();
    console.log(cgID, 'Filtering by closed');
    await page.locator('.nav-item', {hasText: 'Closed'}).click();
    console.log(cgID, 'Filtering by all');
    await page.locator('.nav-item', {hasText: 'All'}).click();
    await page.locator('.add-filter').click();
    await page.getByText('Filtered by:+ Filter').click();
    await page.getByRole('button', {name: '+ Filter'}).click();
    await page
      .locator('div')
      .filter({hasText: /^CreatorSelect$/})
      .getByRole('button')
      .click();
    let elm = await page.locator(
      `.filter-modal-item:nth-child(1) .dropdown button:nth-child(${
        Math.floor(Math.random() * 10) + 1
      })`,
    );
    console.log(cgID, `Filtering by ${await elm.allTextContents()}`);
    await elm.click();

    await page.getByRole('button', {name: '+ Filter'}).click();
    await page
      .locator('div')
      .filter({hasText: /^LabelSelect$/})
      .getByRole('button')
      .click();
    elm = await page.locator(
      `.filter-modal-item:nth-child(2) .dropdown button:nth-child(${
        Math.floor(Math.random() * 10) + 1
      })`,
    );
    console.log(cgID, `Filtering by ${await elm.allTextContents()}`);
    await elm.click();
    console.log(cgID, 'Removing user filter');
    await page.locator('.list-view-filter-container .pill.user').click();
    console.log(cgID, 'Removing label filter');
    await page.locator('.list-view-filter-container .pill.label').click();
    await page.locator('.nav-item', {hasText: 'All'}).click();
    await page
      .locator(
        `.issue-list .row:nth-child(${Math.floor(Math.random() * 5) + 1})`,
      )
      .click();
    await page
      .locator('.issue-sidebar .sidebar-item:first-child button')
      .click();
    await page
      .locator(
        '.issue-sidebar .sidebar-item:first-child .dropdown button:nth-child(2)',
      )
      .click();
    await page
      .locator('.issue-sidebar .sidebar-item:first-child button')
      .click();
    await page
      .locator(
        '.issue-sidebar .sidebar-item:first-child .dropdown button:nth-child(2)',
      )
      .click();
    await page.goBack();
  }
  console.log(cgID, `Done`);
});
