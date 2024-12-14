import {test} from '@playwright/test';

const userCookies = [
  'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIwRnczbjZFUVM4bXpFMTI2QUZKeDEiLCJpYXQiOjE3MzM5NTkyMDksIm5hbWUiOiJyb2NpYm90MSIsInJvbGUiOiJjcmV3IiwiZXhwIjoxODIwMzU5MjEwfQ._KK8Zyf5qV6ICCR2qrPyh_-G15hTm_XKnXrzUKOlB28',
  'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIwRnczbjZFUVM4bXpFMTI2QUZKeDAiLCJpYXQiOjE3MzMyOTc5MTUsInJvbGUiOiJjcmV3IiwibmFtZSI6InJvY2lib3QiLCJleHAiOjE4MTk2OTc5MTV9.mmBeGC_r6y1p3YFqnMN5fRmwm5dBAOHBJVPVHIfOSNA',
  'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIwRnczbjZFUVM4bXpFMTI2QUZKeDIiLCJpYXQiOjE3MzM5NTkyNzUsIm5hbWUiOiJyb2NpYm90MiIsInJvbGUiOiJjcmV3IiwiZXhwIjoxODIwMzU5Mjc1fQ.qGQTHFmnPyfAu3xGlWyEuSREcnwcZCKwyiW9ckRrPZY',
  'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIwRnczbjZFUVM4bXpFMTI2QUZKeDMiLCJpYXQiOjE3MzM5NzQwMDIsIm5hbWUiOiJyb2NpYm90MyIsInJvbGUiOiJjcmV3IiwiZXhwIjoxODIwMzc0MDA0fQ.dpXsIDlMzNUlQpWY0c3Vh1hrBo36hNDmsXHyy59NhaQ',
  'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIwRnczbjZFUVM4bXpFMTI2QUZKeDQiLCJpYXQiOjE3MzM5NzM5NDUsIm5hbWUiOiJyb2NpYm90NCIsInJvbGUiOiJjcmV3IiwiZXhwIjoxODIwMzczOTQ1fQ.MDaVc59EXXDpiUbod2cJ3GwcJhAJ5KJa88CuuWT4P2o',
];

const DELAY_START = parseInt(process.env.DELAY_START ?? '0');
const DELAY_PER_ITERATION = parseInt(process.env.DELAY_PER_ITERATION ?? '4800');
const NUM_ITERATIONS = parseInt(process.env.NUM_ITERATIONS ?? '10');
const URL = process.env.URL ?? 'https://bugs-sandbox.rocicorp.dev';
const DIRECT_URL =
  process.env.DIRECT_URL ?? 'https://bugs-sandbox.rocicorp.dev/issue/3020';
const PERCENT_DIRECT = parseFloat(process.env.PERCENT_DIRECT ?? '0.75');
const AWS_BATCH_JOB_ARRAY_INDEX = process.env.AWS_BATCH_JOB_ARRAY_INDEX ?? '-1';

test('loadtest', async ({page, browser, context}) => {
  // print environment variables
  console.log(process.env);
  test.setTimeout(700000);
  await page.context().addCookies([
    {
      name: 'jwt',
      value: userCookies[Math.floor(Math.random() * userCookies.length)],
      domain: 'bugs-sandbox.rocicorp.dev',
      path: '/',
      expires: -1,
      httpOnly: false,
    },
  ]);
  const testID = Math.random().toString(36).substring(2, 8);
  if(DELAY_START > 0) {
    const delay = Math.random() * DELAY_START;
    console.log(`Delaying for ${delay}ms to create jitter`);
    await page.waitForTimeout(delay);
  }
  const random = Math.random();
  console.log(`Random: ${random}`);
  const wentDirect = random < PERCENT_DIRECT;
  if (wentDirect) {
    console.log('Opening direct issue:', DIRECT_URL);
    await page.goto(DIRECT_URL);
  } else {
    console.log('Opening main page:', URL);
    await page.goto(URL);
  }
  await page.getByLabel('VISITOR PASSWORD').click();
  await page.getByLabel('VISITOR PASSWORD').fill('zql');
  await page.getByLabel('VISITOR PASSWORD').press('Enter');
  let cgID = '';
  const start = Date.now();
  // if it went to direct url, do this branch of code
  if (!wentDirect) {
    await page.waitForSelector('.issue-list .row');
    cgID = await page.evaluate('window.z.clientGroupID');
    console.log(AWS_BATCH_JOB_ARRAY_INDEX, cgID,  `Start rendered in: ${Date.now() - start}ms`);
  } else {
    await page.waitForSelector('[class^="_commentItem"]');
    cgID = await page.evaluate('window.z.clientGroupID');
    console.log(AWS_BATCH_JOB_ARRAY_INDEX, cgID, `Direct Issue Start rendered in: ${Date.now() - start}ms`);
  }



  for (let i = 0; i < NUM_ITERATIONS; i++) {
    const iterationStart = Date.now();

    await openIssueByTitle(page, `Test Issue`);
    if (i % 2 === 0) {
      await commentOnNewIssue(page, 'This is a test comment');
    }
    console.log(AWS_BATCH_JOB_ARRAY_INDEX, cgID, 'Filtering by open');
    await page.locator('.nav-item', {hasText: 'Open'}).click();
    console.log(AWS_BATCH_JOB_ARRAY_INDEX, cgID, 'Filtering by closed');
    await page.locator('.nav-item', {hasText: 'Closed'}).click();
    console.log(AWS_BATCH_JOB_ARRAY_INDEX, cgID, 'Filtering by all');
    await page.locator('.nav-item', {hasText: 'All'}).click();
    await page.locator('.add-filter').click();
    await page.getByText('Filtered by:+ Filter').click();
    await page.getByRole('button', {name: '+ Filter'}).click();
    // select creator
    await page.locator('div.add-filter-modal > div:nth-child(1)').click();

    let elm = await page.locator(
      `#options-listbox > li:nth-child(${Math.floor(Math.random() * 5) + 2})`,
    );

    console.log(AWS_BATCH_JOB_ARRAY_INDEX, cgID, `Filtering by ${await elm.allTextContents()}`);
    await elm.click();
    await page.getByRole('button', {name: '+ Filter'}).click();
    // select assignee
    await page.locator('div.add-filter-modal > div:nth-child(2)').click();
    elm = await page.locator(
      `#options-listbox > li:nth-child(${Math.floor(Math.random() * 5) + 2})`,
    );

    console.log(AWS_BATCH_JOB_ARRAY_INDEX, cgID, `Filtering by ${await elm.allTextContents()}`);
    await elm.click();
    console.log(AWS_BATCH_JOB_ARRAY_INDEX, cgID, 'Removing user filter');
    await page
      .locator('.list-view-filter-container .pill.user')
      .first()
      .click();
    console.log(AWS_BATCH_JOB_ARRAY_INDEX, cgID, 'Removing label filter');
    await page.locator('.list-view-filter-container .pill.user').last().click();
    await page.locator('.nav-item', {hasText: 'All'}).click();
    await page.evaluate(() => {
      window.scrollTo({
        top: document.body.scrollHeight + 1000000,
        behavior: 'smooth',
      });
    });

    console.log(AWS_BATCH_JOB_ARRAY_INDEX, cgID, `Finished iteration in ${Date.now() - iterationStart}ms`);
    await page.goBack();
    await page.waitForTimeout(DELAY_PER_ITERATION);
  }
  await context.close();
  await page.close();
  await browser.close();
  let elapsed = Date.now() - start;
  elapsed = elapsed - DELAY_PER_ITERATION * NUM_ITERATIONS;
  console.log(
    `${cgID} loadtest completed in ${(elapsed / 1000).toFixed(2)} secs`,
  );
  console.log(testID, `Ending Test`);
  console.log(AWS_BATCH_JOB_ARRAY_INDEX, cgID, `Done`);
});

async function waitForIssueList(page) {
  await page.waitForFunction(() => {
    const issues = document.querySelectorAll('.issue-list .row');
    return issues.length > 1;
  });
}

async function checkIssueExists(page, title: string) {
  //scroll tho the end of issue list
  await page.evaluate(() => {
    window.scrollTo({
      top: document.body.scrollHeight + 1000000,
      behavior: 'smooth',
    });
  });
  return (
    (await page
      .locator('.issue-list .row', {
        hasText: title,
      })
      .count()) > 0
  );
}

async function navigateToAll(page) {
  await page.locator('.nav-item', {hasText: 'All'}).click();
}

// async function createNewIssueIfNotExists(page, title: string, description: string) {
//   await waitForIssueList(page);

//   if (!(await checkIssueExists(page, title))) {
//     console.log(`Creating new issue: ${title}`);
//     await page.getByRole('button', { name: 'New Issue' }).click();
//     await page.locator('.new-issue-title').fill(title);
//     await page.locator('.new-issue-description').fill(description);
//     await page.getByRole('button', { name: 'Save Issue' }).click();
//     await page.waitForSelector('.modal', { state: 'hidden' });
//   } else {
//     console.log(`Issue "${title}" already exists, skipping creation`);
//   }

//   await navigateToAll(page);
// }

async function selectRandomEmoji(page) {
  // Wait for the emoji menu to be visible
  await page.waitForSelector('div.emoji-menu button.emoji', {
    state: 'visible',
    timeout: 5000,
  });

  // Get all emoji buttons
  const emojiButtons = page.locator('div.emoji-menu button.emoji');
  const count = await emojiButtons.count();

  // Select a random emoji
  const randomIndex = Math.floor(Math.random() * count);
  await emojiButtons.nth(randomIndex).click();
}

async function commentOnNewIssue(page, comment: string) {
  await page.waitForSelector('[class^="_commentItem"]');
  const comments = page.locator('[class^="_commentItem"]');
  //add emoji first title
  await page.locator('.add-emoji-button').first().click();
  await selectRandomEmoji(page);

  // Make sure comment input is visible and fill it
  await page.locator('.comment-input').scrollIntoViewIfNeeded();
  await page.locator('.comment-input').click(); // Add this line to ensure focus
  await page.locator('.comment-input').fill(comment);

  // Wait for button to be enabled before clicking
  await page.getByRole('button', {name: 'Add comment'}).click();

  const commentCount = await comments.count();
  const randomCommentIndex = Math.floor(Math.random() * commentCount);

  await comments
    .nth(randomCommentIndex)
    .locator('.add-emoji-button')
    .scrollIntoViewIfNeeded();

  await await comments
    .nth(randomCommentIndex)
    .locator('.add-emoji-button')
    .first()
    .click();
  await selectRandomEmoji(page);
  await navigateToAll(page);
}

async function openIssueByTitle(page, issueTitle: string): Promise<boolean> {
  await page.locator('.nav-item', {hasText: 'All'}).click();
  await waitForIssueList(page);

  if (!(await checkIssueExists(page, issueTitle))) {
    console.log(`Issue "${issueTitle}" does not exist, skipping`);
    return false;
  }

  await page
    .locator('.issue-list .row', {hasText: issueTitle})
    .first()
    .scrollIntoViewIfNeeded();
  await page.locator('.issue-list .row', {hasText: issueTitle}).first().click();
  return true;
}
