import {test} from '@playwright/test';

const userCookies = [
  'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJPZVZucjF5NWJFTV9ZZzA2c1VGdEQiLCJpYXQiOjE3MzQxMzY3NDYsInJvbGUiOiJjcmV3IiwibmFtZSI6ImFib29kbWFuIiwiZXhwIjoxNzM2NzI4NzQ2fQ.muDyQMOsjYi--80bl3kxyxzIHmZbA1lCdsK6z3B58LI',
];

const DELAY_START = parseInt(process.env.DELAY_START ?? '0');
const DELAY_PER_ITERATION = parseInt(process.env.DELAY_PER_ITERATION ?? '4800');
const NUM_ITERATIONS = parseInt(process.env.NUM_ITERATIONS ?? '10');
const SITE_URL = process.env.URL ?? 'https://bugs-sandbox.rocicorp.dev';
const ISSUE_ID = process.env.ISSUE_ID ?? '175';
const DIRECT_URL = process.env.DIRECT_URL ?? `${SITE_URL}/issue/${ISSUE_ID}`;
const PERCENT_DIRECT = parseFloat(process.env.PERCENT_DIRECT ?? '0.75');
const AWS_BATCH_JOB_ARRAY_INDEX = process.env.AWS_BATCH_JOB_ARRAY_INDEX ?? '-1';
const ENTER_PASSWORD = process.env.ENTER_PASSWORD === '1';

test('loadtest', async ({page, browser, context}) => {
  // print environment variables
  console.log(process.env);
  test.setTimeout(700000);
  await page.context().addCookies([
    {
      name: 'jwt',
      value: userCookies[Math.floor(Math.random() * userCookies.length)],
      domain: new URL(SITE_URL).host,
      path: '/',
      expires: -1,
      httpOnly: false,
    },
  ]);
  const testID = Math.random().toString(36).substring(2, 8);
  if (DELAY_START > 0) {
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
    console.log('Opening main page:', SITE_URL);
    await page.goto(SITE_URL);
  }
  if (ENTER_PASSWORD) {
    await page.getByLabel('VISITOR PASSWORD').click();
    await page.getByLabel('VISITOR PASSWORD').fill('zql');
    await page.getByLabel('VISITOR PASSWORD').press('Enter');
  }
  let cgID = '';
  const start = Date.now();
  // if it went to direct url, do this branch of code
  if (!wentDirect) {
    await page.waitForSelector('.issue-list .row');
    cgID = await page.evaluate('window.z.clientGroupID');
    console.log(
      AWS_BATCH_JOB_ARRAY_INDEX,
      cgID,
      `Start rendered in: ${Date.now() - start}ms`,
    );
  } else {
    await page.waitForSelector('[class^="_commentItem"]');
    cgID = await page.evaluate('window.z.clientGroupID');
    console.log(
      AWS_BATCH_JOB_ARRAY_INDEX,
      cgID,
      `Direct Issue Start rendered in: ${Date.now() - start}ms`,
    );
  }

  for (let i = 0; i < NUM_ITERATIONS; i++) {
    const iterationStart = Date.now();

    // navigate into test issue
    await openIssueByID(page, ISSUE_ID);

    // every other time comment on the issue
    if (i % 2 === 0) {
      await commentOnNewIssue(page, 'This is a test comment');
    }

    // do some filters
    console.log(AWS_BATCH_JOB_ARRAY_INDEX, cgID, 'Filtering by open');
    await page.locator('.nav-item', {hasText: 'Open'}).click();
    console.log(AWS_BATCH_JOB_ARRAY_INDEX, cgID, 'Filtering by closed');
    await page.locator('.nav-item', {hasText: 'Closed'}).click();
    console.log(AWS_BATCH_JOB_ARRAY_INDEX, cgID, 'Filtering by all');
    await page.locator('.nav-item', {hasText: 'All'}).click();

    // filter by creator and pick a random creator
    await page.locator('.add-filter').click();
    await page.getByText('Filtered by:+ Filter').click();
    await page.getByRole('button', {name: '+ Filter'}).click();
    await page.locator('div.add-filter-modal > div:nth-child(1)').click();

    let elm = await page.locator(
      `#options-listbox > li:nth-child(${Math.floor(Math.random() * 5) + 2})`,
    );

    console.log(
      AWS_BATCH_JOB_ARRAY_INDEX,
      cgID,
      `Filtering by ${await elm.allTextContents()}`,
    );
    await elm.click();

    // filter by assignee and pick a random assignee
    await page.getByRole('button', {name: '+ Filter'}).click();
    await page.locator('div.add-filter-modal > div:nth-child(2)').click();
    elm = await page.locator(
      `#options-listbox > li:nth-child(${Math.floor(Math.random() * 5) + 2})`,
    );

    console.log(
      AWS_BATCH_JOB_ARRAY_INDEX,
      cgID,
      `Filtering by ${await elm.allTextContents()}`,
    );
    await elm.click();

    // remove filters
    console.log(AWS_BATCH_JOB_ARRAY_INDEX, cgID, 'Removing user filter');
    await page
      .locator('.list-view-filter-container .pill.user')
      .first()
      .click();
    console.log(AWS_BATCH_JOB_ARRAY_INDEX, cgID, 'Removing label filter');
    await page.locator('.list-view-filter-container .pill.user').last().click();

    // show all issues
    await page.locator('.nav-item', {hasText: 'All'}).click();

    // scroll to bottom of page
    await page.evaluate(() => {
      window.scrollTo({
        top: document.body.scrollHeight + 1000000,
        behavior: 'smooth',
      });
    });

    console.log(
      AWS_BATCH_JOB_ARRAY_INDEX,
      cgID,
      `Finished iteration in ${Date.now() - iterationStart}ms`,
    );
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

async function openIssueByID(page, issueID: string): Promise<boolean> {
  await page.locator('.nav-item', {hasText: 'All'}).click();
  await waitForIssueList(page);

  await page
    .locator(`.issue-list .row a[href="/issue/${issueID}"]`)
    .first()
    .scrollIntoViewIfNeeded();
  await page
    .locator(`.issue-list .row a[href="/issue/${issueID}"]`)
    .first()
    .click();
  return true;
}
