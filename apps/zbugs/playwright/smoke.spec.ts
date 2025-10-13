// oxlint-disable no-explicit-any
// oxlint-disable no-console
import {test} from '@playwright/test';
const userCookies = process.env.USER_COOKIES
  ? JSON.parse(process.env.USER_COOKIES)
  : [];

const DELAY_START = parseInt(process.env.DELAY_START ?? '0');
const DELAY_PER_ITERATION = parseInt(process.env.DELAY_PER_ITERATION ?? '4800');
const NUM_ITERATIONS = parseInt(process.env.NUM_ITERATIONS ?? '10');
const SITE_URL = process.env.URL ?? 'https://bugs-sandbox.rocicorp.dev';
const ISSUE_ID = process.env.ISSUE_ID ?? '3020';
const DIRECT_URL = process.env.DIRECT_URL ?? `${SITE_URL}/issue/${ISSUE_ID}`;
const PERCENT_DIRECT = parseFloat(process.env.PERCENT_DIRECT ?? '0.10');
const AWS_BATCH_JOB_ARRAY_INDEX = process.env.AWS_BATCH_JOB_ARRAY_INDEX ?? '-1';
const NO_JWT = userCookies.length === 0;
const ADD_COMMENTS_AND_EMOJI =
  (process.env.ADD_COMMENTS_AND_EMOJI ?? '1') === '1';
test('loadtest', async ({page, browser, context}) => {
  test.setTimeout(700000);
  if (!NO_JWT) {
    await page.context().addCookies([
      {
        name: 'jwt',
        value: userCookies[Math.floor(Math.random() * userCookies.length)],
        domain: new URL(SITE_URL).host,
        path: '/',
        expires: -1,
        httpOnly: false,
      },
      {
        name: 'onboardingDismissed',
        value: 'true',
        domain: new URL(SITE_URL).host,
        path: '/',
        expires: -1,
        httpOnly: false,
      },
    ]);
  } else {
    await page.context().addCookies([
      {
        name: 'onboardingDismissed',
        value: 'true',
        domain: new URL(SITE_URL).host,
        path: '/',
        expires: -1,
        httpOnly: false,
      },
    ]);
  }

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

  // Handle onboarding modal if it exists
  try {
    const onboardingButton = await page.locator(
      'button.onboarding-modal-accept',
    );
    if (await onboardingButton.isVisible()) {
      await onboardingButton.click();
    }
  } catch {
    console.log('No onboarding modal present, continuing...');
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

    if (i % 2 === 0) {
      const foundIssue = await openIssueByID(page, ISSUE_ID, cgID);
      if (foundIssue) {
        if (ADD_COMMENTS_AND_EMOJI) {
          await commentOnNewIssue(page, 'This is a test comment', cgID);
        }
      }
    } else {
      await openRandomIssue(page, cgID);
    }
    await page.locator('.nav-item', {hasText: 'All'}).click();

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

async function waitForIssueList(page: any) {
  await page.waitForFunction(() => {
    const issues = document.querySelectorAll('.issue-list .row');
    return issues.length > 1;
  });
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

async function selectRandomEmoji(page: any, cgID: string) {
  try {
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
    await emojiButtons.nth(randomIndex).click({timeout: 2000});
  } catch (error: any) {
    console.log(
      AWS_BATCH_JOB_ARRAY_INDEX,
      cgID,
      'Failed to select random emoji, skipping:',
      error.message,
    );
    return;
  }
}

async function commentOnNewIssue(page: any, comment: string, cgID: string) {
  try {
    await page.waitForSelector('[class^="_commentItem"]', {
      state: 'visible',
      timeout: 5000,
    });
    const comments = page.locator('[class^="_commentItem"]');

    try {
      //add emoji first title
      await page.locator('.add-emoji-button').first().click();
      await selectRandomEmoji(page, cgID);
    } catch (error: any) {
      console.log(
        AWS_BATCH_JOB_ARRAY_INDEX,
        cgID,
        'Failed to add emoji to title, skipping:',
        error.message,
      );
    }

    try {
      // Make sure comment input is visible and fill it
      await page.locator('.comment-input').scrollIntoViewIfNeeded();
      await page.locator('.comment-input').click({timeout: 2000});
      await page.locator('.comment-input').type(comment, {delay: 2});

      // Wait for button to be enabled before clicking
      await page
        .getByRole('button', {name: 'Add comment'})
        .click({timeout: 2000});
    } catch (error: any) {
      console.log(
        AWS_BATCH_JOB_ARRAY_INDEX,
        cgID,
        'Failed to add comment, skipping:',
        error.message,
      );
    }

    try {
      const commentCount = await comments.count();
      const randomCommentIndex = Math.floor(Math.random() * commentCount);

      await comments
        .nth(randomCommentIndex)
        .locator('.add-emoji-button')
        .scrollIntoViewIfNeeded();

      await comments
        .nth(randomCommentIndex)
        .locator('.add-emoji-button')
        .first()
        .click({timeout: 2000});

      await selectRandomEmoji(page, cgID);
    } catch (error: any) {
      console.log(
        AWS_BATCH_JOB_ARRAY_INDEX,
        cgID,
        'Failed to add emoji to random comment, skipping:',
        error.message,
      );
    }
  } catch (error: any) {
    console.log(
      AWS_BATCH_JOB_ARRAY_INDEX,
      cgID,
      'Failed to load comments section, skipping entire comment operation:',
      error.message,
    );
  }
}

async function openIssueByID(
  page: any,
  issueID: string,
  cgID: string,
): Promise<boolean> {
  try {
    await page.locator('.nav-item', {hasText: 'All'}).click();
    await waitForIssueList(page);

    try {
      await page
        .locator(`.issue-list .row a[href="/issue/${issueID}"]`)
        .first()
        .scrollIntoViewIfNeeded();
      await page
        .locator(`.issue-list .row a[href="/issue/${issueID}"]`)
        .first()
        .click();

      console.log(
        AWS_BATCH_JOB_ARRAY_INDEX,
        cgID,
        `Successfully opened issue: ${issueID}`,
      );
      return true;
    } catch (error: any) {
      console.log(
        AWS_BATCH_JOB_ARRAY_INDEX,
        cgID,
        'Failed to click on issue:',
        error.message,
      );
      return false;
    }
  } catch (error: any) {
    console.log(
      AWS_BATCH_JOB_ARRAY_INDEX,
      cgID,
      'Failed to load issue list:',
      error.message,
    );
    return false;
  }
}

async function openRandomIssue(page: any, cgID: string): Promise<boolean> {
  try {
    await page.locator('.nav-item', {hasText: 'All'}).click();
    await waitForIssueList(page);

    try {
      const issues = page.locator('.issue-list .row');
      const count = await issues.count();

      if (count === 0) {
        console.log(AWS_BATCH_JOB_ARRAY_INDEX, cgID, 'No issues found to open');
        return false;
      }

      const randomIndex = Math.floor(Math.random() * count);
      await issues.nth(randomIndex).scrollIntoViewIfNeeded();
      await issues.nth(randomIndex).click({timeout: 2000});

      console.log(
        AWS_BATCH_JOB_ARRAY_INDEX,
        cgID,
        `Opened random issue #${randomIndex + 1} of ${count}`,
      );
      return true;
    } catch (error: any) {
      console.log(
        AWS_BATCH_JOB_ARRAY_INDEX,
        cgID,
        'Failed to select random issue:',
        error.message,
      );
      return false;
    }
  } catch (error: any) {
    console.log(
      AWS_BATCH_JOB_ARRAY_INDEX,
      cgID,
      'Failed to load issue list:',
      error.message,
    );
    return false;
  }
}
