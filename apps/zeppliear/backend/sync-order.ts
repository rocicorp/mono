import {
  commentSchema,
  COMMENT_KEY_PREFIX,
  DESCRIPTION_KEY_PREFIX,
  getDescriptionIssueId,
  getIssue,
  issueSchema,
  ISSUE_KEY_PREFIX,
  reverseTimestampSortKey,
} from '../frontend/issue';
import type {JSONValue, ReadTransaction} from 'replicache';
import {assertNotUndefined} from '../util/asserts';

export async function getSyncOrder(
  tx: ReadTransaction,
  entry: [key: string, value: JSONValue],
): Promise<string> {
  // The default view is a list of issues in reverse modified order, so it is
  // preferable to sync entries in reverse modified order of their
  // corresponding issue, so that if a user clicks on an issue near the top
  // of the default initial list view the entries needed for displaying the
  // detail view is available as soon as possible.
  const [key, value] = entry;
  let issue;
  if (key.startsWith(ISSUE_KEY_PREFIX)) {
    // Note as an optimization we return all of the issue entries in the
    // first pull response regardless of sync order, but we still need
    // to assign them a sync order despite it being unused.
    issue = issueSchema.parse(value);
  } else if (key.startsWith(COMMENT_KEY_PREFIX)) {
    const comment = commentSchema.parse(value);
    issue = await getIssue(tx, comment.issueID);
  } else if (key.startsWith(DESCRIPTION_KEY_PREFIX)) {
    issue = await getIssue(tx, getDescriptionIssueId(key));
  }
  assertNotUndefined(issue);
  return reverseTimestampSortKey(issue.modified, issue.id) + '-' + key;
}
