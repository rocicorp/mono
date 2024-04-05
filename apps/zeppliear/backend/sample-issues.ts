import {Issue, Comment, Description, Priority, Status} from '../frontend/issue';
import {generateNKeysBetween} from 'fractional-indexing';
import {sortBy} from 'lodash';
import {reactIssues} from '../sample-data/issues-react';
import {reactComments} from '../sample-data/comments-react';

export type SampleData = {
  issue: Issue;
  description: Description;
  comments: Comment[];
}[];

export function getReactSampleData(): SampleData {
  const sortedIssues = sortBy(
    reactIssues,
    reactIssue =>
      Number.MAX_SAFE_INTEGER -
      Date.parse(reactIssue.updated_at) +
      '-' +
      reactIssue.number,
  );

  const issuesCount = reactIssues.length;
  const kanbanOrderKeys = generateNKeysBetween(null, null, issuesCount);
  const issues: SampleData = sortedIssues.map((reactIssue, idx) => ({
    issue: {
      id: reactIssue.number.toString(),
      title: reactIssue.title,
      priority: getPriority(reactIssue),
      status: getStatus(reactIssue),
      modified: Date.parse(reactIssue.updated_at),
      created: Date.parse(reactIssue.created_at),
      creator: reactIssue.creator_user_login,
      kanbanOrder: kanbanOrderKeys[idx],
    },
    description: reactIssue.body || '',
    comments: [],
  }));

  const comments = reactComments.map(reactComment => ({
    id: reactComment.comment_id,
    issueID: reactComment.number.toString(),
    created: Date.parse(reactComment.created_at),
    body: reactComment.body || '',
    creator: reactComment.creator_user_login,
  }));
  for (const comment of comments) {
    const issue = issues.find(issue => issue.issue.id === comment.issueID);
    if (issue) {
      issue.comments.push(comment);
    }
  }
  issues;

  // Can use this to generate artifically larger datasets for stress testing.
  const multiplied: SampleData = [];
  for (let i = 0; i < 1; i++) {
    multiplied.push(
      ...issues.map(issue => ({
        ...issue,
        issue: {
          ...issue.issue,
          id: issue.issue.id + '-' + i,
        },
        comments: issue.comments.map(comment => ({
          ...comment,
          issueID: comment.issueID + '-' + i,
        })),
      })),
    );
  }

  return multiplied;
}

function getStatus({
  number,
  // eslint-disable-next-line @typescript-eslint/naming-convention
  created_at,
}: {
  number: number;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  created_at: string;
}): Status {
  const stableRandom = number + Date.parse(created_at);
  // 90% closed, 10% open
  if (stableRandom % 10 < 8) {
    // 2/3's done, 1/3 cancelled
    switch (stableRandom % 3) {
      case 0:
      case 1:
        return Status.Done;
      case 2:
        return Status.Canceled;
    }
  }
  switch (stableRandom % 6) {
    // 2/6 backlog, 3/6 todo, 1/6 in progress
    case 0:
    case 1:
      return Status.Backlog;
    case 2:
    case 3:
    case 4:
      return Status.Todo;
    case 5:
      return Status.InProgress;
  }
  return Status.Todo;
}

function getPriority({
  number,
  // eslint-disable-next-line @typescript-eslint/naming-convention
  created_at,
}: {
  number: number;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  created_at: string;
}): Priority {
  const stableRandom = number + Date.parse(created_at);
  // bell curve priorities
  switch (stableRandom % 10) {
    case 0:
      return Priority.None;
    case 1:
    case 2:
      return Priority.Low;
    case 3:
    case 4:
    case 5:
    case 6:
      return Priority.Medium;
    case 7:
    case 8:
      return Priority.High;
    case 9:
      return Priority.Urgent;
  }
  return Priority.None;
}
