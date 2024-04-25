import {
  Issue,
  Comment,
  Priority,
  Status,
  Member,
  Label,
  IssueLabel,
} from '../frontend/issue';
import {generateNKeysBetween} from 'fractional-indexing';
import {sortBy} from 'lodash';
import {reactIssues} from '../sample-data/issues-react';
import {reactComments} from '../sample-data/comments-react';
import {nanoid} from 'nanoid';

export type SampleData = {
  issues: Issue[];
  comments: Comment[];
  members: Member[];
  labels: Label[];
  issueLabels: IssueLabel[];
};

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
  const memberLoginToID: Record<string, string> = {};
  const issueNumberToID: Record<string, string> = {};
  const issues: Issue[] = sortedIssues.map((reactIssue, idx) => {
    let creatorID = memberLoginToID[reactIssue.creator_user_login];
    if (!creatorID) {
      creatorID = nanoid();
      memberLoginToID[reactIssue.creator_user_login] = creatorID;
    }
    const issueID = nanoid();
    issueNumberToID[reactIssue.number.toString()] = issueID;

    return {
      id: issueID,
      title: reactIssue.title,
      priority: getPriority(reactIssue),
      status: getStatus(reactIssue),
      modified: Date.parse(reactIssue.updated_at),
      created: Date.parse(reactIssue.created_at),
      creatorID,
      kanbanOrder: kanbanOrderKeys[idx],
      description: (reactIssue.body || '').substring(0, 10000),
    };
  });

  const comments: Comment[] = reactComments.map(reactComment => {
    let creatorID = memberLoginToID[reactComment.creator_user_login];
    if (!creatorID) {
      creatorID = nanoid();
      memberLoginToID[reactComment.creator_user_login] = creatorID;
    }
    return {
      id: reactComment.comment_id,
      issueID: issueNumberToID[reactComment.number.toString()],
      created: Date.parse(reactComment.created_at),
      body: reactComment.body || '',
      creatorID,
    };
  });

  const members: Member[] = Object.entries(memberLoginToID).map(
    ([login, id]) => ({id, name: login}),
  );
  const labels = [
    '❤️',
    'Browser: IE',
    'Browser: Safari',
    'CLA Signed',
    'Component: Build Infrastructure',
    'Component: Component API',
    'Component: Concurrent Features',
    'Component: Core Utilities',
    'Component: Developer Tools',
    'Component: DOM',
    'Component: ESLint Rules',
    'Component: Fast Refresh',
    'Component: Flight',
    'Component: Hooks',
    'Component: Optimizing Compiler',
    'Component: ReactIs',
    'Component: Reconciler',
    'Component: Scheduler',
    'Component: Scheduling Profiler',
    'Component: Server Rendering',
    'Component: Shallow Renderer',
    'Component: Suspense',
    'Component: Test Renderer',
    'Component: Test Utils',
    'dependencies',
    'Difficulty: challenging',
    'Difficulty: medium',
    'Difficulty: starter',
    'fb-exported',
    'good first issue',
    'good first issue (taken)',
    'HTML',
    'invalid',
    'Needs Browser Testing',
    'Partner',
    'React 18',
    'React 19',
    'React Core Team',
    'Resolution: Backlog',
    'Resolution: Duplicate',
    'Resolution: Expected Behavior',
    'Resolution: Invalid',
    'Resolution: Needs More Information',
    'Resolution: Stale',
    'Resolution: Support Redirect',
    'Resolution: Unsolved',
    'Resolution: Wontfix',
    'Size: Large',
    'Size: Medium',
    'Size: Medium-Large',
    'Size: Small',
    'Size: Small-Medium',
    'Status: New',
    'Status: Reverted',
    'Status: Unconfirmed',
    'SVG',
    'Type: Big Picture',
    'Type: Breaking Change',
    'Type: Bug',
    'Type: Discussion',
    'Type: Enhancement',
    'Type: Feature Request',
    'Type: Needs Investigation',
    'Type: Question',
    'Type: Regression',
    'Type: Release',
    'Type: Security',
    'Type: Umbrella',
  ].map(name => ({id: nanoid(), name}));

  const issueLabels = issues.flatMap(issue => {
    const numLabels = Math.floor(Math.random() * 4);
    const labelIDs = new Set<string>();
    while (labelIDs.size < numLabels) {
      labelIDs.add(labels[Math.floor(Math.random() * labels.length)].id);
    }
    return [...labelIDs].map(labelID => ({
      id: issue.id + '-' + labelID,
      issueID: issue.id,
      labelID,
    }));
  });

  return {
    issues,
    comments,
    members,
    labels,
    issueLabels,
  };
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
