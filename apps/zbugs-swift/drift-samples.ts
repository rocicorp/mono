/**
 * What the drift test runs zbugs' TS queries and mutators on. The recorder
 * (`zero-swift drift record`, or `UPDATE_GENERATED=1 pnpm test` in
 * packages/zero-swift-cli) writes what they did to
 * Tests/ZBugsTests/drift-vectors.json, and `swift test` checks the Swift
 * port against it.
 *
 * Each sample takes a branch of a query or mutator, so a change to that
 * branch on one side and not the other fails the test.
 */
import type {
  DriftSamples,
  MutatorSample,
  QuerySample,
} from '../../packages/zero-swift-cli/src/drift.ts';
import {mutators} from '../zbugs/shared/mutators.ts';
import {queries} from '../zbugs/shared/queries.ts';
import {schema} from '../zbugs/shared/schema.ts';

const crew = {sub: 'u-crew', role: 'crew'};
const alice = {sub: 'u-alice', role: 'user'};
const bob = {sub: 'u-bob', role: 'user'};

const users = [
  {id: 'u-crew', login: 'crew', name: 'Crew', avatar: 'c.png', role: 'crew'},
  {id: 'u-alice', login: 'alice', name: 'Alice', avatar: 'a.png', role: 'user'},
  {id: 'u-bob', login: 'bob', name: null, avatar: 'b.png', role: 'user'},
];
const project = {
  id: 'p-zero',
  name: 'Zero',
  lowerCaseName: 'zero',
  issueCountEstimate: 2,
  supportsSearch: true,
  markURL: null,
  logoURL: null,
};
const issue = (id: string, creatorID: string, extra: object = {}) => ({
  id,
  shortID: null,
  title: `Issue ${id}`,
  open: true,
  modified: 1000,
  created: 1000,
  projectID: 'p-zero',
  creatorID,
  assigneeID: null,
  description: '',
  visibility: 'public',
  ...extra,
});

/** Alice's public issue, assigned to the crew; Bob's internal issue. */
const seed = {
  user: users,
  project: [project],
  issue: [
    issue('i-public', 'u-alice', {shortID: 12, assigneeID: 'u-crew'}),
    issue('i-internal', 'u-bob', {visibility: 'internal'}),
  ],
  comment: [
    {
      id: 'c-1',
      issueID: 'i-public',
      created: 1100,
      body: 'Hi',
      creatorID: 'u-bob',
    },
    {
      id: 'c-2',
      issueID: 'i-internal',
      created: 1200,
      body: 'Hm',
      creatorID: 'u-bob',
    },
  ],
  label: [{id: 'l-bug', name: 'bug', projectID: 'p-zero'}],
  issueLabel: [{issueID: 'i-public', labelID: 'l-bug', projectID: 'p-zero'}],
  emoji: [
    {
      id: 'e-1',
      value: '👍',
      annotation: 'thumbs up',
      subjectID: 'i-public',
      creatorID: 'u-bob',
      created: 1300,
    },
  ],
  issueNotifications: [
    {userID: 'u-crew', issueID: 'i-public', subscribed: true, created: 1000},
  ],
};

const listContext = {
  open: true,
  assignee: null,
  creator: null,
  labels: null,
  textFilter: null,
  sortField: 'modified',
  sortDirection: 'desc',
};
const fullListContext = {
  open: false,
  projectName: 'Zero',
  assignee: 'crew',
  creator: 'alice',
  labels: ['bug', 'p1'],
  textFilter: '50%_off\\',
  sortField: 'created',
  sortDirection: 'asc',
};
const rowSort = {id: 'i-public', created: 1000, modified: 2000};

const querySamples: QuerySample[] = [
  {name: 'allLabels'},
  {name: 'allUsers'},
  {name: 'allProjects'},
  {name: 'user', args: 'u-alice'},
  {name: 'crewUser', args: 'u-crew'},
  {name: 'labels', args: {projectName: 'Zero'}},
  {name: 'labelsOrderByName', args: {projectName: 'ZÉRO', orderBy: 'desc'}},
  {name: 'issuePreloadV2', args: {projectName: 'Zero'}, context: crew},
  {name: 'issuePreloadV2', args: {projectName: 'zero'}, context: alice},
  {name: 'issuePreloadV2', args: {projectName: 'Zero'}},
  {name: 'userPref', args: 'theme', context: alice},
  {name: 'userPref', args: 'theme'},
  {
    name: 'usersForProject',
    args: {disabled: true, login: 'alice', projectName: 'Zero'},
  },
  {
    name: 'usersForProject',
    args: {disabled: true, projectName: 'Zero', filter: 'crew'},
  },
  {
    name: 'usersForProject',
    args: {
      disabled: false,
      login: 'alice',
      projectName: 'Zero',
      filter: 'crew',
    },
  },
  {
    name: 'usersForProject',
    args: {disabled: false, projectName: 'Zero', filter: 'creators'},
  },
  {
    name: 'usersForProject',
    args: {disabled: false, projectName: 'Zero', filter: 'assignees'},
  },
  {name: 'usersForProject', args: {disabled: false, projectName: 'Zero'}},
  {
    name: 'commentsPage',
    args: {
      issueID: 'i-public',
      limit: 25,
      start: null,
      dir: 'forward',
      inclusive: false,
    },
  },
  {
    name: 'commentsPage',
    args: {
      issueID: 'i-public',
      limit: 25,
      start: {id: 'c-1', created: 1100},
      dir: 'backward',
      inclusive: true,
    },
  },
  {name: 'comment', args: 'c-1'},
  {name: 'issueDetail', args: {idField: 'shortID', id: 12}, context: crew},
  {name: 'issueDetail', args: {idField: 'id', id: 'i-public'}, context: alice},
  {name: 'issueDetail', args: {idField: 'id', id: 'i-public'}},
  {
    name: 'issueListV2',
    args: {listContext, limit: 50, start: null, dir: 'forward'},
    context: alice,
  },
  {
    name: 'issueListV2',
    args: {
      listContext: fullListContext,
      limit: null,
      start: rowSort,
      dir: 'backward',
      inclusive: true,
    },
    context: crew,
  },
  {
    name: 'issueListV2',
    args: {listContext, limit: 0, start: rowSort, dir: 'forward'},
  },
  {
    name: 'listIssueByID',
    args: {listContext, idField: 'shortID', idValue: 12},
    context: alice,
  },
  {
    name: 'listIssueByID',
    args: {listContext: fullListContext, idField: 'id', idValue: 'i-public'},
    context: crew,
  },
  {name: 'emojiChange', args: 'i-public'},
  {
    name: 'prevNext',
    args: {listContext: null, issue: null, dir: 'next'},
    context: alice,
  },
  {
    name: 'prevNext',
    args: {listContext: fullListContext, issue: rowSort, dir: 'prev'},
    context: crew,
  },
  {name: 'issueList', args: {listContext, limit: 10}, context: alice},
  {name: 'userPicker', args: {disabled: true, login: 'bob', filter: null}},
  {name: 'userPicker', args: {disabled: false, login: null, filter: 'crew'}},
  {
    name: 'userPicker',
    args: {disabled: false, login: 'bob', filter: 'creators'},
  },
  {name: 'userPicker', args: {disabled: true, login: null, filter: null}},
];

const mutatorSamples: MutatorSample[] = [
  {
    name: 'issue.create',
    args: {
      id: 'i-new',
      title: 'New',
      created: 5000,
      modified: 5000,
      projectID: 'p-zero',
    },
    context: alice,
    seed,
  },
  {
    name: 'issue.create',
    args: {
      id: 'i-new',
      title: 'New',
      description: 'Body',
      created: 5000,
      modified: 5000,
      projectID: 'p-zero',
    },
    context: crew,
    seed,
  },
  {
    name: 'issue.create',
    args: {
      id: 'i-new',
      title: 'New',
      created: 5000,
      modified: 5000,
      projectID: 'p-zero',
    },
    seed,
  },
  // Reassigns from the crew to Bob: subscribes Bob, unsubscribes the crew.
  {
    name: 'issue.update',
    args: {
      id: 'i-public',
      title: 'Renamed',
      assigneeID: 'u-bob',
      modified: 6000,
    },
    context: alice,
    seed,
  },
  // Unassigns: unsubscribes the crew.
  {
    name: 'issue.update',
    args: {id: 'i-public', assigneeID: null, open: false, modified: 6000},
    context: crew,
    seed,
  },
  // The same assignee: no notification changes.
  {
    name: 'issue.update',
    args: {
      id: 'i-public',
      assigneeID: 'u-crew',
      visibility: 'internal',
      description: 'More',
      modified: 6000,
    },
    context: alice,
    seed,
  },
  {
    name: 'issue.update',
    args: {id: 'i-public', title: 'Mine', modified: 6000},
    context: bob,
    seed,
  },
  {
    name: 'issue.update',
    args: {id: 'i-missing', title: 'Gone', modified: 6000},
    context: crew,
    seed,
  },
  {name: 'issue.delete', args: 'i-public', context: alice, seed},
  {name: 'issue.delete', args: 'i-public', context: bob, seed},
  {name: 'issue.delete', args: 'i-public', seed},
  {
    name: 'issue.deleteMany',
    args: ['i-public', 'i-internal'],
    context: crew,
    seed,
  },
  {
    name: 'issue.deleteMany',
    args: ['i-public', 'i-internal'],
    context: alice,
    seed,
  },
  {
    name: 'issue.addLabel',
    args: {issueID: 'i-internal', labelID: 'l-bug'},
    context: bob,
    seed,
  },
  {
    name: 'issue.addLabel',
    args: {issueID: 'i-internal', labelID: 'l-bug', projectID: 'p-other'},
    context: crew,
    seed,
  },
  {
    name: 'issue.addLabel',
    args: {issueID: 'i-missing', labelID: 'l-bug'},
    context: crew,
    seed,
  },
  {
    name: 'issue.removeLabel',
    args: {issueID: 'i-public', labelID: 'l-bug'},
    context: alice,
    seed,
  },
  {
    name: 'notification.update',
    args: {issueID: 'i-public', subscribed: 'subscribe', created: 7000},
    context: crew,
    seed,
  },
  {
    name: 'notification.update',
    args: {issueID: 'i-public', subscribed: 'unsubscribe', created: 7000},
    context: alice,
    seed,
  },
  {
    name: 'notification.update',
    args: {issueID: 'i-internal', subscribed: 'subscribe', created: 7000},
    context: alice,
    seed,
  },
  {
    name: 'emoji.addToIssue',
    args: {
      id: 'e-new',
      unicode: '🎉',
      annotation: 'tada',
      subjectID: 'i-public',
      created: 8000,
    },
    context: alice,
    seed,
  },
  {
    name: 'emoji.addToIssue',
    args: {
      id: 'e-new',
      unicode: '🎉',
      annotation: 'tada',
      subjectID: 'i-internal',
      created: 8000,
    },
    context: alice,
    seed,
  },
  {
    name: 'emoji.addToComment',
    args: {
      id: 'e-new',
      unicode: '❤️',
      annotation: 'heart',
      subjectID: 'c-1',
      created: 8000,
    },
    context: crew,
    seed,
  },
  {name: 'emoji.remove', args: 'e-1', context: bob, seed},
  {name: 'emoji.remove', args: 'e-1', context: alice, seed},
  {
    name: 'comment.add',
    args: {id: 'c-new', issueID: 'i-public', body: 'Yes', created: 9000},
    context: alice,
    seed,
  },
  {
    name: 'comment.add',
    args: {id: 'c-new', issueID: 'i-missing', body: 'No', created: 9000},
    context: alice,
    seed,
  },
  {
    name: 'comment.edit',
    args: {id: 'c-1', body: 'Edited'},
    context: bob,
    seed,
  },
  {name: 'comment.remove', args: 'c-1', context: alice, seed},
  {name: 'comment.remove', args: 'c-2', context: crew, seed},
  {
    name: 'label.create',
    args: {id: 'l-new', name: 'perf', projectID: 'p-zero'},
    context: crew,
    seed,
  },
  {
    name: 'label.create',
    args: {id: 'l-new', name: 'perf', projectID: 'p-zero'},
    context: alice,
    seed,
  },
  {
    name: 'label.create',
    args: {id: 'l-new', name: 'perf', projectID: 'p-zero'},
    seed,
  },
  {
    name: 'label.createAndAddToIssue',
    args: {issueID: 'i-internal', labelID: 'l-new', labelName: 'perf'},
    context: crew,
    seed,
  },
  {
    name: 'label.createAndAddToIssue',
    args: {
      issueID: 'i-missing',
      labelID: 'l-new',
      labelName: 'perf',
      projectID: 'p-zero',
    },
    context: crew,
    seed,
  },
  {
    name: 'viewState.set',
    args: {issueID: 'i-public', viewed: 10000},
    context: bob,
    seed,
  },
  {
    name: 'userPref.set',
    args: {key: 'theme', value: 'dark'},
    context: alice,
    seed,
  },
  {name: 'userPref.set', args: {key: 'theme', value: 'dark'}, seed},
];

export default {
  schema,
  queries,
  mutators,
  querySamples,
  mutatorSamples,
} satisfies DriftSamples;
