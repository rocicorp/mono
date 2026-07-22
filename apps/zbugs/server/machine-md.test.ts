import {expect, test} from 'vitest';
import {
  formatDate,
  oneLine,
  renderErrorMd,
  renderIssueMd,
  type MdComment,
  type MdIssue,
} from './machine-md.ts';

const fullIssue: MdIssue = {
  id: 'aB3xY_-9',
  shortID: 3456,
  title: 'Query  hangs\nafter reconnect',
  open: true,
  visibility: 'public',
  created: Date.UTC(2026, 0, 2, 3, 4, 5),
  modified: Date.UTC(2026, 6, 1, 6, 7, 8),
  description: 'Steps to reproduce:\n\n1. Disconnect\n2. Reconnect',
  project: {name: 'Zero', lowerCaseName: 'zero'},
  creator: {login: 'alice'},
  assignee: {login: 'bob'},
  labels: [{name: 'perf'}, {name: 'bug'}],
  emoji: [
    {value: '👍', creator: {login: 'bob'}},
    {value: '🎉', creator: {login: 'carol'}},
    {value: '👍', creator: {login: 'alice'}},
  ],
};

const comments: MdComment[] = [
  {
    created: Date.UTC(2026, 0, 3),
    body: 'Repros for me too.',
    creator: {login: 'carol'},
    emoji: [{value: '🚀', creator: {login: 'dave'}}],
  },
  {
    created: Date.UTC(2026, 0, 4),
    body: '',
    creator: undefined,
    emoji: [],
  },
];

test('renderIssueMd full issue', () => {
  expect(renderIssueMd(fullIssue, comments, {commentsCapped: false})).toBe(
    `# Bug 3456: Query hangs after reconnect

- Status: open
- Project: Zero
- Creator: @alice
- Assignee: @bob
- Labels: bug, perf
- Created: 2026-01-02T03:04:05Z
- Modified: 2026-07-01T06:07:08Z
- Reactions: 🎉 ×1 (carol) · 👍 ×2 (alice, bob)
- URL: https://bugs.rocicorp.dev/p/zero/issue/3456

## Description

Steps to reproduce:

1. Disconnect
2. Reconnect

## Comments (2)

### @carol — 2026-01-03T00:00:00Z

Repros for me too.

Reactions: 🚀 ×1 (dave)

### @unknown — 2026-01-04T00:00:00Z

_No content._
`,
  );
});

test('renderIssueMd minimal issue with null shortID', () => {
  const minimal: MdIssue = {
    id: 'aB3xY_-9',
    shortID: null,
    title: 'Untitled',
    open: false,
    visibility: 'internal',
    created: Date.UTC(2026, 0, 1),
    modified: Date.UTC(2026, 0, 1),
    description: '  ',
  };
  expect(renderIssueMd(minimal, [], {commentsCapped: false})).toBe(
    `# Issue aB3xY_-9: Untitled

- Status: closed
- Visibility: internal
- Created: 2026-01-01T00:00:00Z
- Modified: 2026-01-01T00:00:00Z

## Description

_No description._

## Comments (0)
`,
  );
});

test('renderIssueMd capped comments note', () => {
  const md = renderIssueMd(fullIssue, comments, {commentsCapped: true});
  expect(md).toContain(
    '## Comments (2)\n\n_Showing the 2 most recent comments; earlier comments omitted._\n',
  );
});

test('renderIssueMd keeps markdown in title to one line', () => {
  const md = renderIssueMd(
    {...fullIssue, title: '# nested\r\n\theading '},
    [],
    {commentsCapped: false},
  );
  expect(md.split('\n')[0]).toBe('# Bug 3456: # nested heading');
});

test('reactions without creators still count', () => {
  const md = renderIssueMd(
    {...fullIssue, emoji: [{value: '👀'}, {value: '👀'}]},
    [],
    {commentsCapped: false},
  );
  expect(md).toContain('- Reactions: 👀 ×2\n');
});

test('formatDate', () => {
  expect(formatDate(Date.UTC(2026, 6, 1, 12, 34, 56, 789))).toBe(
    '2026-07-01T12:34:56Z',
  );
});

test('oneLine', () => {
  expect(oneLine('  a\r\n b\t\tc  ')).toBe('a b c');
});

test('renderErrorMd', () => {
  expect(renderErrorMd('Not Found', 'No such issue.')).toBe(
    '# Not Found\n\nNo such issue.\n',
  );
});
