import {expect, test} from 'vitest';
import {planWarningMessage} from './plan-warnings.ts';

test('missing-index per row', () => {
  expect(
    planWarningMessage({
      type: 'missing-index',
      table: 'comment',
      path: ['comments'],
      perRow: true,
      columns: ['issueID'],
      rows: 1_234_567,
      suggestedIndex: ['issueID', 'created', 'id'],
    }),
  ).toBe(
    "Each lookup of comment (related 'comments') by issueID scans all " +
      '~1,234,567 rows because no index covers issueID, and it runs once ' +
      'per parent row. Consider adding an index on comment (issueID, ' +
      'created, id) upstream.',
  );
});

test('missing-index per query', () => {
  expect(
    planWarningMessage({
      type: 'missing-index',
      table: 'issue',
      path: [],
      perRow: false,
      columns: ['creatorID', 'open'],
      rows: 50_000,
      suggestedIndex: ['creatorID', 'open', 'id'],
    }),
  ).toBe(
    'Each read of issue by creatorID, open scans all ~50,000 rows because ' +
      'no index covers creatorID, open. Consider adding an index on issue ' +
      '(creatorID, open, id) upstream.',
  );
});

test('full-sort', () => {
  expect(
    planWarningMessage({
      type: 'full-sort',
      table: 'user',
      path: ['comments', 'author'],
      perRow: true,
      orderBy: [
        ['name', 'asc'],
        ['id', 'desc'],
      ],
      rows: 20_000.4,
      suggestedIndex: ['name', 'id'],
    }),
  ).toBe(
    "Each read of user (related 'comments' > 'author') sorts ~20,000 rows " +
      'for ORDER BY name asc, id desc, reading every matching row before ' +
      'returning the first, so a limit does not reduce the rows read, and ' +
      'it runs once per parent row. Consider adding an index on user ' +
      '(name, id) upstream.',
  );
});

test('high-cost', () => {
  expect(
    planWarningMessage({type: 'high-cost', table: 'issue', cost: 2_500_000}),
  ).toBe(
    'The best plan found for the query is estimated to process ~2,500,000 rows.',
  );
});
