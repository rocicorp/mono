import {expect, test} from 'vitest';
import type {
  AST,
  Condition,
  CorrelatedSubquery,
} from '../../../../zero-protocol/src/ast.ts';
import {findCoveringQuery, isQueryCoveredBy} from './query-covering.ts';

const allIssues: AST = {
  table: 'issues',
  orderBy: [['id', 'asc']],
};

const allComments: AST = {
  table: 'comments',
  orderBy: [['id', 'asc']],
};

function where(condition: Condition): AST {
  return {
    ...allIssues,
    where: condition,
  };
}

function eq(column: string, value: string | number): Condition {
  return {
    type: 'simple',
    left: {type: 'column', name: column},
    op: '=',
    right: {type: 'literal', value},
  };
}

function gt(column: string, value: number): Condition {
  return {
    type: 'simple',
    left: {type: 'column', name: column},
    op: '>',
    right: {type: 'literal', value},
  };
}

function and(...conditions: Condition[]): Condition {
  return {
    type: 'and',
    conditions,
  };
}

function or(...conditions: Condition[]): Condition {
  return {
    type: 'or',
    conditions,
  };
}

function commentsRelated(subquery: AST): CorrelatedSubquery {
  return {
    system: 'client',
    correlation: {
      parentField: ['id'],
      childField: ['issueID'],
    },
    subquery: {
      ...subquery,
      alias: 'comments',
    },
  };
}

test('same query covers itself', () => {
  expect(isQueryCoveredBy(where(eq('id', '123')), where(eq('id', '123')))).toBe(
    true,
  );
});

test('unfiltered query covers filtered query on the same table', () => {
  expect(isQueryCoveredBy(where(eq('id', '123')), allIssues)).toBe(true);
});

test('filter conjunction is covered by a subset of its filters', () => {
  const covered = where(and(eq('status', 'open'), eq('owner', 'alice')));
  const covering = where(eq('status', 'open'));

  expect(isQueryCoveredBy(covered, covering)).toBe(true);
  expect(isQueryCoveredBy(covering, covered)).toBe(false);
});

test('simple equality and range implication are recognized', () => {
  expect(
    isQueryCoveredBy(
      where(eq('id', '1')),
      where({
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: 'IN',
        right: {type: 'literal', value: ['1', '2']},
      }),
    ),
  ).toBe(true);

  expect(
    isQueryCoveredBy(where(gt('priority', 5)), where(gt('priority', 3))),
  ).toBe(true);
});

test('or coverage is conservative but detects disjunct coverage', () => {
  const bug = eq('type', 'bug');
  const feature = eq('type', 'feature');

  expect(isQueryCoveredBy(where(bug), where(or(bug, feature)))).toBe(true);
  expect(isQueryCoveredBy(where(or(bug, feature)), where(bug))).toBe(false);
});

test('unlimited covering query covers limited and paged covered query', () => {
  const covered: AST = {
    ...where(eq('status', 'open')),
    limit: 10,
    start: {
      row: {id: 'abc'},
      exclusive: true,
    },
  };

  expect(isQueryCoveredBy(covered, allIssues)).toBe(true);
});

test('limited covering query must have equivalent input and a large enough limit', () => {
  const covered: AST = {
    ...where(eq('status', 'open')),
    limit: 10,
  };
  const sameInputLargerLimit: AST = {
    ...where(eq('status', 'open')),
    limit: 20,
  };
  const broaderInputSameLimit: AST = {
    ...allIssues,
    limit: 10,
  };

  expect(isQueryCoveredBy(covered, sameInputLargerLimit)).toBe(true);
  expect(isQueryCoveredBy(covered, broaderInputSameLimit)).toBe(false);
});

test('related query coverage is recursive', () => {
  const commentsWithText: AST = {
    ...allComments,
    where: eq('text', 'hello'),
  };
  const covered: AST = {
    ...where(eq('status', 'open')),
    related: [commentsRelated(commentsWithText)],
  };
  const covering: AST = {
    ...allIssues,
    related: [commentsRelated(allComments)],
  };

  expect(isQueryCoveredBy(covered, covering)).toBe(true);
  expect(isQueryCoveredBy(covered, allIssues)).toBe(false);
});

test('not exists coverage reverses subquery implication', () => {
  const noComments: AST = where({
    type: 'correlatedSubquery',
    op: 'NOT EXISTS',
    related: commentsRelated(allComments),
  });
  const noHelloComments: AST = where({
    type: 'correlatedSubquery',
    op: 'NOT EXISTS',
    related: commentsRelated({
      ...allComments,
      where: eq('text', 'hello'),
    }),
  });

  expect(isQueryCoveredBy(noComments, noHelloComments)).toBe(true);
  expect(isQueryCoveredBy(noHelloComments, noComments)).toBe(false);
});

test('findCoveringQuery returns the first active covering query', () => {
  const running = new Map([
    [
      'query-1',
      {
        transformedAst: allComments,
        transformationHash: 'hash-1',
      },
    ],
    [
      'query-2',
      {
        transformedAst: allIssues,
        transformationHash: 'hash-2',
        queryName: 'allIssues',
      },
    ],
  ]);

  expect(findCoveringQuery('query-3', where(eq('id', '123')), running)).toEqual(
    {
      queryID: 'query-2',
      transformationHash: 'hash-2',
      queryName: 'allIssues',
    },
  );
});
