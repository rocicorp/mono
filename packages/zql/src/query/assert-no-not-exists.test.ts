import {expect, test} from 'vitest';
import type {Condition} from '../../../zero-protocol/src/ast.ts';
import {assertNoNotExists} from './assert-no-not-exists.ts';

test('throws an error when NOT EXISTS is used', () => {
  const condition: Condition = {
    type: 'correlatedSubquery',
    op: 'NOT EXISTS',
    related: {
      correlation: {
        parentField: ['id'],
        childField: ['issue_id'],
      },
      subquery: {
        table: 'comments',
      },
    },
  };

  expect(() => assertNoNotExists(condition)).toThrow(
    'NOT EXISTS is not supported on the client',
  );
});

test('does not throw for EXISTS', () => {
  const condition: Condition = {
    type: 'correlatedSubquery',
    op: 'EXISTS',
    related: {
      correlation: {
        parentField: ['id'],
        childField: ['issue_id'],
      },
      subquery: {
        table: 'comments',
      },
    },
  };

  expect(() => assertNoNotExists(condition)).not.toThrow();
});

test('checks nested conditions', () => {
  const condition: Condition = {
    type: 'and',
    conditions: [
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'status'},
        right: {type: 'literal', value: 'open'},
      },
      {
        type: 'or',
        conditions: [
          {
            type: 'simple',
            op: '>',
            left: {type: 'column', name: 'priority'},
            right: {type: 'literal', value: 3},
          },
          {
            type: 'correlatedSubquery',
            op: 'NOT EXISTS', // This should trigger the error
            related: {
              correlation: {
                parentField: ['id'],
                childField: ['task_id'],
              },
              subquery: {
                table: 'comments',
              },
            },
          },
        ],
      },
    ],
  };

  expect(() => assertNoNotExists(condition)).toThrow(
    'NOT EXISTS is not supported on the client',
  );
});

test('checks subquery where conditions', () => {
  const condition: Condition = {
    type: 'correlatedSubquery',
    op: 'EXISTS',
    related: {
      correlation: {
        parentField: ['id'],
        childField: ['issue_id'],
      },
      subquery: {
        table: 'comments',
        where: {
          type: 'correlatedSubquery',
          op: 'NOT EXISTS', // This should trigger the error
          related: {
            correlation: {
              parentField: ['id'],
              childField: ['comment_id'],
            },
            subquery: {
              table: 'reactions',
            },
          },
        },
      },
    },
  };

  expect(() => assertNoNotExists(condition)).toThrow(
    'NOT EXISTS is not supported on the client',
  );
});
