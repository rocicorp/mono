import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import {describe, expect, test} from 'vitest';
import {deaggregate} from './deaggregation.js';
import {Normalized} from './normalize.js';
import {stripCommentsAndWhitespace} from './query-test-util.js';

describe('zql/deaggregation', () => {
  type Case = {
    name: string;
    ast: AST;
    original: string; // Provided purely for test readability
    afterDeaggregation?: string;
  };

  const cases: Case[] = [
    {
      name: 'nothing to deaggregate',
      ast: {
        table: 'issues',
        select: [[['issues', 'title'], 'theTitle']],
      },
      original: `
      SELECT issues.title AS "theTitle" FROM issues`,
    },
    {
      name: 'array in top-level select',
      ast: {
        table: 'issues',
        select: [[['issues', 'title'], 'theTitle']],
        aggregate: [
          {aggregate: 'array', field: ['issues', 'label'], alias: 'ignored'},
        ],
        groupBy: [['issues', 'title']],
      },
      original: `
      SELECT issues.title AS "theTitle", array_agg(issues.label) AS "array_agg(issues.label)" 
        FROM issues GROUP BY issues.title`,
      afterDeaggregation: `
      SELECT issues.label AS label, issues.title AS "theTitle" FROM issues
      `,
    },
    {
      name: 'array in nested select',
      ast: {
        table: 'issues',
        select: [[['issues', 'title'], 'theTitle']],
        aggregate: [
          {aggregate: 'array', field: ['issues', 'label'], alias: 'ignored'},
        ],
        joins: [
          {
            type: 'inner',
            other: {
              table: 'users',
              aggregate: [
                {aggregate: 'array', field: ['users', 'role'], alias: 'igno'},
              ],
              groupBy: [['users', 'id']],
            },
            as: 'users',
            on: [
              ['issues', 'user_id'],
              ['users', 'id'],
            ],
          },
        ],
        groupBy: [['issues', 'title']],
      },
      original: `
      SELECT issues.title AS "theTitle", array_agg(issues.label) AS "array_agg(issues.label)" FROM issues
        INNER JOIN SELECT array_agg(users.role) AS "array_agg(users.role)" FROM users GROUP BY users.id
        AS users ON issues.user_id = users.id
      GROUP BY issues.title      
      `,
      afterDeaggregation: `
      SELECT issues.label AS label, issues.title AS "theTitle" FROM issues
        INNER JOIN (SELECT users.role AS role FROM users) AS users 
      ON issues.user_id = users.id
      `,
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      expect(new Normalized(c.ast).query().query).toBe(
        stripCommentsAndWhitespace(c.original),
      );
      const deaggregated = deaggregate(c.ast);
      expect(new Normalized(deaggregated).query().query).toBe(
        stripCommentsAndWhitespace(c.afterDeaggregation ?? c.original),
      );
    });
  }
});
