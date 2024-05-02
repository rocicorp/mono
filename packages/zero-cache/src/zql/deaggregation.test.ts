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
        select: [['title', 'theTitle']],
      },
      original: `
      SELECT title AS "theTitle" FROM issues`,
    },
    {
      name: 'array in top-level select',
      ast: {
        table: 'issues',
        select: [['title', 'theTitle']],
        aggregate: [{aggregate: 'array', field: 'label', alias: 'ignored'}],
        groupBy: ['title'],
      },
      original: `
      SELECT title AS "theTitle", array_agg(label) AS "array_agg(label)" 
        FROM issues GROUP BY title`,
      afterDeaggregation: `
      SELECT label AS label, title AS "theTitle" FROM issues
      `,
    },
    {
      name: 'array in nested select',
      ast: {
        table: 'issues',
        select: [['title', 'theTitle']],
        aggregate: [{aggregate: 'array', field: 'label', alias: 'ignored'}],
        joins: [
          {
            type: 'inner',
            other: {
              table: 'users',
              aggregate: [{aggregate: 'array', field: 'role', alias: 'igno'}],
              groupBy: ['id'],
            },
            as: 'users',
            on: ['issues.user_id', 'users.id'],
          },
        ],
        groupBy: ['title'],
      },
      original: `
      SELECT title AS "theTitle", array_agg(label) AS "array_agg(label)" FROM issues
        INNER JOIN SELECT array_agg(role) AS "array_agg(role)" FROM users GROUP BY id
        AS users ON issues.user_id = users.id
      GROUP BY title      
      `,
      afterDeaggregation: `
      SELECT label AS label, title AS "theTitle" FROM issues
        INNER JOIN (SELECT role AS role FROM users) AS users 
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
