import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import {describe, expect, test} from 'vitest';
import {deaggregateArrays} from './deaggregation.js';
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
        select: [[['issues', 'id'], 'issues.id']],
        aggregate: [
          {
            aggregate: 'array',
            field: ['issueLabels', 'labelID'],
            alias: 'ignored',
          },
        ],
        joins: [
          {
            type: 'left',
            other: {table: 'issueLabels'},
            as: 'issueLabels',
            on: [
              ['issues', 'id'],
              ['issueLabels', 'issueID'],
            ],
          },
        ],
        groupBy: [['issues', 'id']],
        orderBy: [[['issues', 'modified'], 'desc']],
      },
      original: `
      SELECT 
        issues.id AS "issues.id", 
        array_agg("issueLabels"."labelID") AS "array_agg(""issueLabels"".""labelID"")" 
      FROM issues 
      LEFT JOIN "issueLabels" AS "issueLabels" ON issues.id = "issueLabels"."issueID" 
      GROUP BY issues.id ORDER BY issues.modified desc
      `,
      afterDeaggregation: `
      SELECT 
         "issueLabels"."labelID" AS "labelID", 
         issues."issues.id" AS "issues.id" FROM 
       (SELECT issues.id AS "issues.id" FROM issues 
        ORDER BY issues.modified desc) 
      AS issues 
      LEFT JOIN "issueLabels" AS "issueLabels" 
      ON issues."issues.id" = "issueLabels"."issueID"
      `,
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      expect(new Normalized(c.ast).query().query).toBe(
        stripCommentsAndWhitespace(c.original),
      );
      const deaggregated = deaggregateArrays(c.ast, () => true);
      expect(new Normalized(deaggregated).query().query).toBe(
        stripCommentsAndWhitespace(c.afterDeaggregation ?? c.original),
      );
    });
  }
});
