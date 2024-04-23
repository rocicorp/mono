import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import {describe, expect, test} from 'vitest';
import {
  expandSelection,
  expandSubqueries,
  reAliasAndBubbleSelections,
  type PrimaryKeys,
} from './expansion.js';
import {Normalized} from './normalize.js';
import {and, cond, or} from './query-test-util.js';

describe('zql/expansion', () => {
  type Case = {
    name: string;
    ast: AST;
    original: string; // Provided purely for test readability
    afterSubqueryExpansion: string;
    afterReAliasAndBubble?: string;
  };

  const PRIMARY_KEYS: PrimaryKeys = (table: string) => [
    `${table}_key`,
    `${table}_id`,
  ];

  const cases: Case[] = [
    {
      name: 'adds primary keys, preserved existing selects',
      ast: {
        table: 'issues',
        select: [['title', 'theTitle']],
      },
      original: `
      SELECT title AS "theTitle" FROM issues`,
      afterSubqueryExpansion: `
      SELECT issues_id AS issues_id, issues_key AS issues_key, title AS "theTitle" FROM issues
      `,
      afterReAliasAndBubble: `
      SELECT 
        issues.issues_id AS "issues/issues_id", 
        issues.issues_key AS "issues/issues_key", 
        issues.title AS "issues/title" 
      FROM issues
      `,
    },
    {
      name: 'adds query-relevant fields',
      ast: {
        table: 'issues',
        select: [['title', 'title']],
        where: or(
          cond('priority', '>', 3),
          and(cond('owner_id', '=', 1234), cond('component_id', '=', 2345)),
        ),
        orderBy: [['date', 'priority'], 'asc'],
      },
      original: `
      SELECT title AS title 
      FROM issues WHERE (priority > $1 OR (component_id = $2 AND owner_id = $3))
      ORDER BY date, priority asc
      `,
      afterSubqueryExpansion: `
      SELECT 
        component_id AS component_id,
        date AS date,
        issues_id AS issues_id,
        issues_key AS issues_key,
        owner_id AS owner_id,
        priority AS priority,
        title AS title
      FROM issues WHERE (priority > $1 OR (component_id = $2 AND owner_id = $3))
      ORDER BY date, priority asc
      `,
      afterReAliasAndBubble: `
      SELECT 
        issues.component_id AS "issues/component_id",
        issues.date AS "issues/date",
        issues.issues_id AS "issues/issues_id",
        issues.issues_key AS "issues/issues_key",
        issues.owner_id AS "issues/owner_id",
        issues.priority AS "issues/priority", 
        issues.title AS "issues/title"
      FROM issues WHERE (priority > $1 OR (component_id = $2 AND owner_id = $3))
      ORDER BY issues.date, issues.priority asc
      `,
    },
    {
      name: 'propagates referenced ON fields into joins',
      ast: {
        table: 'issues',
        select: [
          ['title', 'title'],
          ['owner.name', 'owner_name'],
        ],
        joins: [
          {
            type: 'inner',
            other: {table: 'users'},
            as: 'owner',
            on: ['owner.id', 'owner_id'],
          },
        ],
        orderBy: [['owner.level'], 'asc'],
      },
      original: `
      SELECT 
         owner.name AS owner_name,
         title AS title
      FROM issues INNER JOIN (users) AS owner ON owner.id = owner_id
      ORDER BY owner.level asc
      `,
      afterSubqueryExpansion: `
      SELECT 
        issues_id AS issues_id,     -- Added as PRIMARY_KEY
        issues_key AS issues_key,   -- Added as PRIMARY_KEY
        owner.name AS owner_name,   -- Original SELECT
        owner_id AS owner_id,       -- AS owner ON owner.id = owner_id
        title AS title              -- Original SELECT
      FROM issues INNER JOIN
        (SELECT
           id AS id,                -- AS owner ON owner.id = owner_id
           level AS level,          -- ORDER BY owner.level
           name AS name,            -- SELECT owner.name
           users_id AS users_id,    -- Added as PRIMARY_KEY
           users_key AS users_key   -- Added as PRIMARY_KEY
         FROM users) 
      AS owner ON owner.id = owner_id
      ORDER BY owner.level asc
      `,
      afterReAliasAndBubble: `
      SELECT 
        issues.issues_id AS "issues/issues_id",
        issues.issues_key AS "issues/issues_key",
        issues.owner_id AS "issues/owner_id",
        issues.title AS "issues/title",
        owner."users/id" AS "owner/users/id",
        owner."users/level" AS "owner/users/level",
        owner."users/name" AS "owner/users/name",
        owner."users/users_id" AS "owner/users/users_id",
        owner."users/users_key" AS "owner/users/users_key"
      FROM issues INNER JOIN (SELECT 
        users.id AS "users/id",
        users.level AS "users/level",
        users.name AS "users/name",
        users.users_id AS "users/users_id",
        users.users_key AS "users/users_key"
      FROM users) AS owner ON owner."users/id" = issues.owner_id
      ORDER BY owner."users/level" asc
      `,
    },
    {
      name: 'propagates fields from join subqueries',
      ast: {
        table: 'issues',
        select: [
          ['title', 'title'],
          ['owner.name', 'owner_name'],
        ],
        joins: [
          {
            type: 'inner',
            other: {
              select: [['name', 'name']],
              table: 'users',
              where: cond('level', '>', 3),
            },
            as: 'owner',
            on: ['owner.id', 'owner_id'],
          },
        ],
      },
      original: `
      SELECT 
        owner.name AS owner_name, 
        title AS title 
      FROM issues INNER JOIN (SELECT 
          name AS name 
        FROM users WHERE level > $1) 
      AS owner ON owner.id = owner_id
      `,
      afterSubqueryExpansion: `
      SELECT 
        issues_id AS issues_id, 
        issues_key AS issues_key, 
        owner.name AS owner_name, 
        owner_id AS owner_id, 
        title AS title 
      FROM issues INNER JOIN (SELECT 
          id AS id, 
          level AS level, 
          name AS name, 
          users_id AS users_id, 
          users_key AS users_key
        FROM users WHERE level > $1) 
      AS owner ON owner.id = owner_id
      `,
      afterReAliasAndBubble: `
      SELECT 
        issues.issues_id AS "issues/issues_id",
        issues.issues_key AS "issues/issues_key",
        issues.owner_id AS "issues/owner_id",
        issues.title AS "issues/title",
        owner."users/id" AS "owner/users/id",
        owner."users/level" AS "owner/users/level",
        owner."users/name" AS "owner/users/name",
        owner."users/users_id" AS "owner/users/users_id",
        owner."users/users_key" AS "owner/users/users_key"
      FROM issues INNER JOIN (SELECT 
          users.id AS "users/id",
          users.level AS "users/level",
          users.name AS "users/name",
          users.users_id AS "users/users_id",
          users.users_key AS "users/users_key"
        FROM users WHERE level > $1)
      AS owner ON owner."users/id" = issues.owner_id
      `,
    },
    {
      name: 'nested self join',
      ast: {
        table: 'issue',
        select: [
          ['issue.title', 'title'],
          ['owner.name', 'name'],
        ],
        joins: [
          {
            type: 'inner',
            other: {
              table: 'users',
            },
            as: 'owner',
            on: ['issue.user_id', 'owner.id'],
          },
          {
            type: 'inner',
            other: {
              table: 'issue',
              joins: [
                {
                  type: 'inner',
                  other: {
                    table: 'users',
                  },
                  as: 'owner',
                  on: ['issue.user_id', 'owner.id'],
                },
              ],
            },
            as: 'parent',
            on: ['issue.parent_id', 'parent.id'],
          },
        ],
        orderBy: [['owner.awesomeness'], 'desc'],
      },
      original: `
      SELECT 
         issue.title AS title, 
         owner.name AS name 
      FROM issue 
      INNER JOIN (users) AS owner ON issue.user_id = owner.id 
      INNER JOIN (issue INNER JOIN (users) AS owner ON issue.user_id = owner.id) AS parent
      ON issue.parent_id = parent.id
      ORDER BY owner.awesomeness desc
      `,
      afterSubqueryExpansion: `
      SELECT 
        issue.title AS title,        -- Original SELECT
        issue_id AS issue_id,        -- Added as PRIMARY_KEY
        issue_key AS issue_key,      -- Added as PRIMARY_KEY
        owner.name AS name,          -- Original SELECT
        parent_id AS parent_id,      -- AS parent ON issue.parent_id = parent.id
        user_id AS user_id           -- AS owner ON issue.user_id = owner.id
      FROM issue INNER JOIN (SELECT 
        awesomeness AS awesomeness,  -- ORDER BY owner.awesomeness
        id AS id,                    -- AS owner ON issue.user_id = owner.id
        name AS name,                -- SELECT owner.name
        users_id AS users_id,        -- Added as PRIMARY_KEY
        users_key AS users_key       -- Added as PRIMARY_KEY
      FROM users) AS owner ON issue.user_id = owner.id 
      INNER JOIN (SELECT 
        id AS id,                    -- AS parent ON issue.parent_id = parent.id
        issue_id AS issue_id,        -- Added as PRIMARY_KEY
        issue_key AS issue_key,      -- Added as PRIMARY_KEY
        user_id AS user_id           -- AS owner ON issue.user_id = owner.id) 
      FROM issue INNER JOIN (SELECT 
          id AS id,                  -- AS owner ON issue.user_id = owner.id) 
          users_id AS users_id,      -- Added as PRIMARY_KEY
          users_key AS users_key     -- Added as PRIMARY_KEY
        FROM users) 
        AS owner ON issue.user_id = owner.id) 
      AS parent ON issue.parent_id = parent.id
      ORDER BY owner.awesomeness desc
      `,
      afterReAliasAndBubble: `
      SELECT
        issue.issue_id                 AS "issue/issue_id",
        issue.issue_key                AS "issue/issue_key",
        issue.parent_id                AS "issue/parent_id",
        issue.title                    AS "issue/title",
        issue.user_id                  AS "issue/user_id",
        owner."users/awesomeness"      AS "owner/users/awesomeness",
        owner."users/id"               AS "owner/users/id",
        owner."users/name"             AS "owner/users/name",
        owner."users/users_id"         AS "owner/users/users_id",
        owner."users/users_key"        AS "owner/users/users_key",
        parent."issue/id"              AS "parent/issue/id",
        parent."issue/issue_id"        AS "parent/issue/issue_id",
        parent."issue/issue_key"       AS "parent/issue/issue_key",
        parent."issue/user_id"         AS "parent/issue/user_id",
        parent."owner/users/id"        AS "parent/owner/users/id",
        parent."owner/users/users_id"  AS "parent/owner/users/users_id",
        parent."owner/users/users_key" AS "parent/owner/users/users_key"
      FROM issue INNER JOIN (SELECT 
        users.awesomeness              AS "users/awesomeness",
        users.id                       AS "users/id",
        users.name                     AS "users/name",
        users.users_id                 AS "users/users_id",
        users.users_key                AS "users/users_key"
      FROM users) AS owner ON issue.user_id = owner."users/id"
      INNER JOIN (SELECT 
        issue.id                       AS "issue/id",
        issue.issue_id                 AS "issue/issue_id",
        issue.issue_key                AS "issue/issue_key",
        issue.user_id                  AS "issue/user_id",
        owner."users/id"               AS "owner/users/id",
        owner."users/users_id"         AS "owner/users/users_id",
        owner."users/users_key"        AS "owner/users/users_key"
      FROM issue INNER JOIN (SELECT
          users.id                     AS "users/id",
          users.users_id               AS "users/users_id",
          users.users_key              AS "users/users_key"
        FROM users)
        AS owner ON issue.user_id = owner."users/id")
      AS parent ON issue.parent_id = parent."issue/id"
      ORDER BY owner."users/awesomeness" desc
      `,
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      expect(new Normalized(c.ast).query().query).toBe(
        stripCommentsAndWhitespace(c.original),
      );
      const expanded = expandSubqueries(c.ast, PRIMARY_KEYS, new Set());
      expect(new Normalized(expanded).query().query).toBe(
        stripCommentsAndWhitespace(c.afterSubqueryExpansion),
      );
      const reAliased = reAliasAndBubbleSelections(expanded, new Map());
      expect(new Normalized(reAliased).query().query).toBe(
        stripCommentsAndWhitespace(c.afterReAliasAndBubble),
      );

      // Run the whole function.
      const expandedAndReAliased = expandSelection(c.ast, PRIMARY_KEYS);
      expect(new Normalized(expandedAndReAliased).query().query).toBe(
        stripCommentsAndWhitespace(c.afterReAliasAndBubble),
      );
    });
  }
});

function stripCommentsAndWhitespace(query: string = '') {
  return query
    .trim()
    .replaceAll(/--.*\n/g, '')
    .replaceAll(/\s+/g, ' ');
}
