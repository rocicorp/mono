import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import {describe, expect, test} from 'vitest';
import {
  expandSelection,
  expandSubqueries,
  reAliasAndBubbleSelections,
  type RequiredColumns,
} from './expansion.js';
import {Normalized} from './normalize.js';
import {and, cond, or, stripCommentsAndWhitespace} from './query-test-util.js';

describe('zql/expansion', () => {
  type Case = {
    name: string;
    ast: AST;
    original: string; // Provided purely for test readability
    afterSubqueryExpansion: string;
    afterReAliasAndBubble?: string;
  };

  const REQUIRED_COLUMNS: RequiredColumns = (
    schema: string | undefined,
    table: string,
  ) => [`${schema ? schema + '_' : ''}${table}_key`, `_0_version`];

  const cases: Case[] = [
    {
      name: 'adds primary keys, preserved existing selects',
      ast: {
        table: 'issues',
        select: [[['issues', 'title'], 'theTitle']],
      },
      original: `
      SELECT title AS "theTitle" FROM issues`,
      afterSubqueryExpansion: `
      SELECT 
        _0_version AS _0_version,
        issues_key AS issues_key,
        title AS "theTitle"
      FROM issues
      `,
      afterReAliasAndBubble: `
      SELECT 
        public.issues._0_version AS "public/issues/_0_version", 
        public.issues.issues_key AS "public/issues/issues_key", 
        public.issues.title AS "public/issues/title" 
      FROM issues
      `,
    },
    {
      name: 'passes schema',
      ast: {
        schema: 'zero',
        table: 'clients',
        select: [[['clients', 'id'], 'id']],
      },
      original: `
      SELECT id AS id FROM zero.clients`,
      afterSubqueryExpansion: `
      SELECT 
        _0_version AS _0_version,
        id AS id,
        zero_clients_key AS zero_clients_key
      FROM zero.clients
      `,
      afterReAliasAndBubble: `
      SELECT 
        zero.clients._0_version AS "zero/clients/_0_version",
        zero.clients.id AS "zero/clients/id",
        zero.clients.zero_clients_key AS "zero/clients/zero_clients_key"
      FROM zero.clients
      `,
    },
    {
      name: 'adds query-relevant fields',
      ast: {
        table: 'issues',
        select: [[['issues', 'title'], 'title']],
        where: or(
          cond(['issues', 'priority'], '>', 3),
          and(
            cond(['issues', 'owner_id'], '=', 1234),
            cond(['issues', 'component_id'], '=', 2345),
          ),
        ),
        orderBy: [
          [
            ['issues', 'date'],
            ['issues', 'priority'],
          ],
          'asc',
        ],
      },
      original: `
      SELECT title AS title 
      FROM issues WHERE (priority > $1 OR (component_id = $2 AND owner_id = $3))
      ORDER BY date asc, priority asc
      `,
      afterSubqueryExpansion: `
      SELECT
        _0_version AS _0_version,
        component_id AS component_id,
        date AS date,
        issues_key AS issues_key,
        owner_id AS owner_id,
        priority AS priority,
        title AS title
      FROM issues WHERE (priority > $1 OR (component_id = $2 AND owner_id = $3))
      ORDER BY date asc, priority asc
      `,
      afterReAliasAndBubble: `
      SELECT
        public.issues._0_version AS "public/issues/_0_version",
        public.issues.component_id AS "public/issues/component_id",
        public.issues.date AS "public/issues/date",
        public.issues.issues_key AS "public/issues/issues_key",
        public.issues.owner_id AS "public/issues/owner_id",
        public.issues.priority AS "public/issues/priority", 
        public.issues.title AS "public/issues/title"
      FROM issues WHERE (priority > $1 OR (component_id = $2 AND owner_id = $3))
      ORDER BY public.issues.date asc, public.issues.priority asc
      `,
    },
    {
      name: 'propagates referenced ON fields into joins',
      ast: {
        table: 'issues',
        select: [
          [['issues', 'title'], 'title'],
          [['owner', 'name'], 'owner_name'],
        ],
        joins: [
          {
            type: 'inner',
            other: {table: 'users'},
            as: 'owner',
            on: [
              ['owner', 'id'],
              ['issues', 'owner_id'],
            ],
          },
        ],
        orderBy: [[['owner', 'level']], 'asc'],
      },
      original: `
      SELECT 
         owner.name AS owner_name,
         title AS title
      FROM issues INNER JOIN users AS owner ON owner.id = owner_id
      ORDER BY owner.level asc
      `,
      afterSubqueryExpansion: `
      SELECT 
        _0_version AS _0_version,     -- Added by REQUIRED_COLUMNS
        issues_key AS issues_key,     -- Added by REQUIRED_COLUMNS
        owner.name AS owner_name,     -- Original SELECT
        owner_id AS owner_id,         -- AS owner ON owner.id = owner_id
        title AS title                -- Original SELECT
      FROM issues INNER JOIN
        (SELECT
           _0_version AS _0_version,  -- Added by REQUIRED_COLUMNS
           id AS id,                  -- AS owner ON owner.id = owner_id
           level AS level,            -- ORDER BY owner.level
           name AS name,              -- SELECT owner.name
           users_key AS users_key     -- Added by REQUIRED_COLUMNS
         FROM users) 
      AS owner ON owner.id = owner_id
      ORDER BY owner.level asc
      `,
      afterReAliasAndBubble: `
      SELECT 
        owner."public/users/_0_version" AS "owner/public/users/_0_version",
        owner."public/users/id" AS "owner/public/users/id",
        owner."public/users/level" AS "owner/public/users/level",
        owner."public/users/name" AS "owner/public/users/name",
        owner."public/users/users_key" AS "owner/public/users/users_key",
        public.issues._0_version AS "public/issues/_0_version",
        public.issues.issues_key AS "public/issues/issues_key",
        public.issues.owner_id AS "public/issues/owner_id",
        public.issues.title AS "public/issues/title"
      FROM issues INNER JOIN (SELECT 
        public.users._0_version AS "public/users/_0_version",
        public.users.id AS "public/users/id",
        public.users.level AS "public/users/level",
        public.users.name AS "public/users/name",
        public.users.users_key AS "public/users/users_key"
      FROM users) AS owner ON owner."public/users/id" = public.issues.owner_id
      ORDER BY owner."public/users/level" asc
      `,
    },
    {
      name: 'propagates fields from join subqueries',
      ast: {
        table: 'issues',
        select: [
          [['issues', 'title'], 'title'],
          [['owner', 'name'], 'owner_name'],
        ],
        joins: [
          {
            type: 'inner',
            other: {
              select: [[['users', 'name'], 'name']],
              table: 'users',
              where: cond(['users', 'level'], '>', 3),
            },
            as: 'owner',
            on: [
              ['owner', 'id'],
              ['issues', 'owner_id'],
            ],
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
        _0_version AS _0_version, 
        issues_key AS issues_key, 
        owner.name AS owner_name, 
        owner_id AS owner_id, 
        title AS title 
      FROM issues INNER JOIN (SELECT 
          _0_version AS _0_version,
          id AS id, 
          level AS level, 
          name AS name, 
          users_key AS users_key
        FROM users WHERE level > $1) 
      AS owner ON owner.id = owner_id
      `,
      afterReAliasAndBubble: `
      SELECT 
        owner."public/users/_0_version" AS "owner/public/users/_0_version",
        owner."public/users/id" AS "owner/public/users/id",
        owner."public/users/level" AS "owner/public/users/level",
        owner."public/users/name" AS "owner/public/users/name",
        owner."public/users/users_key" AS "owner/public/users/users_key",
        public.issues._0_version AS "public/issues/_0_version",
        public.issues.issues_key AS "public/issues/issues_key",
        public.issues.owner_id AS "public/issues/owner_id",
        public.issues.title AS "public/issues/title"
      FROM issues INNER JOIN (SELECT
          public.users._0_version AS "public/users/_0_version", 
          public.users.id AS "public/users/id",
          public.users.level AS "public/users/level",
          public.users.name AS "public/users/name",
          public.users.users_key AS "public/users/users_key"
        FROM users WHERE level > $1)
      AS owner ON owner."public/users/id" = public.issues.owner_id
      `,
    },
    {
      name: 'nested self join',
      ast: {
        table: 'issue',
        select: [
          [['issue', 'title'], 'title'],
          [['owner', 'name'], 'name'],
        ],
        joins: [
          {
            type: 'inner',
            other: {
              table: 'users',
            },
            as: 'owner',
            on: [
              ['issue', 'user_id'],
              ['owner', 'id'],
            ],
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
                  on: [
                    ['issue', 'user_id'],
                    ['owner', 'id'],
                  ],
                },
              ],
            },
            as: 'parent',
            on: [
              ['issue', 'parent_id'],
              ['parent', 'id'],
            ],
          },
        ],
        orderBy: [[['owner', 'awesomeness']], 'desc'],
      },
      original: `
      SELECT 
         issue.title AS title, 
         owner.name AS name 
      FROM issue 
      INNER JOIN users AS owner ON issue.user_id = owner.id 
      INNER JOIN issue INNER JOIN users AS owner ON issue.user_id = owner.id AS parent
      ON issue.parent_id = parent.id
      ORDER BY owner.awesomeness desc
      `,
      afterSubqueryExpansion: `
      SELECT 
        _0_version AS _0_version,    -- Added by REQUIRED_COLUMNS
        issue.title AS title,        -- Original SELECT
        issue_key AS issue_key,      -- Added by REQUIRED_COLUMNS
        owner.name AS name,          -- Original SELECT
        parent_id AS parent_id,      -- AS parent ON issue.parent_id = parent.id
        user_id AS user_id           -- AS owner ON issue.user_id = owner.id
      FROM issue INNER JOIN (SELECT 
        _0_version AS _0_version,    -- Added by REQUIRED_COLUMNS
        awesomeness AS awesomeness,  -- ORDER BY owner.awesomeness
        id AS id,                    -- AS owner ON issue.user_id = owner.id
        name AS name,                -- SELECT owner.name
        users_key AS users_key       -- Added by REQUIRED_COLUMNS
      FROM users) AS owner ON issue.user_id = owner.id 
      INNER JOIN (SELECT 
        _0_version AS _0_version,    -- Added by REQUIRED_COLUMNS
        id AS id,                    -- AS parent ON issue.parent_id = parent.id
        issue_key AS issue_key,      -- Added by REQUIRED_COLUMNS
        user_id AS user_id           -- AS owner ON issue.user_id = owner.id) 
      FROM issue INNER JOIN (SELECT 
          _0_version AS _0_version,  -- Added by REQUIRED_COLUMNS
          id AS id,                  -- AS owner ON issue.user_id = owner.id) 
          users_key AS users_key     -- Added by REQUIRED_COLUMNS
        FROM users) 
        AS owner ON issue.user_id = owner.id) 
      AS parent ON issue.parent_id = parent.id
      ORDER BY owner.awesomeness desc
      `,
      afterReAliasAndBubble: `
      SELECT
        owner."public/users/_0_version"         AS "owner/public/users/_0_version",
        owner."public/users/awesomeness"        AS "owner/public/users/awesomeness",
        owner."public/users/id"                 AS "owner/public/users/id",
        owner."public/users/name"               AS "owner/public/users/name",
        owner."public/users/users_key"          AS "owner/public/users/users_key",
        parent."owner/public/users/_0_version"  AS "parent/owner/public/users/_0_version",
        parent."owner/public/users/id"          AS "parent/owner/public/users/id",
        parent."owner/public/users/users_key"   AS "parent/owner/public/users/users_key",
        parent."public/issue/_0_version"        AS "parent/public/issue/_0_version",
        parent."public/issue/id"                AS "parent/public/issue/id",
        parent."public/issue/issue_key"         AS "parent/public/issue/issue_key",
        parent."public/issue/user_id"           AS "parent/public/issue/user_id",
        public.issue._0_version                 AS "public/issue/_0_version",
        public.issue.issue_key                  AS "public/issue/issue_key",
        public.issue.parent_id                  AS "public/issue/parent_id",
        public.issue.title                      AS "public/issue/title",
        public.issue.user_id                    AS "public/issue/user_id"
      FROM issue INNER JOIN (SELECT 
        public.users._0_version                 AS "public/users/_0_version",
        public.users.awesomeness                AS "public/users/awesomeness",
        public.users.id                         AS "public/users/id",
        public.users.name                       AS "public/users/name",
        public.users.users_key                  AS "public/users/users_key"
      FROM users) AS owner ON public.issue.user_id = owner."public/users/id"
      INNER JOIN (SELECT 
        owner."public/users/_0_version"         AS "owner/public/users/_0_version",
        owner."public/users/id"                 AS "owner/public/users/id",
        owner."public/users/users_key"          AS "owner/public/users/users_key",
        public.issue._0_version                 AS "public/issue/_0_version",
        public.issue.id                         AS "public/issue/id",
        public.issue.issue_key                  AS "public/issue/issue_key",
        public.issue.user_id                    AS "public/issue/user_id"
      FROM issue INNER JOIN (SELECT
          public.users._0_version               AS "public/users/_0_version",
          public.users.id                       AS "public/users/id",
          public.users.users_key                AS "public/users/users_key"
        FROM users)
        AS owner ON public.issue.user_id = owner."public/users/id")
      AS parent ON public.issue.parent_id = parent."public/issue/id"
      ORDER BY owner."public/users/awesomeness" desc
      `,
    },
    {
      name: 'self join with aliased selects',
      ast: {
        select: [
          [['issues', 'id'], 'id'],
          [['issues', 'title'], 'title'],
          [['owner', 'name'], 'owner_name'],
          [['parent', 'title'], 'parent_title'],
          [['parent', 'owner_name'], 'parent_owner'],
        ],
        table: 'issues',
        joins: [
          {
            type: 'inner',
            other: {table: 'users'},
            as: 'owner',
            on: [
              ['issues', 'owner_id'],
              ['owner', 'id'],
            ],
          },
          {
            type: 'inner',
            other: {
              select: [
                [['issues', 'id'], 'issues_id'],
                [['issues', 'title'], 'title'],
                [['owner', 'name'], 'owner_name'],
              ],
              table: 'issues',
              joins: [
                {
                  type: 'inner',
                  other: {table: 'users'},
                  as: 'owner',
                  on: [
                    ['issues', 'owner_id'],
                    ['owner', 'id'],
                  ],
                },
              ],
            },
            as: 'parent',
            on: [
              ['issues', 'parent_id'],
              ['parent', 'issues_id'],
            ],
          },
        ],
      },
      original: `
      SELECT 
        issues.id AS id,
        issues.title AS title,
        owner.name AS owner_name,
        parent.owner_name AS parent_owner,
        parent.title AS parent_title
      FROM issues 
      INNER JOIN users AS owner ON issues.owner_id = owner.id 
      INNER JOIN (SELECT 
          issues.id AS issues_id,
          owner.name AS owner_name,
          title AS title
        FROM issues
        INNER JOIN users AS owner ON issues.owner_id = owner.id) 
      AS parent ON issues.parent_id = parent.issues_id`,
      afterSubqueryExpansion: `
      SELECT 
        _0_version AS _0_version,
        issues.id AS id,
        issues.title AS title,
        issues_key AS issues_key,
        owner.name AS owner_name,
        owner_id AS owner_id,
        parent.owner_name AS parent_owner,
        parent.title AS parent_title,
        parent_id AS parent_id FROM issues
      INNER JOIN (SELECT 
        _0_version AS _0_version,
        id AS id,
        name AS name,
        users_key AS users_key FROM users) AS owner ON issues.owner_id = owner.id
      INNER JOIN (SELECT 
        _0_version AS _0_version,
        issues.id AS issues_id,
        issues_key AS issues_key,
        owner.name AS owner_name,
        owner_id AS owner_id,
        title AS title FROM issues INNER JOIN (SELECT 
          _0_version AS _0_version, 
          id AS id, 
          name AS name, 
          users_key AS users_key FROM users)
        AS owner ON issues.owner_id = owner.id)
      AS parent ON issues.parent_id = parent.issues_id
      `,
      afterReAliasAndBubble: `
      SELECT 
        owner."public/users/_0_version"         AS "owner/public/users/_0_version",
        owner."public/users/id"                 AS "owner/public/users/id",
        owner."public/users/name"               AS "owner/public/users/name",
        owner."public/users/users_key"          AS "owner/public/users/users_key",
        parent."owner/public/users/_0_version"  AS "parent/owner/public/users/_0_version",
        parent."owner/public/users/id"          AS "parent/owner/public/users/id",
        parent."owner/public/users/name"        AS "parent/owner/public/users/name",
        parent."owner/public/users/users_key"   AS "parent/owner/public/users/users_key",
        parent."public/issues/_0_version"       AS "parent/public/issues/_0_version",
        parent."public/issues/id"               AS "parent/public/issues/id",
        parent."public/issues/issues_key"       AS "parent/public/issues/issues_key",
        parent."public/issues/owner_id"         AS "parent/public/issues/owner_id",
        parent."public/issues/title"            AS "parent/public/issues/title",
        public.issues._0_version                AS "public/issues/_0_version",
        public.issues.id                        AS "public/issues/id",
        public.issues.issues_key                AS "public/issues/issues_key",
        public.issues.owner_id                  AS "public/issues/owner_id",
        public.issues.parent_id                 AS "public/issues/parent_id",
        public.issues.title                     AS "public/issues/title"
      FROM issues INNER JOIN (SELECT 
        public.users._0_version                 AS "public/users/_0_version",
        public.users.id                         AS "public/users/id",
        public.users.name                       AS "public/users/name",
        public.users.users_key                  AS "public/users/users_key"
      FROM users) AS owner ON public.issues.owner_id = owner."public/users/id" 
      INNER JOIN (SELECT 
        owner."public/users/_0_version"         AS "owner/public/users/_0_version",
        owner."public/users/id"                 AS "owner/public/users/id",
        owner."public/users/name"               AS "owner/public/users/name",
        owner."public/users/users_key"          AS "owner/public/users/users_key",
        public.issues._0_version                AS "public/issues/_0_version",
        public.issues.id                        AS "public/issues/id",
        public.issues.issues_key                AS "public/issues/issues_key",
        public.issues.owner_id                  AS "public/issues/owner_id",
        public.issues.title                     AS "public/issues/title"
      FROM issues INNER JOIN (SELECT 
          public.users._0_version               AS "public/users/_0_version",
          public.users.id                       AS "public/users/id",
          public.users.name                     AS "public/users/name",
          public.users.users_key                AS "public/users/users_key"
        FROM users) AS owner ON public.issues.owner_id = owner."public/users/id") 
      AS parent ON public.issues.parent_id = parent."public/issues/id"
      `,
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      expect(new Normalized(c.ast).query().query).toBe(
        stripCommentsAndWhitespace(c.original),
      );
      const expanded = expandSubqueries(c.ast, REQUIRED_COLUMNS, new Set());
      expect(new Normalized(expanded).query().query).toBe(
        stripCommentsAndWhitespace(c.afterSubqueryExpansion),
      );
      const reAliased = reAliasAndBubbleSelections(expanded, new Map());
      expect(new Normalized(reAliased).query().query).toBe(
        stripCommentsAndWhitespace(c.afterReAliasAndBubble),
      );

      // Run the whole function.
      const expandedAndReAliased = expandSelection(c.ast, REQUIRED_COLUMNS);
      expect(new Normalized(expandedAndReAliased).query().query).toBe(
        stripCommentsAndWhitespace(c.afterReAliasAndBubble),
      );
    });
  }
});
