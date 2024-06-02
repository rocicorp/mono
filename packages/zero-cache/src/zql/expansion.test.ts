import {describe, expect, test} from 'vitest';
import {
  expandSelection,
  expandSubqueries,
  reAliasAndBubbleSelections,
  type RequiredColumns,
} from './expansion.js';
import {Normalized} from './normalize.js';
import {and, cond, or, stripCommentsAndWhitespace} from './query-test-util.js';
import type {ServerAST} from './server-ast.js';

describe('zql/expansion', () => {
  type Case = {
    name: string;
    ast: ServerAST;
    original: string; // Provided purely for test readability
    afterSubqueryExpansion: string;
    afterReAliasAndBubble?: string;
  };

  const REQUIRED_COLUMNS: RequiredColumns = (
    schema: string | undefined,
    table: string,
  ) => [
    [table, `${schema ? schema + '_' : ''}${table}_key`],
    [table, `_0_version`],
  ];

  const cases: Case[] = [
    {
      name: 'adds primary keys, preserved existing selects',
      ast: {
        table: 'issues',
        select: [[['issues', 'title'], 'theTitle']],
      },
      original: `
      SELECT issues.title AS "theTitle" FROM issues`,
      afterSubqueryExpansion: `
      SELECT 
        issues._0_version AS _0_version,
        issues.issues_key AS issues_key,
        issues.title AS "theTitle"
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
      SELECT clients.id AS id FROM zero.clients`,
      afterSubqueryExpansion: `
      SELECT 
        clients._0_version AS _0_version,
        clients.id AS id,
        clients.zero_clients_key AS zero_clients_key
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
      SELECT issues.title AS title 
      FROM issues WHERE (issues.priority > $1 OR (issues.component_id = $2 AND issues.owner_id = $3))
      ORDER BY issues.date asc, issues.priority asc
      `,
      afterSubqueryExpansion: `
      SELECT
        issues._0_version AS _0_version,
        issues.component_id AS component_id,
        issues.date AS date,
        issues.issues_key AS issues_key,
        issues.owner_id AS owner_id,
        issues.priority AS priority,
        issues.title AS title
      FROM issues WHERE (issues.priority > $1 OR (issues.component_id = $2 AND issues.owner_id = $3))
      ORDER BY issues.date asc, issues.priority asc
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
      FROM issues WHERE (issues.priority > $1 OR (issues.component_id = $2 AND issues.owner_id = $3))
      ORDER BY public.issues.date asc, public.issues.priority asc
      `,
    },
    {
      name: 'handles subqueries',
      ast: {
        table: 'issues',
        select: [[['issues', 'title'], 'title']],
        subQuery: {
          alias: 'issues',
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
        },
      },
      original: `
      SELECT issues.title AS title FROM 
        (SELECT issues.title AS title FROM issues 
          WHERE (issues.priority > $1 OR (issues.component_id = $2 AND issues.owner_id = $3)) 
          ORDER BY issues.date asc, issues.priority asc) AS issues
      `,
      afterSubqueryExpansion: `
      SELECT issues.title AS title FROM 
        (SELECT 
          issues._0_version AS _0_version, 
          issues.component_id AS component_id, 
          issues.date AS date, 
          issues.issues_key AS issues_key, 
          issues.owner_id AS owner_id, 
          issues.priority AS priority, 
          issues.title AS title 
        FROM issues 
        WHERE (issues.priority > $1 OR (issues.component_id = $2 AND issues.owner_id = $3)) 
        ORDER BY issues.date asc, issues.priority asc)
      AS issues
      `,
      afterReAliasAndBubble: `
      SELECT 
        issues."public/issues/_0_version" AS "issues/public/issues/_0_version", 
        issues."public/issues/component_id" AS "issues/public/issues/component_id", 
        issues."public/issues/date" AS "issues/public/issues/date", 
        issues."public/issues/issues_key" AS "issues/public/issues/issues_key", 
        issues."public/issues/owner_id" AS "issues/public/issues/owner_id", 
        issues."public/issues/priority" AS "issues/public/issues/priority", 
        issues."public/issues/title" AS "issues/public/issues/title" FROM
       (SELECT 
          public.issues._0_version AS "public/issues/_0_version", 
          public.issues.component_id AS "public/issues/component_id", 
          public.issues.date AS "public/issues/date", 
          public.issues.issues_key AS "public/issues/issues_key", 
          public.issues.owner_id AS "public/issues/owner_id", 
          public.issues.priority AS "public/issues/priority", 
          public.issues.title AS "public/issues/title" 
        FROM issues 
        WHERE (issues.priority > $1 OR (issues.component_id = $2 AND issues.owner_id = $3)) 
        ORDER BY public.issues.date asc, public.issues.priority asc) 
      AS issues
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
        issues.title AS title,
        owner.name AS owner_name
      FROM issues INNER JOIN users AS owner ON owner.id = issues.owner_id
      ORDER BY owner.level asc
      `,
      afterSubqueryExpansion: `
      SELECT 
        issues._0_version AS _0_version,     -- Added by REQUIRED_COLUMNS
        issues.issues_key AS issues_key,     -- Added by REQUIRED_COLUMNS
        issues.owner_id AS owner_id,         -- AS owner ON owner.id = owner_id
        issues.title AS title,                -- Original SELECT
        owner.name AS owner_name     -- Original SELECT
      FROM issues INNER JOIN
        (SELECT
           users._0_version AS _0_version,  -- Added by REQUIRED_COLUMNS
           users.id AS id,                  -- AS owner ON owner.id = owner_id
           users.level AS level,            -- ORDER BY owner.level
           users.name AS name,              -- SELECT owner.name
           users.users_key AS users_key     -- Added by REQUIRED_COLUMNS
         FROM users) 
      AS owner ON owner.id = issues.owner_id
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
      name: 'handles subqueries with joins',
      ast: {
        table: 'issues',
        select: [
          [['issues', 'id'], 'id'],
          [['labels', 'name'], 'label'],
        ],
        subQuery: {
          alias: 'issues',
          ast: {
            table: 'issues',
            select: [[['issues', 'id'], 'id']],
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
        },
        joins: [
          {
            type: 'left',
            other: {
              table: 'labels',
              select: [
                [['labels', 'id'], 'id'],
                [['labels', 'name'], 'name'],
              ],
            },
            as: 'labels',
            on: [
              ['labels', 'id'],
              ['issues', 'id'],
            ],
          },
        ],
      },
      original: `
      SELECT issues.id AS id, labels.name AS label FROM (SELECT issues.id AS id FROM issues WHERE (issues.priority > $1 OR (issues.component_id = $2 AND issues.owner_id = $3)) ORDER BY issues.date asc, issues.priority asc) AS issues LEFT JOIN (SELECT labels.id AS id, labels.name AS name FROM labels) AS labels ON labels.id = issues.id
      `,
      afterSubqueryExpansion: `
      SELECT issues.id AS id, labels.name AS label FROM (SELECT issues._0_version AS _0_version, issues.component_id AS component_id, issues.date AS date, issues.id AS id, issues.issues_key AS issues_key, issues.owner_id AS owner_id, issues.priority AS priority FROM issues WHERE (issues.priority > $1 OR (issues.component_id = $2 AND issues.owner_id = $3)) ORDER BY issues.date asc, issues.priority asc) AS issues LEFT JOIN (SELECT labels._0_version AS _0_version, labels.id AS id, labels.labels_key AS labels_key, labels.name AS name FROM labels) AS labels ON labels.id = issues.id
      `,
      afterReAliasAndBubble: `
      SELECT 
        issues."public/issues/_0_version" AS "issues/public/issues/_0_version",
        issues."public/issues/component_id" AS "issues/public/issues/component_id", 
        issues."public/issues/date" AS "issues/public/issues/date", 
        issues."public/issues/id" AS "issues/public/issues/id", 
        issues."public/issues/issues_key" AS "issues/public/issues/issues_key", 
        issues."public/issues/owner_id" AS "issues/public/issues/owner_id", 
        issues."public/issues/priority" AS "issues/public/issues/priority", 
        labels."public/labels/_0_version" AS "labels/public/labels/_0_version", 
        labels."public/labels/id" AS "labels/public/labels/id", 
        labels."public/labels/labels_key" AS "labels/public/labels/labels_key", 
        labels."public/labels/name" AS "labels/public/labels/name" FROM
       (SELECT 
          public.issues._0_version AS "public/issues/_0_version", 
          public.issues.component_id AS "public/issues/component_id", 
          public.issues.date AS "public/issues/date", 
          public.issues.id AS "public/issues/id", 
          public.issues.issues_key AS "public/issues/issues_key", 
          public.issues.owner_id AS "public/issues/owner_id", 
          public.issues.priority AS "public/issues/priority" 
        FROM issues 
        WHERE (issues.priority > $1 OR (issues.component_id = $2 AND issues.owner_id = $3)) 
        ORDER BY public.issues.date asc, public.issues.priority asc) 
      AS issues 
      LEFT JOIN
       (SELECT 
          public.labels._0_version AS "public/labels/_0_version", 
          public.labels.id AS "public/labels/id", 
          public.labels.labels_key AS "public/labels/labels_key", 
          public.labels.name AS "public/labels/name" FROM labels) 
      AS labels 
      ON labels."public/labels/id" = issues."public/issues/id"
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
        issues.title AS title,
        owner.name AS owner_name
      FROM issues INNER JOIN (SELECT 
          users.name AS name 
        FROM users WHERE users.level > $1) 
      AS owner ON owner.id = issues.owner_id
      `,
      afterSubqueryExpansion: `
      SELECT 
        issues._0_version AS _0_version, 
        issues.issues_key AS issues_key, 
        issues.owner_id AS owner_id,
        issues.title AS title,
        owner.name AS owner_name
      FROM issues INNER JOIN (SELECT 
          users._0_version AS _0_version,
          users.id AS id, 
          users.level AS level, 
          users.name AS name, 
          users.users_key AS users_key
        FROM users WHERE users.level > $1) 
      AS owner ON owner.id = issues.owner_id
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
        FROM users WHERE users.level > $1)
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
        issue._0_version AS _0_version,    -- Added by REQUIRED_COLUMNS
        issue.issue_key AS issue_key,      -- Added by REQUIRED_COLUMNS
        issue.parent_id AS parent_id,      -- AS parent ON issue.parent_id = parent.id
        issue.title AS title,        -- Original SELECT
        issue.user_id AS user_id,           -- AS owner ON issue.user_id = owner.id
        owner.name AS name          -- Original SELECT
      FROM issue INNER JOIN (SELECT 
        users._0_version AS _0_version,    -- Added by REQUIRED_COLUMNS
        users.awesomeness AS awesomeness,  -- ORDER BY owner.awesomeness
        users.id AS id,                    -- AS owner ON issue.user_id = owner.id
        users.name AS name,                -- SELECT owner.name
        users.users_key AS users_key       -- Added by REQUIRED_COLUMNS
      FROM users) AS owner ON issue.user_id = owner.id 
      INNER JOIN (SELECT 
        issue._0_version AS _0_version,    -- Added by REQUIRED_COLUMNS
        issue.id AS id,                    -- AS parent ON issue.parent_id = parent.id
        issue.issue_key AS issue_key,      -- Added by REQUIRED_COLUMNS
        issue.user_id AS user_id           -- AS owner ON issue.user_id = owner.id) 
      FROM issue INNER JOIN (SELECT 
          users._0_version AS _0_version,  -- Added by REQUIRED_COLUMNS
          users.id AS id,                  -- AS owner ON issue.user_id = owner.id) 
          users.users_key AS users_key     -- Added by REQUIRED_COLUMNS
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
          issues.title AS title,
          owner.name AS owner_name
        FROM issues
        INNER JOIN users AS owner ON issues.owner_id = owner.id) 
      AS parent ON issues.parent_id = parent.issues_id`,
      afterSubqueryExpansion: `
      SELECT 
        issues._0_version AS _0_version,
        issues.id AS id,
        issues.issues_key AS issues_key,
        issues.owner_id AS owner_id,
        issues.parent_id AS parent_id,
        issues.title AS title,
        owner.name AS owner_name,
        parent.owner_name AS parent_owner,
        parent.title AS parent_title
        FROM issues
      INNER JOIN (SELECT 
        users._0_version AS _0_version,
        users.id AS id,
        users.name AS name,
        users.users_key AS users_key FROM users) AS owner ON issues.owner_id = owner.id
      INNER JOIN (SELECT 
        issues._0_version AS _0_version,
        issues.id AS issues_id,
        issues.issues_key AS issues_key,
        issues.owner_id AS owner_id,
        issues.title AS title,
        owner.name AS owner_name
        FROM issues INNER JOIN (SELECT 
          users._0_version AS _0_version, 
          users.id AS id, 
          users.name AS name, 
          users.users_key AS users_key FROM users)
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
      const expanded = expandSubqueries(c.ast, REQUIRED_COLUMNS, []);
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
