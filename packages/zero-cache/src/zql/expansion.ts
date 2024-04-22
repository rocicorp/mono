import type {AST, Condition} from '@rocicorp/zql/src/zql/ast/ast.js';
import {assert} from 'shared/src/asserts.js';
import {union} from 'shared/src/set-utils.js';

export type PrimaryKeys = (table: string) => readonly string[];

/**
 * Expands the selection of a query to include all of the column values necessary to
 * recompute the query on the client, and aliases the columns so that the result can
 * be easily deconstructed into its constituent rows.
 *
 * ### Self Describing Aliases
 *
 * Given that a single result from JOIN query can include rows from multiple tables,
 * and even multiple rows from a single table in the case of self JOINs, a mechanism
 * for deconstructing the result is necessary to compute the views of the original
 * rows to send to the client.
 *
 * The format for aliasing columns is either of:
 *
 * ```
 *               {source-table}/{column-name}  // e.g. "users/id"
 * {subquery-id}/{source-table}/{column-name}  // e.g. "owner/users/id"
 * ```
 *
 * The following examples of selection expansion will clarify how the first part,
 * the "subquery-id", is determined.
 *
 * ### Simple Queries
 *
 * For simple queries, a selection must be expanded to include:
 * * The primary keys of the table in order to identify the row.
 * * The columns used to filter and order the rows (i.e. `WHERE` and `ORDER BY`).
 *
 * Logically:
 * ```sql
 * SELECT name FROM users WHERE level = 'superstar';
 * ```
 *
 * becomes:
 * ```sql
 * SELECT id AS "users/id", name AS "users/name", level AS "users/level"
 *   FROM users WHERE level = 'superstar';
 * ```
 *
 * Note that the keys of the returned row prefix the column name with the source
 * table, obviating the need for the query result processor to inspect the query
 * AST itself.
 *
 * ```
 *  users/id | users/name | users/level
 *  ---------+-------------------------
 *         1 | Alice      | superstar
 *         2 | Bob        | superstar
 * ```
 *
 * Note that in case of a simple SELECT statement, no subquery id alias is needed.
 *
 * ### Simple table joins
 *
 * For simple join queries, this is expanded to:
 * * The primary keys of the joined table.
 * * The columns used in the `ON` clause to select the rows of the joined table.
 * * Any columns selected by the containing query.
 *
 * Logically:
 * ```sql
 * SELECT issues.title, owner.name
 *   FROM issues JOIN users AS owner ON issues.owner_id = owner.id;
 * ```
 *
 * becomes:
 * ```sql
 * SELECT issues.id           AS  "issues/id",
 *        issues.owner_id     AS  "issues/owner_id",
 *        issues.title        AS  "issues/title",
 *        owner."users/id"    AS  "owner/users/id",
 *        owner."users/name"  AS  "owner/users/name"
 *   FROM issues JOIN (
 *     SELECT id    AS  "users/id",
 *            name  AS  "users/name"
 *       FROM users)
 *   AS owner ON issues.owner_id = owner."users/id";
 * ```
 *
 * From this it becomes more apparent how the aliased column names are used to
 * deconstruct the result into constituent rows:
 *
 * ```
 *  issues/id | issues/owner_id | issues/title | owner/users/id | owner/users/name
 * -----------+-----------------+--------------+----------------+------------------
 *          1 |               1 | Foo issue    |              1 | Alice
 *          2 |               3 | Bar issue    |              3 | Candice
 * ```
 *
 * Also note that the columns from the JOIN'ed table have the subquery-id "owner", which
 * is the alias assigned to the JOIN statement.
 *
 * ### Joins with queries
 *
 * For joins with subqueries, expanded selections from each nested query must be bubbled
 * up to the top level selection.
 *
 * Note, however, that the alias given to a subquery might not be unique at the top level
 * scope, as they only need to be unique within the scope of their subquery. For example,
 * in the query:
 *
 * ```sql
 * SELECT issues.title, owner.name FROM issues
 *   JOIN users AS owner ON owner.id = issues.user_id
 *   JOIN (SELECT issues.title AS parent_title, owner.name AS parent_owner_name
 *         FROM issues JOIN users AS owner ON owner.id = issues.user_id) AS parent
 *   ON parent.id = issues.parent_id;
 * ```
 *
 * both the top-level JOIN on the `users` table, and the nested JOIN on the `users`
 * within the `parent` subquery use the `owner` alias. Again, this is legal because the
 * latter is scoped within the inner subquery. When bubbling up its columns to the
 * higher level SELECT, the alias of the subquery is prepended with its containing JOIN
 * to ensure prevent ambiguous names.
 *
 * ```sql
 * SELECT issues.id                  AS  "issues/id",
 *        issues.user_id             AS  "issues/user_id",
 *        issues.parent_id           AS  "issues/parent_id",
 *        issues.title               AS  "issues/title",
 *        owner."users/id"           AS  "owner/users/id",
 *        owner."users/name"         AS  "owner/users/name",
 *        parent."issues/id"         AS  "parent/issues/id",
 *        parent."issues/user_id"    AS  "parent/issues/user_id",
 *        parent."issues/title"      AS  "parent/issues/title",
 *        parent."owner/users/id"    AS  "parent/owner/users/id",
 *        parent."owner/users/name"  AS  "parent/owner/users/name"
 *   FROM issues
 *   JOIN (
 *         SELECT id    AS  "users/id",
 *                name  AS  "users/name"
 *         FROM users
 *        ) AS owner ON owner."users/id" = issues.user_id
 *   JOIN (
 *         SELECT issues.id           AS  "issues/id",
 *                issues.user_id      AS  "issues/user_id",
 *                issues.title        AS  "issues/title",
 *                owner."users/id"    AS  "owner/users/id",
 *                owner."users/name"  AS  "owner/users/name"
 *           FROM issues
 *           JOIN (
 *                 SELECT id    AS  "users/id",
 *                        name  AS  "users/name"
 *                 FROM users
 *                ) AS owner ON owner."users/id" = issues.user_id
 *        ) AS parent
 *   ON parent."issues/id" = issues.parent_id;
 * ```
 */
export function expandSelection(ast: AST, primaryKeys: PrimaryKeys): AST {
  const expanded = expandSubqueries(ast, primaryKeys, new Set());
  const reAliased = reAliasAndBubbleSelections(expanded, new Map());
  return reAliased;
}

/**
 * The first step of full query expansion is sub-query expansion. In this step,
 * all AST's are converted to explicit `SELECT` statements that select all of the
 * columns necessary to recompute the execution. In this step, column references are
 * plumbed downward into sub-queries; higher level `SELECT` and `ON` references are
 * passed down to the subqueries so that those subqueries can explicitly SELECT on
 * them. Within each sub-query `WHERE` statements are also traversed and their columns
 * are added to the selection.
 *
 * At the end of this step, all JOIN queries become sub-selects with explicit column
 * declarations. For example:
 *
 * ```sql
 * SELECT issues.title, owner.name
 *   FROM issues JOIN users AS owner ON issues.owner_id = owner.id
 *   WHERE issues.priority > 3;
 * ```
 *
 * Becomes:
 *
 * ```
 * SELECT issues.id,        -- Primary key
 *        issues.owner_id   -- Referenced by ON
 *        issues.priority,  -- Referenced by WHERE conditions
 *        issues.title,
 *        owner.name
 *   FROM issues JOIN (
 *      SELECT id,          -- Primary key, and referenced by containing ON
 *             name         -- Referenced by containing SELECT
 *        FROM users
 *   ) AS owner ON issues.owner_id = owner.id
 *   WHERE issues.priority > 3;
 * ```
 */
// Exported for testing
export function expandSubqueries(
  ast: AST,
  primaryKeys: PrimaryKeys,
  externallyReferencedColumns: Set<string>,
): AST {
  const {select, where, joins, groupBy, orderBy, table, alias} = ast;

  // Collect all references from SELECT, WHERE, and ON clauses
  const selectors = new Map<string, Set<string>>(); // Maps from alias to column aliases
  const defaultFrom = alias ?? table;
  const addSelector = (selector: string) => {
    const parts = selector.split('.'); // "issues.id" or just "id"
    const [from, col] = parts.length === 2 ? parts : [defaultFrom, selector];
    selectors.get(from)?.add(col) ?? selectors.set(from, new Set([col]));
  };
  // Add all referenced fields / selectors.
  select?.forEach(([selector]) => addSelector(selector));
  const selected = new Set(selectors.get(defaultFrom) ?? new Set()); // Remember what is SELECT'ed.

  getWhereColumns(where, new Set<string>()).forEach(addSelector);
  joins?.forEach(({on}) => on.forEach(addSelector));
  groupBy?.forEach(addSelector);
  orderBy?.[0].forEach(addSelector);

  // Add primary keys
  primaryKeys(table).forEach(addSelector);

  // Union with selections that are externally referenced (passed by a higher level query).
  const allFromReferences = union(
    externallyReferencedColumns,
    selectors.get(defaultFrom) ?? new Set(),
  );
  // Now add SELECT expressions for referenced columns that weren't originally SELECT'ed.
  const additionalSelection = [...allFromReferences]
    .filter(col => !selected.has(col))
    .map(col => [col, col] as [string, string]);
  const expandedSelect = [...(select ?? []), ...additionalSelection];

  return {
    ...ast,
    select: expandedSelect,
    joins: joins?.map(join => ({
      ...join,
      other: expandSubqueries(
        join.other,
        primaryKeys,
        // Send down references to the JOIN alias as the externallyReferencedColumns.
        selectors.get(join.as) ?? new Set(),
      ),
    })),
  };
}

function getWhereColumns(
  where: Condition | undefined,
  cols: Set<string>,
): Set<string> {
  if (where?.type === 'simple') {
    cols.add(where.field);
  } else if (where?.type === 'conjunction') {
    where.conditions.forEach(condition => getWhereColumns(condition, cols));
  }
  return cols;
}

/**
 * The second step of query expansion, after subqueries have been expanded, is the
 * renaming of the aliases to conform to the `{table}/{column}` suffix. The aliases
 * are then bubbled up from nested selects up to the top level select so that the
 * final query returns all columns from all rows that are analyzed as part of query
 * execution.
 */
// Exported for testing
export function reAliasAndBubbleSelections(
  ast: AST,
  exports: Map<string, string>,
): AST {
  const {select, joins, groupBy, orderBy} = ast;

  // Bubble up new aliases from subqueries.
  const reAliasMaps = new Map<string, Map<string, string>>(); // queryAlias -> prevAlias -> currAlias.
  const reAliasedJoins = joins?.map(join => {
    const reAliasMap = new Map<string, string>();
    reAliasMaps.set(join.as, reAliasMap);
    return {
      ...join,
      other: reAliasAndBubbleSelections(join.other, reAliasMap),
    };
  });
  const bubbleUp = [...reAliasMaps.entries()].flatMap(
    ([joinAlias, reAliasMap]) =>
      [...reAliasMap.values()].map(colAlias => `${joinAlias}.${colAlias}`),
  );

  // reAlias the columns selected from this AST's FROM table/alias.
  const defaultFrom = ast.alias ?? ast.table;
  const reAliasMap = new Map<string, string>();
  reAliasMaps.set(defaultFrom, reAliasMap);
  select?.forEach(([selector, alias]) => {
    const parts = selector.split('.'); // "issues.id" or just "id"
    reAliasMap.set(alias, parts.length === 2 ? parts[1] : selector); // Use the original column name.
    reAliasMap.set(parts.length === 2 ? parts[0] : selector, selector);
  });

  const renameSelector = (selector: string) => {
    const parts = selector.split('.'); // "issues.id" or just "id"
    const [from, col] = parts.length === 2 ? parts : [defaultFrom, selector];
    const newCol = reAliasMaps.get(from)?.get(col);
    assert(newCol, `New column not found for ${from}.${col}`);
    return `${from}.${newCol}`;
  };

  // Return a modified AST with all selectors realiased (SELECT, ON, GROUP BY, ORDER BY),
  // and bubble up all selected aliases to the `exports` Map.
  const exported = new Set<string>();
  return {
    ...ast,
    select: [
      ...(select ?? []).map(([selector, alias]) => {
        const newSelector = renameSelector(selector);
        const newAlias = newSelector.replaceAll('.', '/');
        exports.set(alias, newAlias);
        exported.add(newSelector);
        return [newSelector, newAlias] as [string, string];
      }),
      ...bubbleUp
        .filter(selector => !exported.has(selector))
        .map(selector => {
          const alias = selector.replaceAll('.', '/');
          exports.set(alias, alias);
          return [selector, alias] as [string, string];
        }),
    ],
    joins: reAliasedJoins?.map(join => ({
      ...join,
      on: [renameSelector(join.on[0]), renameSelector(join.on[1])],
    })),
    groupBy: groupBy?.map(renameSelector),
    orderBy: orderBy ? [orderBy[0].map(renameSelector), orderBy[1]] : undefined,
  };
}
