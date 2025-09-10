import {
  syncedQueryWithContext,
  withValidation,
  type ReadonlyJSONValue,
} from '@rocicorp/zero';
import {buildListQuery, queries as sharedQueries} from '../shared/queries.ts';
import type {AuthData} from '../shared/auth.ts';
import {builder} from '../shared/schema.ts';

const queries = {
  ...sharedQueries,
  /**
   * Replace issueListV2 with a server optimized version.
   *
   * This optimization heuristically lifts the most selective
   * exists filter to be the root of the query.
   *
   * For example if there is an assignee filter, instead of the following form
   * used on the client:
   *
   * ```
   * issue
   *   .where(({exists}) => exists('assignee', q => q.where('login', assignee)))
   *   .where(otherFilters)
   *   .start(s)
   *   .orderBy(o)
   *   .limit(l)
   * ```
   *
   * this more efficient alternative form is used on the server:
   *
   * ```
   * user
   *   .where('login', assignee)
   *   .related('assignedIssues', q => q
   *     .where(otherFilters)
   *     .start(s)
   *     .orderBy(o)
   *     .limit(l));
   * ```
   *
   * This alternative form will return the same rows (assuming login is unique,
   * or a superset of the rows if login is not unique), and thus will result in
   * the rows needed by the form used on the client being synced to the client.
   *
   * This alternative form is more efficient, because the original form
   * iterates over the issues in the specified order, checking each for
   * matching assignees until the specified limit number of matches are found.
   * If the assignee filter is highly selective, many non-matching issues will
   * be checked.
   *
   * The form used on the client is structured as it is because it returns the
   * data shape useful for rendering in the UI.  It will do the less efficient
   * iteration of issues on the client, but this is ok because the number of
   * rows synced to the client is small enough that the less efficient approach
   * is still very fast.
   *
   * This code assumes a selectivity order, from most to least selective, of:
   * - assignee,
   * - creator
   */
  issueListV2: syncedQueryWithContext(
    sharedQueries.issueListV2.queryName,
    sharedQueries.issueListV2.parse,
    (auth: AuthData | undefined, listContext, userID, limit, start, dir) => {
      if (!listContext) {
        return builder.issue.where(({or}) => or());
      }
      const buildListQueryArgs = {
        listContext,
        userID,
        role: auth?.role,
        limit: limit ?? undefined,
        start: start ?? undefined,
        dir,
      } as const;
      const {assignee} = listContext;
      if (assignee !== null && assignee !== undefined) {
        return builder.user
          .where('login', assignee)
          .related('assignedIssues', q =>
            buildListQuery({
              ...buildListQueryArgs,
              issueQuery: q,
              listContext: {...listContext, assignee: null},
            }),
          )
          .one();
      }

      const {creator} = listContext;
      if (creator !== null && creator !== undefined) {
        return builder.user
          .where('login', creator)
          .related('createdIssues', q =>
            buildListQuery({
              ...buildListQueryArgs,
              issueQuery: q,
              listContext: {...listContext, creator: null},
            }),
          )
          .one();
      }
      return buildListQuery(buildListQueryArgs);
    },
  ),
};

// It's important to map incoming queries by queryName, not the
// field name in queries. The latter is just a local identifier.
// queryName is more like an API name that should be stable between
// clients and servers.
const validated = Object.fromEntries(
  Object.values(queries).map(q => [q.queryName, withValidation(q)]),
);

export function getQuery(
  context: AuthData | undefined,
  name: string,
  args: readonly ReadonlyJSONValue[],
) {
  if (name in validated) {
    return validated[name](context, ...args);
  }
  throw new Error(`Unknown query: ${name}`);
}
