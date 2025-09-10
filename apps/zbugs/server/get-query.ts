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
