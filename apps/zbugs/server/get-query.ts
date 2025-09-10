import {
  syncedQueryWithContext,
  withValidation,
  type Query,
  type ReadonlyJSONValue,
  type Schema,
} from '@rocicorp/zero';
import {
  buildBaseListQuery,
  buildListQuery,
  queries as sharedQueries,
  type ListQueryArgs,
} from '../shared/queries.ts';
import type {AuthData} from '../shared/auth.ts';
import {builder} from '../shared/schema.ts';

const queries = {
  ...sharedQueries,
  // Replace prevNext, issueList, and issueListV2 with server optimized versions.
  prevNext: syncedQueryWithContext(
    sharedQueries.prevNext.queryName,
    sharedQueries.prevNext.parse,
    (auth: AuthData | undefined, listContext, issue, dir) =>
      serverOptimizedListQuery(
        {
          listContext: listContext ?? undefined,
          limit: 1,
          start: issue ?? undefined,
          dir: dir === 'next' ? 'forward' : 'backward',
          role: auth?.role,
        },
        buildBaseListQuery,
      ),
  ),
  issueList: syncedQueryWithContext(
    sharedQueries.issueList.queryName,
    sharedQueries.issueList.parse,
    (auth: AuthData | undefined, listContext, userID, limit) =>
      serverOptimizedListQuery(
        {
          listContext,
          limit,
          userID,
          role: auth?.role,
        },
        buildListQuery,
      ),
  ),
  issueListV2: syncedQueryWithContext(
    sharedQueries.issueListV2.queryName,
    sharedQueries.issueListV2.parse,
    (auth: AuthData | undefined, listContext, userID, limit, start, dir) =>
      serverOptimizedListQuery(
        {
          listContext,
          limit: limit ?? undefined,
          userID,
          role: auth?.role,
          start: start ?? undefined,
          dir,
        },
        buildListQuery,
      ),
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

function serverOptimizedListQuery(
  args: ListQueryArgs,
  makeListQuery: (args: ListQueryArgs) => Query<Schema, 'issue'>,
) {
  const {listContext} = args;
  if (!listContext) {
    return builder.issue.where(({or}) => or());
  }
  const {assignee} = listContext;
  if (assignee !== null && assignee !== undefined) {
    return builder.user
      .where('login', assignee)
      .related('assignedIssues', q =>
        makeListQuery({
          ...args,
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
        makeListQuery({
          ...args,
          issueQuery: q,
          listContext: {...listContext, creator: null},
        }),
      )
      .one();
  }
  return makeListQuery(args);
}
