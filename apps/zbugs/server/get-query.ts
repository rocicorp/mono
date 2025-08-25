import {
  withValidation,
  type Query,
  type ReadonlyJSONValue,
  type Schema,
} from '@rocicorp/zero';
import {
  buildBaseListQuery,
  buildBaseListQueryFilter,
  buildListQuery,
  queries,
  type ListQueryArgs,
} from '../shared/queries.ts';
import type {AuthData} from '../shared/auth.ts';
import {builder} from '../shared/schema.ts';
import {assert} from '../../../packages/shared/src/asserts.ts';

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
    if (name === 'issueList') {
      assert(queries.issueList.parse);
      const [listContext, userID, limit] = queries.issueList.parse([...args]);
      return serverOptimizedListQuery(
        {
          listContext,
          limit,
          userID,
          role: context?.role,
        },
        buildListQuery,
      );
    }
    if (name === 'prevNext') {
      assert(queries.prevNext.parse);
      const [listContext, issue, dir] = queries.prevNext.parse([...args]);
      return serverOptimizedListQuery(
        {
          listContext: listContext ?? undefined,
          limit: 1,
          start: issue ?? undefined,
          dir,
          role: context?.role,
        },
        buildBaseListQuery,
      );
    }

    return validated[name](context, ...args);
  } else {
    throw new Error(`Unknown query: ${name}`);
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
  const {labels} = listContext;
  if (labels !== null && labels !== undefined && labels.length > 0) {
    const [_, ...restLabels] = labels ?? [];
    const restListContext = {...listContext, labels: restLabels};
    return builder.label
      .where('name', labels[0])
      .related('issueLabels', q => {
        q = q
          .whereExists('issues', q =>
            buildBaseListQueryFilter(q, restListContext, args.role),
          )
          .related('issues', q =>
            makeListQuery({
              ...args,
              issueQuery: q,
              listContext: restListContext,
              limit: 1,
            }),
          );

        const {sortDirection} = listContext;
        const orderByDir =
          args.dir === 'next'
            ? sortDirection
            : sortDirection === 'asc'
              ? 'desc'
              : 'asc';
        q = q
          .orderBy(listContext.sortField, orderByDir)
          .orderBy('issueID', orderByDir);
        if (args.start) {
          q = q.start(args.start);
        }
        if (args.limit) {
          q = q.limit(args.limit);
        }
        return q;
      })
      .one();
  }
  return makeListQuery(args);
}
