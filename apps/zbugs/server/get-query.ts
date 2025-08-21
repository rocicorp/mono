import type {
  AnyQuery,
  Query,
  ReadonlyJSONValue,
  Row,
  Schema,
} from '@rocicorp/zero';
import {
  buildBaseListQuery,
  buildBaseListQueryFilter,
  buildListQuery,
  queries,
  type ListContext,
  type ListQueryArgs,
} from '../shared/queries.ts';
import type {AuthData} from '../shared/auth.ts';
import {builder} from '../shared/schema.ts';

export function getQuery(
  context: AuthData | undefined,
  name: string,
  args: readonly ReadonlyJSONValue[],
) {
  let query;
  if (isSharedQuery(name)) {
    if (name === 'issueList') {
      const [listContext, userID, limit] = args as [
        ListContext['params'],
        string,
        number,
      ];
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
      const [listContext, issue, dir] = args as [
        ListContext['params'] | null,
        Pick<
          Row<Schema['tables']['issue']>,
          'id' | 'created' | 'modified'
        > | null,
        'next' | 'prev',
      ];
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
    query = (
      queries[name] as (
        context: AuthData | undefined,
        ...args: readonly ReadonlyJSONValue[]
      ) => AnyQuery
    )(context, ...args);
  } else {
    throw new Error(`Unknown query: ${name}`);
  }

  return query;
}

function isSharedQuery(key: string): key is keyof typeof queries {
  return key in queries;
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
