import {
  escapeLike,
  type Query,
  type Row,
  queriesWithContext,
} from '@rocicorp/zero';
import {builder, type Schema} from './schema.ts';
import {INITIAL_COMMENT_LIMIT} from './consts.ts';
import type {AuthData, Role} from './auth.ts';

function applyIssuePermissions<TQuery extends Query<Schema, 'issue', any>>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  q: TQuery,
  role: Role | undefined,
): TQuery {
  return q.where(({or, cmp, cmpLit}) =>
    or(cmp('visibility', '=', 'public'), cmpLit(role ?? null, '=', 'crew')),
  ) as TQuery;
}

export const queries = queriesWithContext({
  allLabels: (_auth: AuthData | undefined) => builder.label,

  allUsers: (_auth: AuthData | undefined) => builder.user,

  issuePreload: (auth: AuthData | undefined, userID: string) =>
    applyIssuePermissions(
      builder.issue
        .related('labels')
        .related('viewState', q => q.where('userID', userID))
        .related('creator')
        .related('assignee')
        .related('emoji', emoji => emoji.related('creator'))
        .related('comments', comments =>
          comments
            .related('creator')
            .related('emoji', emoji => emoji.related('creator'))
            .limit(10)
            .orderBy('created', 'desc'),
        ),
      auth?.role,
    ),

  user: (_auth: AuthData | undefined, userID: string) =>
    builder.user.where('id', userID).one(),

  userPref: (auth: AuthData | undefined, key: string) =>
    builder.userPref
      .where('key', key)
      .where('userID', auth?.sub ?? '')
      .one(),

  userPicker: (
    _auth: AuthData | undefined,
    disabled: boolean,
    login: string | null,
    filter: 'crew' | 'creators' | null,
  ) => {
    let q = builder.user;
    if (disabled && login) {
      q = q.where('login', login);
    } else if (filter) {
      if (filter === 'crew') {
        q = q.where(({cmp, not, and}) =>
          and(cmp('role', 'crew'), not(cmp('login', 'LIKE', 'rocibot%'))),
        );
      } else if (filter === 'creators') {
        q = q.whereExists('createdIssues');
      } else {
        throw new Error(`Unknown filter: ${filter}`);
      }
    }
    return q;
  },

  issueDetail: (
    auth: AuthData | undefined,
    idField: 'shortID' | 'id',
    id: string | number,
    userID: string,
  ) =>
    applyIssuePermissions(
      builder.issue
        .where(idField, id)
        .related('emoji', emoji => emoji.related('creator'))
        .related('creator')
        .related('assignee')
        .related('labels')
        .related('notificationState', q => q.where('userID', userID))
        .related('viewState', viewState =>
          viewState.where('userID', userID).one(),
        )
        .related('comments', comments =>
          comments
            .related('creator')
            .related('emoji', emoji => emoji.related('creator'))
            // One more than we display so we can detect if there are more to load.
            .limit(INITIAL_COMMENT_LIMIT + 1)
            .orderBy('created', 'desc')
            .orderBy('id', 'desc'),
        )
        .one(),
      auth?.role,
    ),

  prevNext: (
    auth: AuthData | undefined,
    listContext: ListContext['params'] | null,
    issue: Pick<
      Row<Schema['tables']['issue']>,
      'id' | 'created' | 'modified'
    > | null,
    dir: 'next' | 'prev',
  ) =>
    buildBaseListQuery(
      builder.issue,
      listContext,
      issue,
      dir,
      auth?.role,
    ).one(),

  issueList: (
    auth: AuthData | undefined,
    listContext: ListContext['params'],
    userID: string,
    limit: number,
  ) => buildListQuery(builder.issue, listContext, limit, userID, auth),

  issueListForAssignee: (
    auth: AuthData | undefined,
    listContext: ListContext['params'],
    userID: string,
    limit: number,
  ) => {
    let q = builder.user;
    const {assignee, ...listContextSansAssignee} = listContext;
    if (assignee === null || assignee === undefined) {
      q = q.where(({or}) => or());
    } else {
      q = q.where('login', assignee);
    }
    return q.related('assignedIssues', q =>
      buildListQuery(q, listContextSansAssignee, limit, userID, auth),
    );
  },

  issueListForCreator: (
    auth: AuthData | undefined,
    listContext: ListContext['params'],
    userID: string,
    limit: number,
  ) => {
    let q = builder.user;
    const {creator, ...listContextSansCreator} = listContext;
    if (creator === null || creator === undefined) {
      q = q.where(({or}) => or());
    } else {
      q = q.where('login', creator);
    }
    return q.related('createdIssues', q =>
      buildListQuery(q, listContextSansCreator, limit, userID, auth),
    );
  },

  issueListForLabel: (
    auth: AuthData | undefined,
    listContext: ListContext['params'],
    userID: string,
    limit: number,
  ) => {
    let labelQ = builder.label;
    const {labels, ...listContextSansLabels} = listContext;
    if (labels === null || labels == undefined || labels.length === 0) {
      labelQ = labelQ.where(({or}) => or());
    } else {
      labelQ = labelQ.where('name', labels[0]);
    }
    const [_, ...restLabels] = labels ?? [];
    const restListContext = {...listContextSansLabels, labels: restLabels};

    return labelQ.related('issueLabels', q =>
      q
        .orderBy(listContext.sortField, listContext.sortDirection)
        .limit(limit)
        .whereExists('issues', q =>
          buildBaseListQueryFilter(q, restListContext, auth?.role),
        )
        .related('issues', q =>
          buildListQuery(q, restListContext, 1, userID, auth),
        ),
    );
  },

  emojiChange: (_auth: AuthData | undefined, subjectID: string) =>
    builder.emoji
      .where('subjectID', subjectID ?? '')
      .related('creator', creator => creator.one()),
});

export type ListContext = {
  readonly href: string;
  readonly title: string;
  readonly params: {
    readonly open?: boolean | null;
    readonly assignee?: string | null;
    readonly creator?: string | null;
    readonly labels?: string[] | null;
    readonly textFilter?: string | null;
    readonly sortField: 'modified' | 'created';
    readonly sortDirection: 'asc' | 'desc';
  };
};

function buildListQuery(
  q: (typeof builder)['issue'],
  listContextSansCreator: ListContext['params'] | null,
  limit: number,
  userID: string,
  auth: AuthData | undefined,
) {
  return buildBaseListQuery(q, listContextSansCreator, null, 'next', auth?.role)
    .limit(limit)
    .related('viewState', q => q.where('userID', userID).one())
    .related('labels');
}

function buildBaseListQuery(
  issueQuery: (typeof builder)['issue'],
  listContext: ListContext['params'] | null,
  start: Pick<
    Row<Schema['tables']['issue']>,
    'id' | 'created' | 'modified'
  > | null,
  dir: 'next' | 'prev',
  role: Role | undefined,
) {
  if (!listContext) {
    return issueQuery.where(({or}) => or());
  }

  const {sortField, sortDirection} = listContext;

  const orderByDir =
    dir === 'next' ? sortDirection : sortDirection === 'asc' ? 'desc' : 'asc';

  let q = issueQuery;
  if (start) {
    q = q.start(start);
  }

  return buildBaseListQueryFilter(
    q.orderBy(sortField, orderByDir),
    listContext,
    role,
  );
}

function buildBaseListQueryFilter(
  issueQuery: (typeof builder)['issue'],
  listContext: ListContext['params'],
  role: Role | undefined,
) {
  const {open, creator, assignee, labels, textFilter} = listContext;
  return applyIssuePermissions(
    issueQuery.where(({and, cmp, exists, or}) =>
      and(
        open != null ? cmp('open', open) : undefined,
        creator ? exists('creator', q => q.where('login', creator)) : undefined,
        assignee
          ? exists('assignee', q => q.where('login', assignee))
          : undefined,
        textFilter
          ? or(
              cmp('title', 'ILIKE', `%${escapeLike(textFilter)}%`),
              cmp('description', 'ILIKE', `%${escapeLike(textFilter)}%`),
              exists('comments', q =>
                q.where('body', 'ILIKE', `%${escapeLike(textFilter)}%`),
              ),
            )
          : undefined,
        ...(labels ?? []).map(label =>
          exists('labels', q => q.where('name', label)),
        ),
      ),
    ),
    role,
  );
}
