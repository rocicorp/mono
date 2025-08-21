import {
  escapeLike,
  type Query,
  type Row,
  syncedQueryWithContext,
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

export const queries = {
  allLabels: syncedQueryWithContext(
    'allLabels',
    (_auth: AuthData | undefined) => builder.label,
  ),

  allUsers: syncedQueryWithContext(
    'allUsers',
    (_auth: AuthData | undefined) => builder.user,
  ),

  issuePreload: syncedQueryWithContext(
    'issuePreload',
    (auth: AuthData | undefined, userID: string) =>
      applyIssuePermissions(
        builder.issue
          .related('issueLabels')
          .related('viewState', q => q.where('userID', userID))
          .related('emoji')
          .related('comments', comments =>
            comments
              .related('emoji')
              .limit(10)
              .orderBy('created', 'desc')
              .orderBy('id', 'desc'),
          )
          .orderBy('modified', 'desc')
          .orderBy('id', 'desc')
          .limit(1000),
        auth?.role,
      ),
  ),

  user: syncedQueryWithContext(
    'user',
    (_auth: AuthData | undefined, userID: string) =>
      builder.user.where('id', userID).one(),
  ),

  userPref: syncedQueryWithContext(
    'userPref',
    (auth: AuthData | undefined, key: string) =>
      builder.userPref
        .where('key', key)
        .where('userID', auth?.sub ?? '')
        .one(),
  ),

  userPicker: syncedQueryWithContext(
    'userPicker',
    (
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
          q = q.whereExists('createdIssues', q =>
            q.orderBy('creatorID', 'desc').orderBy('modified', 'desc'),
          );
        } else {
          throw new Error(`Unknown filter: ${filter}`);
        }
      }
      return q;
    },
  ),

  issueDetail: syncedQueryWithContext(
    'issueDetail',
    (
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
  ),
  prevNext: syncedQueryWithContext(
    'prevNext',
    (
      auth: AuthData | undefined,
      listContext: ListContext['params'] | null,
      issue: Pick<
        Row<Schema['tables']['issue']>,
        'id' | 'created' | 'modified'
      > | null,
      dir: 'next' | 'prev',
    ) =>
      buildBaseListQuery({
        listContext: listContext ?? undefined,
        start: issue ?? undefined,
        dir,
        role: auth?.role,
      }).one(),
  ),

  issueList: syncedQueryWithContext(
    'issueList',
    (
      auth: AuthData | undefined,
      listContext: ListContext['params'],
      userID: string,
      limit: number,
    ) => buildListQuery({listContext, limit, userID, role: auth?.role}),
  ),

  issueById: syncedQueryWithContext(
    'issueById',
    (auth: AuthData | undefined, id: string) =>
      applyIssuePermissions(builder.issue.where('id', id), auth?.role).one(),
  ),

  emojiChange: syncedQueryWithContext(
    'emojiChange',
    (_auth: AuthData | undefined, subjectID: string) =>
      builder.emoji
        .where('subjectID', subjectID ?? '')
        .related('creator', creator => creator.one()),
  ),
};

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

export type ListQueryArgs = {
  issueQuery?: (typeof builder)['issue'] | undefined;
  listContext?: ListContext['params'] | undefined;
  userID?: string;
  role?: Role | undefined;
  limit?: number | undefined;
  start?:
    | Pick<Row<Schema['tables']['issue']>, 'id' | 'created' | 'modified'>
    | undefined;
  dir?: 'next' | 'prev' | undefined;
};

export function buildListQuery(args: ListQueryArgs) {
  return buildBaseListQuery(args)
    .related('viewState', q =>
      args.userID
        ? q.where('userID', args.userID).one()
        : q.where(({or}) => or()),
    )
    .related('labels');
}

export function buildBaseListQuery(args: ListQueryArgs) {
  const {
    issueQuery = builder.issue,
    limit,
    listContext,
    role,
    dir = 'next',
    start,
  } = args;
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
  if (limit) {
    q = q.limit(limit);
  }

  return buildBaseListQueryFilter(
    q.orderBy(sortField, orderByDir).orderBy('id', orderByDir),
    listContext,
    role,
  );
}

export function buildBaseListQueryFilter(
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
