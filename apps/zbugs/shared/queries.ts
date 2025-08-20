import {
  escapeLike,
  type Query,
  type Row,
  syncedQuery,
  syncedQueryWithContext,
} from '@rocicorp/zero';
import {builder, type Schema} from './schema.ts';
import {INITIAL_COMMENT_LIMIT} from './consts.ts';
import type {AuthData, Role} from './auth.ts';
import z from 'zod';

function applyIssuePermissions<TQuery extends Query<Schema, 'issue', any>>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  q: TQuery,
  role: Role | undefined,
): TQuery {
  return q.where(({or, cmp, cmpLit}) =>
    or(cmp('visibility', '=', 'public'), cmpLit(role ?? null, '=', 'crew')),
  ) as TQuery;
}

const idValidator = z.tuple([z.string()]).parse;
const keyValidator = idValidator;

const listContextParams = z.object({
  open: z.boolean().nullable(),
  assignee: z.string().nullable(),
  creator: z.string().nullable(),
  labels: z.array(z.string()).nullable(),
  textFilter: z.string().nullable(),
  sortField: z.union([z.literal('modified'), z.literal('created')]),
  sortDirection: z.union([z.literal('asc'), z.literal('desc')]),
});
type ListContextParams = z.infer<typeof listContextParams>;

const issueRowSort = z.object({
  id: z.string(),
  created: z.number(),
  modified: z.number(),
});

export const queries = {
  allLabels: syncedQuery('allLabels', z.tuple([]).parse, () => builder.label),

  allIssues: syncedQuery('allIssues', z.tuple([]).parse, () => builder.issue),

  allUsers: syncedQuery('allUsers', z.tuple([]).parse, () => builder.user),

  issuePreload: syncedQueryWithContext(
    'issuePreload',
    idValidator,
    (auth: AuthData | undefined, userID) =>
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
  ),

  user: syncedQuery('user', idValidator, userID =>
    builder.user.where('id', userID).one(),
  ),

  userPref: syncedQueryWithContext(
    'userPref',
    keyValidator,
    (auth: AuthData | undefined, key) =>
      builder.userPref
        .where('key', key)
        .where('userID', auth?.sub ?? '')
        .one(),
  ),

  userPicker: syncedQuery(
    'userPicker',
    z.tuple([
      z.boolean(),
      z.string().nullable(),
      z.enum(['crew', 'creators']).nullable(),
    ]).parse,
    (disabled, login, filter) => {
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
  ),

  issueDetail: syncedQueryWithContext(
    'issueDetail',
    z.tuple([
      z.union([z.literal('shortID'), z.literal('id')]),
      z.string().or(z.number()),
      z.string(),
    ]).parse,
    (auth: AuthData | undefined, idField, id, userID) =>
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
    z.tuple([
      listContextParams.nullable(),
      issueRowSort.nullable(),
      z.union([z.literal('next'), z.literal('prev')]),
    ]).parse,
    (auth: AuthData | undefined, listContext, issue, dir) =>
      applyIssuePermissions(
        buildListQuery(listContext, issue, dir).one(),
        auth?.role,
      ),
  ),

  issueList: syncedQueryWithContext(
    'issueList',
    z.tuple([listContextParams, z.string(), z.number()]).parse,
    (auth: AuthData | undefined, listContext, userID, limit) =>
      applyIssuePermissions(
        buildListQuery(listContext, null, 'next')
          .limit(limit)
          .related('viewState', q => q.where('userID', userID).one())
          .related('labels'),
        auth?.role,
      ),
  ),

  emojiChange: syncedQuery('emojiChange', idValidator, subjectID =>
    builder.emoji
      .where('subjectID', subjectID ?? '')
      .related('creator', creator => creator.one()),
  ),
};

export type ListContext = {
  readonly href: string;
  readonly title: string;
  readonly params: ListContextParams;
};

function buildListQuery(
  listContext: ListContext['params'] | null,
  start: Pick<
    Row<Schema['tables']['issue']>,
    'id' | 'created' | 'modified'
  > | null,
  dir: 'next' | 'prev',
) {
  if (!listContext) {
    return builder.issue.where(({or}) => or());
  }

  const {
    open,
    creator,
    assignee,
    labels,
    textFilter,
    sortField,
    sortDirection,
  } = listContext;

  const orderByDir =
    dir === 'next' ? sortDirection : sortDirection === 'asc' ? 'desc' : 'asc';

  let q = builder.issue;
  if (start) {
    q = q.start(start);
  }

  return q.orderBy(sortField, orderByDir).where(({and, cmp, exists, or}) =>
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
  );
}
