import {
  escapeLike,
  syncedQuery,
  syncedQueryWithContext,
  type Query,
} from '@rocicorp/zero';
import * as z from 'zod/mini';
import type {AuthData, Role} from './auth.ts';
import {INITIAL_COMMENT_LIMIT} from './consts.ts';
import {builder, ZERO_PROJECT_NAME, type Schema} from './schema.ts';

// oxlint-disable-next-line no-explicit-any
function applyIssuePermissions<TQuery extends Query<Schema, 'issue', any>>(
  q: TQuery,
  role: Role | undefined,
): TQuery {
  return q.where(({or, cmp, cmpLit}) =>
    or(cmp('visibility', '=', 'public'), cmpLit(role ?? null, '=', 'crew')),
  ) as TQuery;
}

const idValidator = z.tuple([z.string()]);
const keyValidator = idValidator;

const listContextParams = z.object({
  open: z.nullable(z.boolean()),
  projectName: z.optional(z.string()),
  assignee: z.nullable(z.string()),
  creator: z.nullable(z.string()),
  labels: z.nullable(z.array(z.string())),
  textFilter: z.nullable(z.string()),
  sortField: z.union([z.literal('modified'), z.literal('created')]),
  sortDirection: z.union([z.literal('asc'), z.literal('desc')]),
});
export type ListContextParams = z.infer<typeof listContextParams>;

const issueRowSort = z.object({
  id: z.string(),
  created: z.number(),
  modified: z.number(),
});

type IssueRowSort = z.infer<typeof issueRowSort>;

export const queries = {
  allLabels: syncedQuery('allLabels', z.tuple([]), () => builder.label),

  allUsers: syncedQuery('allUsers', z.tuple([]), () => builder.user),

  allProjects: syncedQuery('allProjects', z.tuple([]), () => builder.project),

  user: syncedQuery('user', idValidator, userID =>
    builder.user.where('id', userID).one(),
  ),

  labelsForProject: syncedQuery(
    'labels',
    z.tuple([
      z.object({
        projectName: z.string(),
      }),
    ]),
    ({projectName}: {projectName: string}) =>
      builder.label.whereExists('project', q =>
        q.where('lowerCaseName', projectName.toLocaleLowerCase()),
      ),
  ),

  issuePreloadV2: syncedQueryWithContext(
    'issuePreloadV2',
    z.tuple([
      z.object({
        userID: z.string(),
        projectName: z.string(),
      }),
    ]),
    (auth: AuthData | undefined, {userID, projectName}) =>
      applyIssuePermissions(
        builder.issue
          .whereExists('project', p => p.where('name', projectName), {
            flip: true,
          })
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
          )
          .orderBy('modified', 'desc')
          .orderBy('id', 'desc')
          .limit(1000),
        auth?.role,
      ),
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

  userPickerV2: syncedQuery(
    'usersForProject',
    z.tuple([
      z.object({
        disabled: z.boolean(),
        login: z.optional(z.string()),
        projectName: z.string(),
        filter: z.optional(z.enum(['crew', 'creators', 'assignees'])),
      }),
    ]),
    ({disabled, login, projectName, filter}) => {
      let q = builder.user;
      if (disabled && login) {
        q = q.where('login', login);
      } else if (filter) {
        if (filter === 'crew') {
          q = q.where(({cmp, not, and}) =>
            and(cmp('role', 'crew'), not(cmp('login', 'LIKE', 'rocibot%'))),
          );
        } else if (filter === 'creators') {
          q = q.whereExists('createdIssues', i =>
            i.whereExists(
              'project',
              p => p.where('lowerCaseName', projectName.toLocaleLowerCase()),
              {flip: true},
            ),
          );
        } else if (filter === 'assignees') {
          q = q.whereExists('assignedIssues', i =>
            i.whereExists(
              'project',
              p => p.where('lowerCaseName', projectName.toLocaleLowerCase()),
              {flip: true},
            ),
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
    z.tuple([
      z.union([z.literal('shortID'), z.literal('id')]),
      z.union([z.string(), z.number()]),
      z.string(),
    ]),
    (auth: AuthData | undefined, idField, id, userID) =>
      applyIssuePermissions(
        builder.issue
          .where(idField, id)
          .related('project')
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

  issueListV2: syncedQueryWithContext(
    'issueListV2',
    z.tuple([
      listContextParams,
      z.string(),
      z.nullable(z.number()),
      z.nullable(issueRowSort),
      z.union([z.literal('forward'), z.literal('backward')]),
    ]),
    (auth: AuthData | undefined, listContext, userID, limit, start, dir) =>
      issueListV2(listContext, limit, userID, auth, start, dir),
  ),

  emojiChange: syncedQuery('emojiChange', idValidator, subjectID =>
    builder.emoji
      .where('subjectID', subjectID ?? '')
      .related('creator', creator => creator.one()),
  ),

  // The below queries are DEPRECATED
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
          )
          .orderBy('modified', 'desc')
          .orderBy('id', 'desc')
          .limit(1000),
        auth?.role,
      ),
  ),

  prevNext: syncedQueryWithContext(
    'prevNext',
    z.tuple([
      z.nullable(listContextParams),
      z.nullable(issueRowSort),
      z.union([z.literal('next'), z.literal('prev')]),
    ]),
    (auth: AuthData | undefined, listContext, issue, dir) =>
      buildListQuery({
        listContext: listContext ?? undefined,
        start: issue ?? undefined,
        dir: dir === 'next' ? 'forward' : 'backward',
        role: auth?.role,
      }).one(),
  ),

  issueList: syncedQueryWithContext(
    'issueList',
    z.tuple([listContextParams, z.string(), z.number()]),
    (auth: AuthData | undefined, listContext, userID, limit) =>
      issueListV2(listContext, limit, userID, auth, null, 'forward'),
  ),

  userPicker: syncedQuery(
    'userPicker',
    z.tuple([
      z.boolean(),
      z.nullable(z.string()),
      z.nullable(z.enum(['crew', 'creators'])),
    ]),
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
} as const;

export type ListContext = {
  readonly href: string;
  readonly title: string;
  readonly params: ListContextParams;
};

function issueListV2(
  listContext: ListContextParams,
  limit: number | null,
  userID: string,
  auth: AuthData | undefined,
  start: IssueRowSort | null,
  dir: 'forward' | 'backward',
) {
  return buildListQuery({
    listContext,
    limit: limit ?? undefined,
    userID,
    role: auth?.role,
    start: start ?? undefined,
    dir,
  });
}

export type ListQueryArgs = {
  issueQuery?: (typeof builder)['issue'] | undefined;
  listContext?: ListContext['params'] | undefined;
  project?: string | undefined;
  userID?: string;
  role?: Role | undefined;
  limit?: number | undefined;
  start?: IssueRowSort | undefined;
  dir?: 'forward' | 'backward' | undefined;
};

export function buildListQuery(args: ListQueryArgs) {
  const {
    issueQuery = builder.issue,
    limit,
    listContext,
    role,
    dir = 'forward',
    start,
  } = args;

  let q = issueQuery
    .related('viewState', q =>
      args.userID
        ? q.where('userID', args.userID).one()
        : q.where(({or}) => or()),
    )
    .related('labels');

  if (!listContext) {
    return q.where(({or}) => or());
  }
  const {projectName = ZERO_PROJECT_NAME} = listContext;

  q = q.whereExists(
    'project',
    q => q.where('lowerCaseName', projectName.toLocaleLowerCase()),
    {
      flip: true,
    },
  );

  const {sortField, sortDirection} = listContext;
  const orderByDir =
    dir === 'forward'
      ? sortDirection
      : sortDirection === 'asc'
        ? 'desc'
        : 'asc';
  q = q.orderBy(sortField, orderByDir).orderBy('id', orderByDir);

  if (start) {
    q = q.start(start);
  }
  if (limit) {
    q = q.limit(limit);
  }

  const {open, creator, assignee, labels, textFilter} = listContext;
  q = q.where(({and, cmp, exists, or}) =>
    and(
      // oxlint-disable-next-line eqeqeq
      open != null ? cmp('open', open) : undefined,
      creator
        ? exists('creator', q => q.where('login', creator), {flip: true})
        : undefined,
      assignee
        ? exists('assignee', q => q.where('login', assignee), {flip: true})
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
        exists('labels', q => q.where('name', label), {flip: true}),
      ),
    ),
  );

  return applyIssuePermissions(q, role);
}
