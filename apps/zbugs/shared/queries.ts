import {
  defineQueries,
  defineQuery,
  escapeLike,
  type DefaultSchema,
  type Query,
  type Schema,
} from '@rocicorp/zero';
import * as z from 'zod/mini';
import type {AuthData, Role} from './auth.ts';
import {INITIAL_COMMENT_LIMIT} from './consts.ts';
import {QueryError, QueryErrorCode} from './error.ts';
import {builder, ZERO_PROJECT_NAME} from './schema.ts';

function applyIssuePermissions<TQuery extends IssueQuery>(
  q: TQuery,
  role: Role | undefined,
): TQuery {
  return q.where(({or, cmp, cmpLit}) =>
    or(cmp('visibility', '=', 'public'), cmpLit(role, '=', 'crew')),
  ) as TQuery;
}

const idValidator = z.string();
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

export const issueRowSortSchema = z.readonly(
  z.object({
    id: z.string(),
    created: z.number(),
    modified: z.number(),
  }),
);

export type IssueRowSort = z.infer<typeof issueRowSortSchema>;

function labelsOrderByName({
  args: {projectName, orderBy},
}: {
  args: {
    projectName: string;
    orderBy?: 'asc' | 'desc' | undefined;
  };
}) {
  let q = builder.label.whereExists('project', q =>
    q.where('lowerCaseName', projectName.toLocaleLowerCase()),
  );
  if (orderBy !== undefined) {
    q = q.orderBy('name', orderBy);
  }
  return q;
}

export const queries = defineQueries({
  allLabels: defineQuery(z.undefined(), () => builder.label),

  allUsers: defineQuery(z.undefined(), () => builder.user),

  allProjects: defineQuery(z.undefined(), () => builder.project),

  user: defineQuery(idValidator, ({args: userID}) =>
    builder.user.where('id', userID).one(),
  ),

  crewUser: defineQuery(idValidator, ({args: userID}) =>
    builder.user.where('id', userID).where('role', 'crew').one(),
  ),

  labels: defineQuery(
    z.object({
      projectName: z.string(),
    }),
    labelsOrderByName,
  ),

  labelsOrderByName: defineQuery(
    z.object({
      projectName: z.string(),
      orderBy: z.enum(['asc', 'desc']),
    }),
    labelsOrderByName,
  ),

  issuePreloadV2: defineQuery(
    z.object({
      userID: z.string(),
      projectName: z.string(),
    }),

    ({ctx: auth, args: {userID, projectName}}) =>
      applyIssuePermissions(
        builder.issue
          .whereExists('project', p =>
            p.where('lowerCaseName', projectName.toLocaleLowerCase()),
          )
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

  userPref: defineQuery(keyValidator, ({ctx: auth, args: key}) =>
    builder.userPref.where('key', key).where('userID', auth?.sub).one(),
  ),
  usersForProject: defineQuery(
    z.object({
      disabled: z.boolean(),
      login: z.optional(z.string()),
      projectName: z.string(),
      filter: z.optional(z.enum(['crew', 'creators', 'assignees'])),
    }),

    ({args: {disabled, login, projectName, filter}}) => {
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
            i.whereExists('project', p =>
              p.where('lowerCaseName', projectName.toLocaleLowerCase()),
            ),
          );
        } else if (filter === 'assignees') {
          q = q.whereExists('assignedIssues', i =>
            i.whereExists('project', p =>
              p.where('lowerCaseName', projectName.toLocaleLowerCase()),
            ),
          );
        } else {
          throw new QueryError(
            `Unknown filter: ${filter}`,
            QueryErrorCode.UNKNOWN_FILTER,
            filter,
          );
        }
      }
      return q;
    },
  ),

  issueDetail: defineQuery(
    z.object({
      idField: z.union([z.literal('shortID'), z.literal('id')]),
      id: z.union([z.string(), z.number()]),
      userID: z.string(),
    }),

    ({args: {idField, id, userID}, ctx: auth}) =>
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

  issueListV2: defineQuery(
    z.object({
      listContext: listContextParams,
      userID: z.string(),
      limit: z.nullable(z.number()),
      start: z.nullable(issueRowSortSchema),
      dir: z.union([z.literal('forward'), z.literal('backward')]),
      inclusive: z.optional(z.boolean()),
    }),

    ({ctx: auth, args: {listContext, userID, limit, start, dir, inclusive}}) =>
      issueListV2(listContext, limit, userID, auth, start, dir, inclusive),
  ),

  /**
   * This is used to get a single issue so that we can get the order for start at.
   */
  issueByID: defineQuery(
    z.object({
      listContext: listContextParams,
      idField: z.union([z.literal('shortID'), z.literal('id')]),
      id: z.union([z.string(), z.number()]),
    }),
    ({ctx: auth, args: {listContext, idField, id}}) =>
      buildListQuery({role: auth?.role, listContext}).where(idField, id).one(),
  ),

  emojiChange: defineQuery(idValidator, ({args: subjectID}) =>
    builder.emoji
      .where('subjectID', subjectID)
      .related('creator', creator => creator.one()),
  ),

  // TODO(arv): Remove

  // The below queries are DEPRECATED
  issuePreload: defineQuery(idValidator, ({ctx: auth, args: userID}) =>
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
  prevNext: defineQuery(
    z.object({
      listContext: z.nullable(listContextParams),
      issue: z.nullable(issueRowSortSchema),
      dir: z.union([z.literal('next'), z.literal('prev')]),
    }),
    ({ctx: auth, args: {listContext, issue, dir}}) =>
      buildListQuery({
        listContext: listContext ?? undefined,
        start: issue ?? undefined,
        dir: dir === 'next' ? 'forward' : 'backward',
        role: auth?.role,
      }).one(),
  ),

  issueList: defineQuery(
    z.object({
      listContext: listContextParams,
      userID: z.string(),
      limit: z.number(),
    }),
    ({ctx: auth, args: {listContext, userID, limit}}) =>
      issueListV2(listContext, limit, userID, auth, null, 'forward', false),
  ),

  userPicker: defineQuery(
    z.object({
      disabled: z.boolean(),
      login: z.nullable(z.string()),
      filter: z.nullable(z.enum(['crew', 'creators'])),
    }),
    ({args: {disabled, login, filter}}) => {
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
          throw new QueryError(
            `Unknown filter: ${filter}`,
            QueryErrorCode.UNKNOWN_FILTER,
            filter,
          );
        }
      }
      return q;
    },
  ),
});

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
  inclusive: boolean | undefined,
) {
  return buildListQuery({
    listContext,
    limit: limit ?? undefined,
    userID,
    role: auth?.role,
    start: start ?? undefined,
    dir,
    inclusive,
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
  inclusive?: boolean | undefined;
};

export type ListItemQueryArgs = {
  issueQuery?: (typeof builder)['issue'] | undefined;
  listContext?: ListContext['params'] | undefined;
  project?: string | undefined;
  role?: Role | undefined;
};

function alwaysFalse<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn,
>(q: Query<TTable, TSchema, TReturn>): Query<TTable, TSchema, TReturn> {
  return q.where(({or}) => or());
}

type CommonFilterArgs<Q> = {
  issueQuery: Q;
  listContext: ListContext['params'];
  role?: Role | undefined;
};

// TReturn must be any or the `.one()` case does not match
// oxlint-disable-next-line no-explicit-any
type IssueQuery = Query<'issue', DefaultSchema, any>;

function applyCommonFilters<TQuery extends IssueQuery>({
  issueQuery,
  listContext,
  role,
}: CommonFilterArgs<TQuery>): TQuery {
  const {projectName = ZERO_PROJECT_NAME} = listContext;

  let q = issueQuery.whereExists('project', q =>
    q.where('lowerCaseName', projectName.toLocaleLowerCase()),
  );

  const {open, creator, assignee, labels, textFilter} = listContext;
  q = q.where(({and, cmp, exists, or}) =>
    and(
      // oxlint-disable-next-line eqeqeq
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

  return applyIssuePermissions(q, role) as TQuery;
}

export function buildListQuery(args: ListQueryArgs) {
  const {
    issueQuery = builder.issue,
    limit,
    listContext,
    role,
    dir = 'forward',
    start,
    inclusive = false,
  } = args;

  let q = issueQuery
    .related('viewState', q =>
      (args.userID ? q.where('userID', args.userID) : alwaysFalse(q)).one(),
    )
    .related('labels');

  if (!listContext) {
    return alwaysFalse(q);
  }

  q = applyCommonFilters({issueQuery: q, listContext, role});

  const {sortField, sortDirection} = listContext;
  const orderByDir =
    dir === 'forward'
      ? sortDirection
      : sortDirection === 'asc'
        ? 'desc'
        : 'asc';
  q = q.orderBy(sortField, orderByDir).orderBy('id', orderByDir);

  if (start) {
    q = q.start(start, {inclusive});
  }
  if (limit) {
    q = q.limit(limit);
  }

  return q;
}

export function buildListItemQuery(args: ListItemQueryArgs) {
  const {issueQuery = builder.issue, listContext, role} = args;

  if (!listContext) {
    return alwaysFalse(issueQuery).one();
  }

  const q = applyCommonFilters({issueQuery, listContext, role});
  return q.one();
}
