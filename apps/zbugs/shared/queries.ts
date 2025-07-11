import {escapeLike, named, type Row} from '@rocicorp/zero';
import {builder, type Schema} from './schema.ts';
import {INITIAL_COMMENT_LIMIT} from './consts.ts';

export const allLabels = named('allLabels', () => builder.label);
export const allUsers = named('allUsers', () => builder.user);

export const issuePreload = named('issuePreload', (userID: string) =>
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
);

export const user = named('user', (userID: string) =>
  builder.user.where('id', userID).one(),
);

export const userPref = named('userPref', (key: string, userID: string) =>
  builder.userPref.where('key', key).where('userID', userID).one(),
);

export const userPicker = named(
  'userPicker',
  (
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
);

export const issueDetail = named(
  'issueDetail',
  (idField: 'shortID' | 'id', id: string | number, userID: string) =>
    builder.issue
      .where(idField, id)
      .related('emoji', emoji => emoji.related('creator'))
      .related('creator')
      .related('assignee')
      .related('labels')
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
);

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

export const prevNext = named(
  'prevNext',
  (
    listContext: ListContext['params'] | null,
    issue: Pick<
      Row<Schema['tables']['issue']>,
      'id' | 'created' | 'modified'
    > | null,
    dir: 'next' | 'prev',
  ) => buildListQuery(listContext, issue, dir).one(),
);

export const issueList = named(
  'issueList',
  (listContext: ListContext['params'], userID: string, limit: number) =>
    buildListQuery(listContext, null, 'next')
      .limit(limit)
      .related('viewState', q => q.where('userID', userID).one())
      .related('labels'),
);

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

export const emojiChange = named('emojiChange', (subjectID: string) =>
  builder.emoji
    .where('subjectID', subjectID ?? '')
    .related('creator', creator => creator.one()),
);
