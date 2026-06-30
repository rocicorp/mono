import type {Query, SchemaQuery} from '@rocicorp/zero';
import type {BenchmarkProfile} from './config.ts';
import {FORUM_CATEGORY_ID, REL_ORG_ID, SHARED_OWNER_ID} from './profiles.ts';
import type {schema} from './schema.ts';

type ThroughputSchema = typeof schema;
type ThroughputTable = keyof ThroughputSchema['tables'] & string;
export type ThroughputQuery = Query<ThroughputTable, ThroughputSchema, object>;

export type BuiltProfileQuery = {
  readonly name: string;
  readonly query: ThroughputQuery;
};

export const PROFILE_QUERY_NAMES = {
  'feed-append': ['feed:recent-events'],
  'email': [
    'email:thread-list-with-messages',
    'email:message-list-with-thread',
    'email:unread-thread-list',
  ],
  'forum': [
    'forum:category-thread-tree',
    'forum:thread-list-with-posts',
    'forum:post-list-with-thread',
  ],
  'relational': [
    'relational:org-account-tree',
    'relational:account-list',
    'relational:activity-list',
  ],
} as const satisfies Record<BenchmarkProfile, readonly string[]>;

export function buildProfileQuery(
  builder: SchemaQuery<ThroughputSchema>,
  profile: BenchmarkProfile,
  queryIndex: number,
  rowsPerQuery: number,
): BuiltProfileQuery {
  switch (profile) {
    case 'feed-append':
      return {
        name: profileQueryName(profile, queryIndex),
        query: builder.event
          .where('bucket', 0)
          .orderBy('seq', 'desc')
          .limit(rowsPerQuery) as ThroughputQuery,
      };

    case 'email':
      return buildEmailQuery(builder, queryIndex, rowsPerQuery);

    case 'forum':
      return buildForumQuery(builder, queryIndex, rowsPerQuery);

    case 'relational':
      return buildRelationalQuery(builder, queryIndex, rowsPerQuery);
  }
}

export function profileQueryName(
  profile: BenchmarkProfile,
  queryIndex: number,
): string {
  const names = PROFILE_QUERY_NAMES[profile];
  return names[normalizeProfileQueryIndex(profile, queryIndex)];
}

export function normalizeProfileQueryIndex(
  profile: BenchmarkProfile,
  queryIndex: number,
): number {
  const names = PROFILE_QUERY_NAMES[profile];
  return ((queryIndex % names.length) + names.length) % names.length;
}

export function findProfileQuery(
  name: string,
):
  | {readonly profile: BenchmarkProfile; readonly queryIndex: number}
  | undefined {
  for (const [profile, names] of Object.entries(PROFILE_QUERY_NAMES)) {
    const queryIndex = (names as readonly string[]).indexOf(name);
    if (queryIndex !== -1) {
      return {profile: profile as BenchmarkProfile, queryIndex};
    }
  }
  return undefined;
}

function buildEmailQuery(
  builder: SchemaQuery<ThroughputSchema>,
  queryIndex: number,
  rowsPerQuery: number,
): BuiltProfileQuery {
  const name = profileQueryName('email', queryIndex);
  switch (normalizeProfileQueryIndex('email', queryIndex)) {
    case 0:
      return {
        name,
        query: builder.emailThread
          .where('ownerID', SHARED_OWNER_ID)
          .where('mailbox', 'inbox')
          .related('messages', q => q.orderBy('seq', 'desc').limit(5))
          .orderBy('seq', 'desc')
          .limit(rowsPerQuery) as ThroughputQuery,
      };

    case 1:
      return {
        name,
        query: builder.emailMessage
          .where('ownerID', SHARED_OWNER_ID)
          .where('mailbox', 'inbox')
          .related('thread')
          .orderBy('seq', 'desc')
          .limit(rowsPerQuery) as ThroughputQuery,
      };

    case 2:
      return {
        name,
        query: builder.emailThread
          .where('ownerID', SHARED_OWNER_ID)
          .where('mailbox', 'inbox')
          .related('messages', q =>
            q.where('unread', true).orderBy('seq', 'desc').limit(10),
          )
          .orderBy('seq', 'desc')
          .limit(rowsPerQuery) as ThroughputQuery,
      };
  }
  throw new Error(`Invalid email query index: ${queryIndex}`);
}

function buildForumQuery(
  builder: SchemaQuery<ThroughputSchema>,
  queryIndex: number,
  rowsPerQuery: number,
): BuiltProfileQuery {
  const name = profileQueryName('forum', queryIndex);
  switch (normalizeProfileQueryIndex('forum', queryIndex)) {
    case 0:
      return {
        name,
        query: builder.forumCategory
          .where('id', FORUM_CATEGORY_ID)
          .related('threads', q =>
            q
              .orderBy('seq', 'desc')
              .limit(rowsPerQuery)
              .related('author')
              .related('posts', p =>
                p.orderBy('seq', 'desc').limit(3).related('author'),
              ),
          ) as ThroughputQuery,
      };

    case 1:
      return {
        name,
        query: builder.forumThread
          .where('categoryID', FORUM_CATEGORY_ID)
          .related('category')
          .related('author')
          .related('posts', q =>
            q.orderBy('seq', 'desc').limit(5).related('author'),
          )
          .orderBy('seq', 'desc')
          .limit(rowsPerQuery) as ThroughputQuery,
      };

    case 2:
      return {
        name,
        query: builder.forumPost
          .where('categoryID', FORUM_CATEGORY_ID)
          .related('thread', q => q.related('author').related('category'))
          .related('author')
          .orderBy('seq', 'desc')
          .limit(rowsPerQuery) as ThroughputQuery,
      };
  }
  throw new Error(`Invalid forum query index: ${queryIndex}`);
}

function buildRelationalQuery(
  builder: SchemaQuery<ThroughputSchema>,
  queryIndex: number,
  rowsPerQuery: number,
): BuiltProfileQuery {
  const name = profileQueryName('relational', queryIndex);
  switch (normalizeProfileQueryIndex('relational', queryIndex)) {
    case 0:
      return {
        name,
        query: builder.relOrg
          .where('id', REL_ORG_ID)
          .related('accounts', q =>
            q
              .orderBy('seq', 'desc')
              .limit(rowsPerQuery)
              .related('contacts')
              .related('activities', a =>
                a.orderBy('seq', 'desc').limit(5).related('contact'),
              ),
          )
          .related('activities', q =>
            q.orderBy('seq', 'desc').limit(rowsPerQuery),
          ) as ThroughputQuery,
      };

    case 1:
      return {
        name,
        query: builder.relAccount
          .where('orgID', REL_ORG_ID)
          .related('org')
          .related('contacts')
          .related('activities', q =>
            q.orderBy('seq', 'desc').limit(5).related('contact'),
          )
          .orderBy('seq', 'desc')
          .limit(rowsPerQuery) as ThroughputQuery,
      };

    case 2:
      return {
        name,
        query: builder.relActivity
          .where('orgID', REL_ORG_ID)
          .related('org')
          .related('account', q => q.related('contacts'))
          .related('contact')
          .orderBy('seq', 'desc')
          .limit(rowsPerQuery) as ThroughputQuery,
      };
  }
  throw new Error(`Invalid relational query index: ${queryIndex}`);
}
