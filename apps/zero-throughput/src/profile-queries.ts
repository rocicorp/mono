import type {Query, SchemaQuery} from '@rocicorp/zero';
import type {BenchmarkModel, BenchmarkProfile} from './config.ts';
import type {schema} from './schema.ts';
import {
  emailOwnerIDForClient,
  feedBucketForClient,
  forumCategoryIDForClient,
  relOrgIDForClient,
} from './workload-models.ts';

type ThroughputSchema = typeof schema;
type ThroughputTable = keyof ThroughputSchema['tables'];
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
  model: BenchmarkModel,
  queryIndex: number,
  rowsPerQuery: number,
  clientIndex: number,
): BuiltProfileQuery {
  switch (profile) {
    case 'feed-append':
      return {
        name: profileQueryName(profile, queryIndex),
        query: builder.event
          .where('bucket', feedBucketForClient(model, clientIndex))
          .orderBy('seq', 'desc')
          .limit(rowsPerQuery) as ThroughputQuery,
      };

    case 'email':
      return buildEmailQuery(
        builder,
        model,
        queryIndex,
        rowsPerQuery,
        clientIndex,
      );

    case 'forum':
      return buildForumQuery(
        builder,
        model,
        queryIndex,
        rowsPerQuery,
        clientIndex,
      );

    case 'relational':
      return buildRelationalQuery(
        builder,
        model,
        queryIndex,
        rowsPerQuery,
        clientIndex,
      );
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

export function profileQueryIndexesForRun(
  profile: BenchmarkProfile,
  queriesPerUser: number,
): readonly number[] {
  const indexes: number[] = [];
  const seen = new Set<string>();
  for (let queryIndex = 0; queryIndex < queriesPerUser; queryIndex++) {
    const normalized = normalizeProfileQueryIndex(profile, queryIndex);
    const name = profileQueryName(profile, normalized);
    if (!seen.has(name)) {
      indexes.push(normalized);
      seen.add(name);
    }
  }
  return indexes;
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
  model: BenchmarkModel,
  queryIndex: number,
  rowsPerQuery: number,
  clientIndex: number,
): BuiltProfileQuery {
  const name = profileQueryName('email', queryIndex);
  const ownerID = emailOwnerIDForClient(model, clientIndex);
  switch (normalizeProfileQueryIndex('email', queryIndex)) {
    case 0:
      return {
        name,
        query: builder.emailThread
          .where('ownerID', ownerID)
          .where('mailbox', 'inbox')
          .related('messages', q => q.orderBy('seq', 'desc').limit(5))
          .orderBy('seq', 'desc')
          .limit(rowsPerQuery) as ThroughputQuery,
      };

    case 1:
      return {
        name,
        query: builder.emailMessage
          .where('ownerID', ownerID)
          .where('mailbox', 'inbox')
          .related('thread')
          .orderBy('seq', 'desc')
          .limit(rowsPerQuery) as ThroughputQuery,
      };

    case 2:
      return {
        name,
        query: builder.emailThread
          .where('ownerID', ownerID)
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
  model: BenchmarkModel,
  queryIndex: number,
  rowsPerQuery: number,
  clientIndex: number,
): BuiltProfileQuery {
  const name = profileQueryName('forum', queryIndex);
  const categoryID = forumCategoryIDForClient(model, clientIndex);
  switch (normalizeProfileQueryIndex('forum', queryIndex)) {
    case 0:
      return {
        name,
        query: builder.forumCategory
          .where('id', categoryID)
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
          .where('categoryID', categoryID)
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
          .where('categoryID', categoryID)
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
  model: BenchmarkModel,
  queryIndex: number,
  rowsPerQuery: number,
  clientIndex: number,
): BuiltProfileQuery {
  const name = profileQueryName('relational', queryIndex);
  const orgID = relOrgIDForClient(model, clientIndex);
  switch (normalizeProfileQueryIndex('relational', queryIndex)) {
    case 0:
      return {
        name,
        query: builder.relOrg
          .where('id', orgID)
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
          .where('orgID', orgID)
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
          .where('orgID', orgID)
          .related('org')
          .related('account', q => q.related('contacts'))
          .related('contact')
          .orderBy('seq', 'desc')
          .limit(rowsPerQuery) as ThroughputQuery,
      };
  }
  throw new Error(`Invalid relational query index: ${queryIndex}`);
}
