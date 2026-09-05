import type {Query} from '../../../packages/zql/src/query/query.ts';
import type {BenchmarkModel, BenchmarkProfile} from './config.ts';
import {queries} from './queries.ts';
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
        query: queries.feedRecentEvents({
          bucket: feedBucketForClient(model, clientIndex),
          limit: rowsPerQuery,
        }) as unknown as ThroughputQuery,
      };

    case 'email':
      return buildEmailQuery(model, queryIndex, rowsPerQuery, clientIndex);

    case 'forum':
      return buildForumQuery(model, queryIndex, rowsPerQuery, clientIndex);

    case 'relational':
      return buildRelationalQuery(model, queryIndex, rowsPerQuery, clientIndex);
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
        query: queries.emailThreadListWithMessages({
          ownerID,
          limit: rowsPerQuery,
        }) as unknown as ThroughputQuery,
      };

    case 1:
      return {
        name,
        query: queries.emailMessageListWithThread({
          ownerID,
          limit: rowsPerQuery,
        }) as unknown as ThroughputQuery,
      };

    case 2:
      return {
        name,
        query: queries.emailUnreadThreadList({
          ownerID,
          limit: rowsPerQuery,
        }) as unknown as ThroughputQuery,
      };
  }
  throw new Error(`Invalid email query index: ${queryIndex}`);
}

function buildForumQuery(
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
        query: queries.forumCategoryThreadTree({
          categoryID,
          limit: rowsPerQuery,
        }) as unknown as ThroughputQuery,
      };

    case 1:
      return {
        name,
        query: queries.forumThreadListWithPosts({
          categoryID,
          limit: rowsPerQuery,
        }) as unknown as ThroughputQuery,
      };

    case 2:
      return {
        name,
        query: queries.forumPostListWithThread({
          categoryID,
          limit: rowsPerQuery,
        }) as unknown as ThroughputQuery,
      };
  }
  throw new Error(`Invalid forum query index: ${queryIndex}`);
}

function buildRelationalQuery(
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
        query: queries.relationalOrgAccountTree({
          orgID,
          limit: rowsPerQuery,
        }) as unknown as ThroughputQuery,
      };

    case 1:
      return {
        name,
        query: queries.relationalAccountList({
          orgID,
          limit: rowsPerQuery,
        }) as unknown as ThroughputQuery,
      };

    case 2:
      return {
        name,
        query: queries.relationalActivityList({
          orgID,
          limit: rowsPerQuery,
        }) as unknown as ThroughputQuery,
      };
  }
  throw new Error(`Invalid relational query index: ${queryIndex}`);
}
