import type postgres from 'postgres';
import type {BenchmarkConfig, BenchmarkModel} from './config.ts';
import {
  EMAIL_THREAD_COUNT,
  FORUM_CATEGORY_ID,
  FORUM_THREAD_COUNT,
  FORUM_USER_COUNT,
  REL_ACCOUNT_COUNT,
  REL_CONTACTS_PER_ACCOUNT,
  REL_ORG_ID,
  SHARED_OWNER_ID,
} from './profiles.ts';

export const REALISTIC_FEED_BUCKET_COUNT = 64;
export const REALISTIC_FEED_ACTIVE_BUCKET_LIMIT = 16;

export const REALISTIC_EMAIL_ACTIVE_OWNER_LIMIT = 100;
export const REALISTIC_EMAIL_COLD_OWNER_COUNT = 100;
export const REALISTIC_EMAIL_INBOX_THREAD_COUNT = EMAIL_THREAD_COUNT;
export const REALISTIC_EMAIL_ARCHIVE_THREAD_COUNT = 32;

export const REALISTIC_FORUM_CATEGORY_COUNT = 32;
export const REALISTIC_FORUM_ACTIVE_CATEGORY_LIMIT = 8;

export const REALISTIC_REL_ACTIVE_ORG_LIMIT = 100;
export const REALISTIC_REL_COLD_ORG_COUNT = 100;

export type WriteImpact = {
  readonly activePartition: boolean;
  readonly affectedActiveClientGroups: number;
  readonly visibleRows: boolean;
};

export type WriteImpactTotals = {
  readonly totalLogicalWrites: number;
  readonly activePartitionWrites: number;
  readonly zeroActiveClientGroupWrites: number;
  readonly affectedActiveClientGroupWrites: number;
  readonly visibleRowWrites: number;
  readonly nonVisibleRowWrites: number;
};

type WriteSQL = postgres.Sql | postgres.TransactionSql;

export type ThroughputWriteModel = {
  readonly name: BenchmarkModel;
  writeOne(sql: WriteSQL, seq: number): Promise<WriteImpact>;
};

export function createThroughputWriteModel(
  config: BenchmarkConfig,
  payload: string,
): ThroughputWriteModel {
  return {
    name: config.model,
    writeOne: (sql, seq) => writeOne(config, payload, sql, seq),
  };
}

export function emptyWriteImpactTotals(): WriteImpactTotals {
  return {
    totalLogicalWrites: 0,
    activePartitionWrites: 0,
    zeroActiveClientGroupWrites: 0,
    affectedActiveClientGroupWrites: 0,
    visibleRowWrites: 0,
    nonVisibleRowWrites: 0,
  };
}

export function addWriteImpact(
  totals: WriteImpactTotals,
  impact: WriteImpact,
): WriteImpactTotals {
  return {
    totalLogicalWrites: totals.totalLogicalWrites + 1,
    activePartitionWrites:
      totals.activePartitionWrites + (impact.activePartition ? 1 : 0),
    zeroActiveClientGroupWrites:
      totals.zeroActiveClientGroupWrites +
      (impact.affectedActiveClientGroups === 0 ? 1 : 0),
    affectedActiveClientGroupWrites:
      totals.affectedActiveClientGroupWrites +
      (impact.affectedActiveClientGroups > 0 ? 1 : 0),
    visibleRowWrites: totals.visibleRowWrites + (impact.visibleRows ? 1 : 0),
    nonVisibleRowWrites:
      totals.nonVisibleRowWrites + (impact.visibleRows ? 0 : 1),
  };
}

export function feedBucketForClient(
  model: BenchmarkModel,
  clientIndex: number,
): number {
  return model === 'hot'
    ? 0
    : positiveModulo(clientIndex, REALISTIC_FEED_ACTIVE_BUCKET_LIMIT);
}

export function emailOwnerIDForClient(
  model: BenchmarkModel,
  clientIndex: number,
): string {
  return model === 'hot'
    ? SHARED_OWNER_ID
    : realisticEmailActiveOwnerID(
        positiveModulo(clientIndex, REALISTIC_EMAIL_ACTIVE_OWNER_LIMIT),
      );
}

export function forumCategoryIDForClient(
  model: BenchmarkModel,
  clientIndex: number,
): string {
  return model === 'hot'
    ? FORUM_CATEGORY_ID
    : realisticForumCategoryID(
        positiveModulo(clientIndex, REALISTIC_FORUM_ACTIVE_CATEGORY_LIMIT),
      );
}

export function relOrgIDForClient(
  model: BenchmarkModel,
  clientIndex: number,
): string {
  return model === 'hot'
    ? REL_ORG_ID
    : realisticRelActiveOrgID(
        positiveModulo(clientIndex, REALISTIC_REL_ACTIVE_ORG_LIMIT),
      );
}

export function activeRealisticFeedBucketCount(users: number): number {
  return boundedActiveCount(users, REALISTIC_FEED_ACTIVE_BUCKET_LIMIT);
}

export function activeRealisticEmailOwnerCount(users: number): number {
  return boundedActiveCount(users, REALISTIC_EMAIL_ACTIVE_OWNER_LIMIT);
}

export function activeRealisticForumCategoryCount(users: number): number {
  return boundedActiveCount(users, REALISTIC_FORUM_ACTIVE_CATEGORY_LIMIT);
}

export function activeRealisticRelOrgCount(users: number): number {
  return boundedActiveCount(users, REALISTIC_REL_ACTIVE_ORG_LIMIT);
}

export function realisticEmailActiveOwnerID(index: number): string {
  return `email-owner-active-${index}`;
}

export function realisticEmailColdOwnerID(index: number): string {
  return `email-owner-cold-${index}`;
}

export function realisticEmailInboxThreadID(
  ownerID: string,
  index: number,
): string {
  return `${ownerID}-inbox-thread-${index}`;
}

export function realisticEmailArchiveThreadID(
  ownerID: string,
  index: number,
): string {
  return `${ownerID}-archive-thread-${index}`;
}

export function realisticForumCategoryID(index: number): string {
  return `forum-category-${index}`;
}

export function realisticForumCategorySlug(index: number): string {
  return `category-${index}`;
}

export function realisticForumThreadID(
  categoryIndex: number,
  threadIndex: number,
): string {
  return `forum-thread-${categoryIndex}-${threadIndex}`;
}

export function realisticRelActiveOrgID(index: number): string {
  return `rel-org-active-${index}`;
}

export function realisticRelColdOrgID(index: number): string {
  return `rel-org-cold-${index}`;
}

export function realisticRelAccountID(
  orgID: string,
  accountIndex: number,
): string {
  return `${orgID}-account-${accountIndex}`;
}

export function realisticRelContactID(
  accountID: string,
  contactIndex: number,
): string {
  return `${accountID}-contact-${contactIndex}`;
}

function writeOne(
  config: BenchmarkConfig,
  payload: string,
  sql: WriteSQL,
  seq: number,
): Promise<WriteImpact> {
  if (config.model === 'hot') {
    return writeHot(config, payload, sql, seq);
  }
  return writeRealistic(config, payload, sql, seq);
}

async function writeHot(
  config: BenchmarkConfig,
  payload: string,
  sql: WriteSQL,
  seq: number,
): Promise<WriteImpact> {
  switch (config.profile) {
    case 'feed-append':
      await writeEvent(sql, config, payload, seq, 0);
      return visibleImpact();

    case 'email': {
      const threadID = `email-thread-${seq % EMAIL_THREAD_COUNT}`;
      await insertEmailMessage(sql, {
        config,
        payload,
        seq,
        threadID,
        ownerID: SHARED_OWNER_ID,
        mailbox: 'inbox',
      });
      await updateEmailThreadSeq(sql, threadID, seq);
      return visibleImpact();
    }

    case 'forum': {
      const threadID = `forum-thread-${seq % FORUM_THREAD_COUNT}`;
      const authorID = `forum-user-${seq % FORUM_USER_COUNT}`;
      await insertForumPost(sql, {
        config,
        payload,
        seq,
        threadID,
        categoryID: FORUM_CATEGORY_ID,
        authorID,
      });
      await updateForumThreadSeq(sql, threadID, seq);
      await updateForumCategorySeq(sql, FORUM_CATEGORY_ID, seq);
      return visibleImpact();
    }

    case 'relational': {
      const accountIndex = seq % REL_ACCOUNT_COUNT;
      const contactIndex = seq % REL_CONTACTS_PER_ACCOUNT;
      const accountID = `rel-account-${accountIndex}`;
      const contactID = `${accountID}-contact-${contactIndex}`;
      await insertRelActivity(sql, {
        config,
        payload,
        seq,
        orgID: REL_ORG_ID,
        accountID,
        contactID,
      });
      await updateRelAccountSeq(sql, accountID, seq);
      await updateRelOrgSeq(sql, REL_ORG_ID, seq);
      return visibleImpact();
    }
  }
}

function writeRealistic(
  config: BenchmarkConfig,
  payload: string,
  sql: WriteSQL,
  seq: number,
): Promise<WriteImpact> {
  switch (config.profile) {
    case 'feed-append':
      return writeRealisticFeedAppend(config, payload, sql, seq);
    case 'email':
      return writeRealisticEmail(config, payload, sql, seq);
    case 'forum':
      return writeRealisticForum(config, payload, sql, seq);
    case 'relational':
      return writeRealisticRelational(config, payload, sql, seq);
  }
}

async function writeRealisticFeedAppend(
  config: BenchmarkConfig,
  payload: string,
  sql: WriteSQL,
  seq: number,
): Promise<WriteImpact> {
  const activeCount = activeRealisticFeedBucketCount(config.users);
  const active = distributionBucket(seq, 10) < 3;
  const bucket = active
    ? deterministicIndex(seq, activeCount)
    : activeCount +
      deterministicIndex(seq, REALISTIC_FEED_BUCKET_COUNT - activeCount);

  await writeEvent(sql, config, payload, seq, bucket);
  return impact(active, active ? 1 : 0, active);
}

async function writeRealisticEmail(
  config: BenchmarkConfig,
  payload: string,
  sql: WriteSQL,
  seq: number,
): Promise<WriteImpact> {
  const bucket = distributionBucket(seq, 10);
  if (bucket < 2) {
    const ownerID = realisticEmailActiveOwnerID(
      deterministicIndex(seq, activeRealisticEmailOwnerCount(config.users)),
    );
    const threadID = realisticEmailInboxThreadID(
      ownerID,
      deterministicIndex(seq, REALISTIC_EMAIL_INBOX_THREAD_COUNT),
    );
    await insertEmailMessage(sql, {
      config,
      payload,
      seq,
      threadID,
      ownerID,
      mailbox: 'inbox',
    });
    await updateEmailThreadSeq(sql, threadID, seq);
    return visibleImpact();
  }

  if (bucket < 6) {
    const ownerID = realisticEmailColdOwnerID(
      deterministicIndex(seq, REALISTIC_EMAIL_COLD_OWNER_COUNT),
    );
    const threadID = realisticEmailInboxThreadID(
      ownerID,
      deterministicIndex(seq, REALISTIC_EMAIL_INBOX_THREAD_COUNT),
    );
    await insertEmailMessage(sql, {
      config,
      payload,
      seq,
      threadID,
      ownerID,
      mailbox: 'inbox',
    });
    await updateEmailThreadSeq(sql, threadID, seq);
    return impact(false, 0, false);
  }

  if (bucket < 8) {
    const ownerID = realisticEmailActiveOwnerID(
      deterministicIndex(seq, activeRealisticEmailOwnerCount(config.users)),
    );
    const threadID = realisticEmailArchiveThreadID(
      ownerID,
      deterministicIndex(seq, REALISTIC_EMAIL_ARCHIVE_THREAD_COUNT),
    );
    await insertEmailMessage(sql, {
      config,
      payload,
      seq,
      threadID,
      ownerID,
      mailbox: 'archive',
    });
    await updateEmailThreadSeq(sql, threadID, seq);
    return impact(true, 0, false);
  }

  const metadataTargetsActiveOwner = Math.floor((seq - 1) / 10) % 2 === 0;
  const ownerID = metadataTargetsActiveOwner
    ? realisticEmailActiveOwnerID(
        deterministicIndex(seq, activeRealisticEmailOwnerCount(config.users)),
      )
    : realisticEmailColdOwnerID(
        deterministicIndex(seq, REALISTIC_EMAIL_COLD_OWNER_COUNT),
      );
  const threadIndex = outsideWindowIndex(
    config.rowsPerQuery,
    REALISTIC_EMAIL_INBOX_THREAD_COUNT,
    seq,
  );
  await sql`
    UPDATE zero_throughput_email_thread
    SET
      participant_count = participant_count + 1,
      updated_at = clock_timestamp()
    WHERE id = ${realisticEmailInboxThreadID(ownerID, threadIndex)}
  `;
  const visibleRows =
    metadataTargetsActiveOwner && threadIndex < config.rowsPerQuery;
  return impact(metadataTargetsActiveOwner, visibleRows ? 1 : 0, visibleRows);
}

async function writeRealisticForum(
  config: BenchmarkConfig,
  payload: string,
  sql: WriteSQL,
  seq: number,
): Promise<WriteImpact> {
  const activeCount = activeRealisticForumCategoryCount(config.users);
  const active = distributionBucket(seq, 4) === 0;
  const categoryIndex = active
    ? deterministicIndex(seq, activeCount)
    : activeCount +
      deterministicIndex(seq, REALISTIC_FORUM_CATEGORY_COUNT - activeCount);
  const categoryID = realisticForumCategoryID(categoryIndex);
  const threadID = realisticForumThreadID(
    categoryIndex,
    deterministicIndex(seq, FORUM_THREAD_COUNT),
  );
  const authorID = `forum-user-${seq % FORUM_USER_COUNT}`;

  await insertForumPost(sql, {
    config,
    payload,
    seq,
    threadID,
    categoryID,
    authorID,
  });
  await updateForumThreadSeq(sql, threadID, seq);
  await updateForumCategorySeq(sql, categoryID, seq);
  return impact(active, active ? 1 : 0, active);
}

async function writeRealisticRelational(
  config: BenchmarkConfig,
  payload: string,
  sql: WriteSQL,
  seq: number,
): Promise<WriteImpact> {
  const bucket = distributionBucket(seq, 10);
  if (bucket < 2) {
    const orgID = realisticRelActiveOrgID(
      deterministicIndex(seq, activeRealisticRelOrgCount(config.users)),
    );
    await insertRealisticRelActivity(config, payload, sql, seq, orgID);
    return visibleImpact();
  }

  if (bucket < 9) {
    const orgID = realisticRelColdOrgID(
      deterministicIndex(seq, REALISTIC_REL_COLD_ORG_COUNT),
    );
    await insertRealisticRelActivity(config, payload, sql, seq, orgID);
    return impact(false, 0, false);
  }

  const metadataTargetsActiveOrg = Math.floor((seq - 1) / 10) % 2 === 0;
  const orgID = metadataTargetsActiveOrg
    ? realisticRelActiveOrgID(
        deterministicIndex(seq, activeRealisticRelOrgCount(config.users)),
      )
    : realisticRelColdOrgID(
        deterministicIndex(seq, REALISTIC_REL_COLD_ORG_COUNT),
      );
  await updateRelOrgSeq(sql, orgID, seq);
  return impact(
    metadataTargetsActiveOrg,
    metadataTargetsActiveOrg ? 1 : 0,
    metadataTargetsActiveOrg,
  );
}

async function insertRealisticRelActivity(
  config: BenchmarkConfig,
  payload: string,
  sql: WriteSQL,
  seq: number,
  orgID: string,
): Promise<void> {
  const accountID = realisticRelAccountID(
    orgID,
    deterministicIndex(seq, REL_ACCOUNT_COUNT),
  );
  const contactID = realisticRelContactID(
    accountID,
    deterministicIndex(seq, REL_CONTACTS_PER_ACCOUNT),
  );
  await insertRelActivity(sql, {
    config,
    payload,
    seq,
    orgID,
    accountID,
    contactID,
  });
  await updateRelAccountSeq(sql, accountID, seq);
}

async function writeEvent(
  sql: WriteSQL,
  config: BenchmarkConfig,
  payload: string,
  seq: number,
  bucket: number,
): Promise<void> {
  await sql`
    INSERT INTO zero_throughput_event
      (id, profile, shard, bucket, seq, payload)
    VALUES
      (
        ${`${config.runID}-${seq}`},
        ${config.profile},
        0,
        ${bucket},
        ${seq},
        ${sql.json({data: payload})}
      )
  `;
}

async function insertEmailMessage(
  sql: WriteSQL,
  args: {
    readonly config: BenchmarkConfig;
    readonly payload: string;
    readonly seq: number;
    readonly threadID: string;
    readonly ownerID: string;
    readonly mailbox: string;
  },
): Promise<void> {
  await sql`
    INSERT INTO zero_throughput_email_message
      (id, thread_id, owner_id, mailbox, sender_id, unread, body, seq)
    VALUES
      (
        ${`${args.config.runID}-email-message-${args.seq}`},
        ${args.threadID},
        ${args.ownerID},
        ${args.mailbox},
        ${`sender-${args.seq % 16}`},
        true,
        ${args.payload},
        ${args.seq}
      )
  `;
}

async function updateEmailThreadSeq(
  sql: WriteSQL,
  threadID: string,
  seq: number,
): Promise<void> {
  await sql`
    UPDATE zero_throughput_email_thread
    SET
      seq = ${seq},
      written_at = clock_timestamp(),
      updated_at = clock_timestamp()
    WHERE id = ${threadID}
  `;
}

async function insertForumPost(
  sql: WriteSQL,
  args: {
    readonly config: BenchmarkConfig;
    readonly payload: string;
    readonly seq: number;
    readonly threadID: string;
    readonly categoryID: string;
    readonly authorID: string;
  },
): Promise<void> {
  await sql`
    INSERT INTO zero_throughput_forum_post
      (id, thread_id, category_id, author_id, body, seq)
    VALUES
      (
        ${`${args.config.runID}-forum-post-${args.seq}`},
        ${args.threadID},
        ${args.categoryID},
        ${args.authorID},
        ${args.payload},
        ${args.seq}
      )
  `;
}

async function updateForumThreadSeq(
  sql: WriteSQL,
  threadID: string,
  seq: number,
): Promise<void> {
  await sql`
    UPDATE zero_throughput_forum_thread
    SET
      seq = ${seq},
      written_at = clock_timestamp(),
      updated_at = clock_timestamp()
    WHERE id = ${threadID}
  `;
}

async function updateForumCategorySeq(
  sql: WriteSQL,
  categoryID: string,
  seq: number,
): Promise<void> {
  await sql`
    UPDATE zero_throughput_forum_category
    SET
      seq = ${seq},
      written_at = clock_timestamp(),
      updated_at = clock_timestamp()
    WHERE id = ${categoryID}
  `;
}

async function insertRelActivity(
  sql: WriteSQL,
  args: {
    readonly config: BenchmarkConfig;
    readonly payload: string;
    readonly seq: number;
    readonly orgID: string;
    readonly accountID: string;
    readonly contactID: string;
  },
): Promise<void> {
  await sql`
    INSERT INTO zero_throughput_rel_activity
      (id, org_id, account_id, contact_id, kind, body, seq)
    VALUES
      (
        ${`${args.config.runID}-rel-activity-${args.seq}`},
        ${args.orgID},
        ${args.accountID},
        ${args.contactID},
        ${args.seq % 5 === 0 ? 'meeting' : 'note'},
        ${args.payload},
        ${args.seq}
      )
  `;
}

async function updateRelAccountSeq(
  sql: WriteSQL,
  accountID: string,
  seq: number,
): Promise<void> {
  await sql`
    UPDATE zero_throughput_rel_account
    SET
      seq = ${seq},
      written_at = clock_timestamp(),
      updated_at = clock_timestamp()
    WHERE id = ${accountID}
  `;
}

async function updateRelOrgSeq(
  sql: WriteSQL,
  orgID: string,
  seq: number,
): Promise<void> {
  await sql`
    UPDATE zero_throughput_rel_org
    SET
      seq = ${seq},
      written_at = clock_timestamp(),
      updated_at = clock_timestamp()
    WHERE id = ${orgID}
  `;
}

function visibleImpact(): WriteImpact {
  return impact(true, 1, true);
}

function impact(
  activePartition: boolean,
  affectedActiveClientGroups: number,
  visibleRows: boolean,
): WriteImpact {
  return {
    activePartition,
    affectedActiveClientGroups,
    visibleRows,
  };
}

function boundedActiveCount(users: number, limit: number): number {
  return Math.max(1, Math.min(users, limit));
}

function deterministicIndex(seq: number, count: number): number {
  if (count <= 0) {
    throw new Error(`Cannot choose from ${count} partitions`);
  }
  return positiveModulo(seq - 1, count);
}

function distributionBucket(seq: number, bucketCount: number): number {
  return deterministicIndex(seq, bucketCount);
}

function outsideWindowIndex(
  rowsPerQuery: number,
  rowCount: number,
  seq: number,
): number {
  if (rowsPerQuery >= rowCount) {
    return rowCount - 1;
  }
  return rowsPerQuery + deterministicIndex(seq, rowCount - rowsPerQuery);
}

function positiveModulo(value: number, divisor: number): number {
  return ((value % divisor) + divisor) % divisor;
}
