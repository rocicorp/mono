import postgres from 'postgres';
import type {BenchmarkConfig} from './config.ts';
import {
  EMAIL_THREAD_COUNT,
  FORUM_CATEGORY_ID,
  FORUM_CATEGORY_SLUG,
  FORUM_THREAD_COUNT,
  FORUM_USER_COUNT,
  REL_ACCOUNT_COUNT,
  REL_CONTACTS_PER_ACCOUNT,
  REL_ORG_ID,
  SHARED_OWNER_ID,
} from './profiles.ts';
import {sleep} from './util.ts';
import {
  REALISTIC_EMAIL_ACTIVE_OWNER_LIMIT,
  REALISTIC_EMAIL_ARCHIVE_THREAD_COUNT,
  REALISTIC_EMAIL_COLD_OWNER_COUNT,
  REALISTIC_EMAIL_INBOX_THREAD_COUNT,
  REALISTIC_FORUM_CATEGORY_COUNT,
  REALISTIC_REL_ACTIVE_ORG_LIMIT,
  REALISTIC_REL_COLD_ORG_COUNT,
  activeRealisticForumCategoryCount,
  activeRealisticRelOrgCount,
  realisticEmailActiveOwnerID,
  realisticEmailArchiveThreadID,
  realisticEmailColdOwnerID,
  realisticEmailInboxThreadID,
  realisticForumCategoryID,
  realisticForumCategorySlug,
  realisticForumThreadID,
  realisticRelAccountID,
  realisticRelActiveOrgID,
  realisticRelColdOrgID,
  realisticRelContactID,
} from './workload-models.ts';

export type BenchmarkDB = postgres.Sql;

export function connectBenchmarkDB(url: string): BenchmarkDB {
  return postgres(url, {
    idle_timeout: 0,
    connect_timeout: 30,
    max_lifetime: null,
    onnotice: () => undefined,
  });
}

export async function waitForPostgres(
  url: string,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    const sql = postgres(url, {
      max: 1,
      idle_timeout: 1,
      connect_timeout: 5,
      onnotice: () => undefined,
    });
    try {
      await sql`SELECT 1`;
      await sql.end();
      return;
    } catch (error) {
      lastError = error;
      await sql.end().catch(() => undefined);
      await sleep(500);
    }
  }
  throw new Error(
    `Timed out waiting for PostgreSQL after ${timeoutMs}ms: ${String(lastError)}`,
  );
}

export async function resetBenchmarkDatabase(
  sql: BenchmarkDB,
  config: BenchmarkConfig,
): Promise<void> {
  const appID = config.zero.appID;
  const schemas = [appID, `${appID}_0`, `${appID}_0/cvr`, `${appID}_0/cdc`];

  for (const schema of schemas) {
    await sql`DROP SCHEMA IF EXISTS ${sql(schema)} CASCADE`;
  }

  await sql`DROP TABLE IF EXISTS zero_throughput_event CASCADE`;
  await sql`DROP TABLE IF EXISTS zero_throughput_email_message CASCADE`;
  await sql`DROP TABLE IF EXISTS zero_throughput_email_thread CASCADE`;
  await sql`DROP TABLE IF EXISTS zero_throughput_forum_post CASCADE`;
  await sql`DROP TABLE IF EXISTS zero_throughput_forum_thread CASCADE`;
  await sql`DROP TABLE IF EXISTS zero_throughput_forum_category CASCADE`;
  await sql`DROP TABLE IF EXISTS zero_throughput_forum_user CASCADE`;
  await sql`DROP TABLE IF EXISTS zero_throughput_rel_activity CASCADE`;
  await sql`DROP TABLE IF EXISTS zero_throughput_rel_contact CASCADE`;
  await sql`DROP TABLE IF EXISTS zero_throughput_rel_account CASCADE`;
  await sql`DROP TABLE IF EXISTS zero_throughput_rel_org CASCADE`;

  await sql`
    CREATE TABLE zero_throughput_event (
      id text PRIMARY KEY,
      profile text NOT NULL,
      shard integer NOT NULL,
      bucket integer NOT NULL,
      seq bigint NOT NULL,
      payload jsonb NOT NULL,
      written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )
  `;
  await sql`
    CREATE UNIQUE INDEX zero_throughput_event_seq_idx
    ON zero_throughput_event (seq)
  `;
  await sql`
    CREATE INDEX zero_throughput_event_bucket_seq_idx
    ON zero_throughput_event (bucket, seq DESC, id ASC)
  `;

  await sql`
    CREATE TABLE zero_throughput_email_thread (
      id text PRIMARY KEY,
      owner_id text NOT NULL,
      mailbox text NOT NULL,
      subject text NOT NULL,
      participant_count integer NOT NULL,
      seq bigint NOT NULL,
      written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )
  `;
  await sql`
    CREATE INDEX zero_throughput_email_thread_owner_mailbox_seq_idx
    ON zero_throughput_email_thread (owner_id, mailbox, seq DESC, id ASC)
  `;
  await sql`
    CREATE TABLE zero_throughput_email_message (
      id text PRIMARY KEY,
      thread_id text NOT NULL,
      owner_id text NOT NULL,
      mailbox text NOT NULL,
      sender_id text NOT NULL,
      unread boolean NOT NULL,
      body text NOT NULL,
      seq bigint NOT NULL,
      written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )
  `;
  await sql`
    CREATE INDEX zero_throughput_email_message_owner_mailbox_seq_idx
    ON zero_throughput_email_message (owner_id, mailbox, seq DESC, id ASC)
  `;
  await sql`
    CREATE INDEX zero_throughput_email_message_thread_seq_idx
    ON zero_throughput_email_message (thread_id, seq DESC, id ASC)
  `;

  await sql`
    CREATE TABLE zero_throughput_forum_user (
      id text PRIMARY KEY,
      name text NOT NULL
    )
  `;
  await sql`
    CREATE TABLE zero_throughput_forum_category (
      id text PRIMARY KEY,
      slug text NOT NULL,
      title text NOT NULL,
      seq bigint NOT NULL,
      written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )
  `;
  await sql`
    CREATE UNIQUE INDEX zero_throughput_forum_category_slug_idx
    ON zero_throughput_forum_category (slug)
  `;
  await sql`
    CREATE TABLE zero_throughput_forum_thread (
      id text PRIMARY KEY,
      category_id text NOT NULL,
      author_id text NOT NULL,
      title text NOT NULL,
      pinned boolean NOT NULL,
      seq bigint NOT NULL,
      written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )
  `;
  await sql`
    CREATE INDEX zero_throughput_forum_thread_category_seq_idx
    ON zero_throughput_forum_thread (category_id, seq DESC, id ASC)
  `;
  await sql`
    CREATE TABLE zero_throughput_forum_post (
      id text PRIMARY KEY,
      thread_id text NOT NULL,
      category_id text NOT NULL,
      author_id text NOT NULL,
      body text NOT NULL,
      seq bigint NOT NULL,
      written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )
  `;
  await sql`
    CREATE INDEX zero_throughput_forum_post_category_seq_idx
    ON zero_throughput_forum_post (category_id, seq DESC, id ASC)
  `;
  await sql`
    CREATE INDEX zero_throughput_forum_post_thread_seq_idx
    ON zero_throughput_forum_post (thread_id, seq DESC, id ASC)
  `;

  await sql`
    CREATE TABLE zero_throughput_rel_org (
      id text PRIMARY KEY,
      name text NOT NULL,
      region text NOT NULL,
      seq bigint NOT NULL,
      written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )
  `;
  await sql`
    CREATE TABLE zero_throughput_rel_account (
      id text PRIMARY KEY,
      org_id text NOT NULL,
      owner_id text NOT NULL,
      name text NOT NULL,
      status text NOT NULL,
      seq bigint NOT NULL,
      written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )
  `;
  await sql`
    CREATE INDEX zero_throughput_rel_account_org_seq_idx
    ON zero_throughput_rel_account (org_id, seq DESC, id ASC)
  `;
  await sql`
    CREATE TABLE zero_throughput_rel_contact (
      id text PRIMARY KEY,
      account_id text NOT NULL,
      name text NOT NULL,
      role text NOT NULL,
      seq bigint NOT NULL,
      written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )
  `;
  await sql`
    CREATE INDEX zero_throughput_rel_contact_account_idx
    ON zero_throughput_rel_contact (account_id, id ASC)
  `;
  await sql`
    CREATE TABLE zero_throughput_rel_activity (
      id text PRIMARY KEY,
      org_id text NOT NULL,
      account_id text NOT NULL,
      contact_id text NOT NULL,
      kind text NOT NULL,
      body text NOT NULL,
      seq bigint NOT NULL,
      written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )
  `;
  await sql`
    CREATE INDEX zero_throughput_rel_activity_org_seq_idx
    ON zero_throughput_rel_activity (org_id, seq DESC, id ASC)
  `;
  await sql`
    CREATE INDEX zero_throughput_rel_activity_account_seq_idx
    ON zero_throughput_rel_activity (account_id, seq DESC, id ASC)
  `;
  await sql`
    CREATE INDEX zero_throughput_rel_activity_contact_seq_idx
    ON zero_throughput_rel_activity (contact_id, seq DESC, id ASC)
  `;

  if (config.model === 'hot') {
    await seedEmail(sql);
    await seedForum(sql);
    await seedRelational(sql);
    return;
  }

  switch (config.profile) {
    case 'feed-append':
      return;
    case 'email':
      await seedRealisticEmail(sql);
      return;
    case 'forum':
      await seedRealisticForum(sql, config);
      return;
    case 'relational':
      await seedRealisticRelational(sql, config);
      return;
  }
}

async function seedEmail(sql: BenchmarkDB): Promise<void> {
  const threads = Array.from({length: EMAIL_THREAD_COUNT}, (_, index) => ({
    id: `email-thread-${index}`,
    owner_id: SHARED_OWNER_ID,
    mailbox: 'inbox',
    subject: `Throughput thread ${index}`,
    participant_count: 3,
    seq: 0,
  }));
  await sql`INSERT INTO zero_throughput_email_thread ${sql(threads)}`;
}

async function seedForum(sql: BenchmarkDB): Promise<void> {
  const users = Array.from({length: FORUM_USER_COUNT}, (_, index) => ({
    id: `forum-user-${index}`,
    name: `Forum User ${index}`,
  }));
  await sql`INSERT INTO zero_throughput_forum_user ${sql(users)}`;
  await sql`
    INSERT INTO zero_throughput_forum_category
      (id, slug, title, seq)
    VALUES
      (${FORUM_CATEGORY_ID}, ${FORUM_CATEGORY_SLUG}, 'General', 0)
  `;
  const threads = Array.from({length: FORUM_THREAD_COUNT}, (_, index) => ({
    id: `forum-thread-${index}`,
    category_id: FORUM_CATEGORY_ID,
    author_id: `forum-user-${index % FORUM_USER_COUNT}`,
    title: `Throughput discussion ${index}`,
    pinned: index === 0,
    seq: 0,
  }));
  await sql`INSERT INTO zero_throughput_forum_thread ${sql(threads)}`;
}

async function seedRelational(sql: BenchmarkDB): Promise<void> {
  await sql`
    INSERT INTO zero_throughput_rel_org
      (id, name, region, seq)
    VALUES
      (${REL_ORG_ID}, 'Throughput Org', 'na', 0)
  `;
  const accounts = Array.from({length: REL_ACCOUNT_COUNT}, (_, index) => ({
    id: `rel-account-${index}`,
    org_id: REL_ORG_ID,
    owner_id: `owner-${index % 8}`,
    name: `Account ${index}`,
    status: index % 3 === 0 ? 'risk' : 'active',
    seq: 0,
  }));
  await sql`INSERT INTO zero_throughput_rel_account ${sql(accounts)}`;

  const contacts = accounts.flatMap(account =>
    Array.from({length: REL_CONTACTS_PER_ACCOUNT}, (_, index) => ({
      id: `${account.id}-contact-${index}`,
      account_id: account.id,
      name: `Contact ${index} at ${account.name}`,
      role: index === 0 ? 'buyer' : 'stakeholder',
      seq: 0,
    })),
  );
  await sql`INSERT INTO zero_throughput_rel_contact ${sql(contacts)}`;
}

async function seedRealisticEmail(sql: BenchmarkDB): Promise<void> {
  const activeOwners = Array.from(
    {length: REALISTIC_EMAIL_ACTIVE_OWNER_LIMIT},
    (_, index) => realisticEmailActiveOwnerID(index),
  );
  const coldOwners = Array.from(
    {length: REALISTIC_EMAIL_COLD_OWNER_COUNT},
    (_, index) => realisticEmailColdOwnerID(index),
  );
  const owners = [...activeOwners, ...coldOwners];
  const threads = owners.flatMap(ownerID => [
    ...Array.from({length: REALISTIC_EMAIL_INBOX_THREAD_COUNT}, (_, index) => ({
      id: realisticEmailInboxThreadID(ownerID, index),
      owner_id: ownerID,
      mailbox: 'inbox',
      subject: `Throughput inbox thread ${index}`,
      participant_count: 3,
      seq: -index,
    })),
    ...Array.from(
      {length: REALISTIC_EMAIL_ARCHIVE_THREAD_COUNT},
      (_, index) => ({
        id: realisticEmailArchiveThreadID(ownerID, index),
        owner_id: ownerID,
        mailbox: 'archive',
        subject: `Throughput archive thread ${index}`,
        participant_count: 3,
        seq: -index,
      }),
    ),
  ]);

  await insertChunks(
    threads,
    chunk => sql`
    INSERT INTO zero_throughput_email_thread ${sql(chunk)}
  `,
  );
}

async function seedRealisticForum(
  sql: BenchmarkDB,
  config: BenchmarkConfig,
): Promise<void> {
  const users = Array.from({length: FORUM_USER_COUNT}, (_, index) => ({
    id: `forum-user-${index}`,
    name: `Forum User ${index}`,
  }));
  await sql`INSERT INTO zero_throughput_forum_user ${sql(users)}`;

  const activeCategoryCount = activeRealisticForumCategoryCount(config.users);
  const categories = Array.from(
    {length: REALISTIC_FORUM_CATEGORY_COUNT},
    (_, index) => ({
      id: realisticForumCategoryID(index),
      slug: realisticForumCategorySlug(index),
      title:
        index < activeCategoryCount
          ? `Active Category ${index}`
          : `Cold Category ${index}`,
      seq: -index,
    }),
  );
  await sql`INSERT INTO zero_throughput_forum_category ${sql(categories)}`;

  const threads = categories.flatMap((category, categoryIndex) =>
    Array.from({length: FORUM_THREAD_COUNT}, (_, index) => ({
      id: realisticForumThreadID(categoryIndex, index),
      category_id: category.id,
      author_id: `forum-user-${index % FORUM_USER_COUNT}`,
      title: `Throughput discussion ${categoryIndex}-${index}`,
      pinned: index === 0,
      seq: -index,
    })),
  );
  await insertChunks(
    threads,
    chunk => sql`
    INSERT INTO zero_throughput_forum_thread ${sql(chunk)}
  `,
  );
}

async function seedRealisticRelational(
  sql: BenchmarkDB,
  config: BenchmarkConfig,
): Promise<void> {
  const activeOrgCount = activeRealisticRelOrgCount(config.users);
  const activeOrgIDs = Array.from(
    {length: REALISTIC_REL_ACTIVE_ORG_LIMIT},
    (_, index) => realisticRelActiveOrgID(index),
  );
  const coldOrgIDs = Array.from(
    {length: REALISTIC_REL_COLD_ORG_COUNT},
    (_, index) => realisticRelColdOrgID(index),
  );
  const orgIDs = [...activeOrgIDs, ...coldOrgIDs];
  const orgs = orgIDs.map((id, index) => ({
    id,
    name:
      index < activeOrgCount
        ? `Active Throughput Org ${index}`
        : `Cold Throughput Org ${index}`,
    region: index % 2 === 0 ? 'na' : 'emea',
    seq: -index,
  }));
  await sql`INSERT INTO zero_throughput_rel_org ${sql(orgs)}`;

  const accounts = orgIDs.flatMap(orgID =>
    Array.from({length: REL_ACCOUNT_COUNT}, (_, index) => ({
      id: realisticRelAccountID(orgID, index),
      org_id: orgID,
      owner_id: `owner-${index % 8}`,
      name: `Account ${index}`,
      status: index % 3 === 0 ? 'risk' : 'active',
      seq: -index,
    })),
  );
  await insertChunks(
    accounts,
    chunk => sql`
    INSERT INTO zero_throughput_rel_account ${sql(chunk)}
  `,
  );

  const contacts = accounts.flatMap(account =>
    Array.from({length: REL_CONTACTS_PER_ACCOUNT}, (_, index) => ({
      id: realisticRelContactID(account.id, index),
      account_id: account.id,
      name: `Contact ${index} at ${account.name}`,
      role: index === 0 ? 'buyer' : 'stakeholder',
      seq: -index,
    })),
  );
  await insertChunks(
    contacts,
    chunk => sql`
    INSERT INTO zero_throughput_rel_contact ${sql(chunk)}
  `,
  );
}

async function insertChunks<T>(
  rows: readonly T[],
  insert: (chunk: T[]) => Promise<unknown>,
): Promise<void> {
  const chunkSize = 500;
  for (let start = 0; start < rows.length; start += chunkSize) {
    await insert(rows.slice(start, start + chunkSize));
  }
}
