import {
  pgTable,
  uniqueIndex,
  varchar,
  integer,
  index,
  foreignKey,
  boolean,
  doublePrecision,
  text,
  unique,
  primaryKey,
} from 'drizzle-orm/pg-core';
import {sql} from 'drizzle-orm';

export const user = pgTable(
  'user',
  {
    id: varchar().primaryKey().notNull(),
    login: varchar().notNull(),
    name: varchar(),
    avatar: varchar(),
    role: varchar().default('user').notNull(),
    githubID: integer().notNull(),
    email: varchar(),
  },
  table => [
    uniqueIndex('user_githubid_idx').using('btree', table.githubID),
    uniqueIndex('user_login_idx').using('btree', table.login),
  ],
);

export const issue = pgTable(
  'issue',
  {
    id: varchar().primaryKey().notNull(),
    shortID: integer().generatedByDefaultAsIdentity({
      name: 'issue_shortID_seq',
      startWith: 3000,
      increment: 1,
      minValue: 1,
      maxValue: 2147483647,
    }),
    title: varchar({length: 128}).notNull(),
    open: boolean().notNull(),
    modified: doublePrecision().default(
      sql`(EXTRACT(epoch FROM CURRENT_TIMESTAMP) * (1000)::numeric)`,
    ),
    created: doublePrecision().default(
      sql`(EXTRACT(epoch FROM CURRENT_TIMESTAMP) * (1000)::numeric)`,
    ),
    creatorID: varchar().notNull(),
    assigneeID: varchar(),
    description: varchar({length: 10240}).default(''),
    visibility: varchar().default('public').notNull(),
  },
  table => [
    index('issue_created_idx').using('btree', table.created),
    index('issue_modified_idx').using('btree', table.modified),
    index('issue_open_modified_idx').using('btree', table.open, table.modified),
    foreignKey({
      columns: [table.creatorID],
      foreignColumns: [user.id],
      name: 'issue_creatorID_fkey',
    }),
    foreignKey({
      columns: [table.assigneeID],
      foreignColumns: [user.id],
      name: 'issue_assigneeID_fkey',
    }),
  ],
);

export const comment = pgTable(
  'comment',
  {
    id: varchar().primaryKey().notNull(),
    issueID: varchar(),
    created: doublePrecision(),
    body: text().notNull(),
    creatorID: varchar(),
  },
  table => [
    index('comment_issueid_idx').using('btree', table.issueID),
    foreignKey({
      columns: [table.issueID],
      foreignColumns: [issue.id],
      name: 'comment_issueID_fkey',
    }).onDelete('cascade'),
    foreignKey({
      columns: [table.creatorID],
      foreignColumns: [user.id],
      name: 'comment_creatorID_fkey',
    }),
  ],
);

export const label = pgTable('label', {
  id: varchar().primaryKey().notNull(),
  name: varchar().notNull(),
});

export const emoji = pgTable(
  'emoji',
  {
    id: varchar().primaryKey().notNull(),
    value: varchar().notNull(),
    annotation: varchar(),
    subjectID: varchar().notNull(),
    creatorID: varchar(),
    created: doublePrecision().default(
      sql`(EXTRACT(epoch FROM CURRENT_TIMESTAMP) * (1000)::numeric)`,
    ),
  },
  table => [
    index('emoji_created_idx').using('btree', table.created),
    index('emoji_subject_id_idx').using('btree', table.subjectID),
    foreignKey({
      columns: [table.creatorID],
      foreignColumns: [user.id],
      name: 'emoji_creatorID_fkey',
    }).onDelete('cascade'),
    unique('emoji_subjectID_creatorID_value_key').on(
      table.value,
      table.subjectID,
      table.creatorID,
    ),
  ],
);

export const issueLabel = pgTable(
  'issueLabel',
  {
    labelID: varchar().notNull(),
    issueID: varchar().notNull(),
  },
  table => [
    index('issuelabel_issueid_idx').using('btree', table.issueID),
    foreignKey({
      columns: [table.labelID],
      foreignColumns: [label.id],
      name: 'issueLabel_labelID_fkey',
    }),
    foreignKey({
      columns: [table.issueID],
      foreignColumns: [issue.id],
      name: 'issueLabel_issueID_fkey',
    }).onDelete('cascade'),
    primaryKey({
      columns: [table.labelID, table.issueID],
      name: 'issueLabel_pkey',
    }),
  ],
);

export const viewState = pgTable(
  'viewState',
  {
    userID: varchar().notNull(),
    issueID: varchar().notNull(),
    viewed: doublePrecision(),
  },
  table => [
    foreignKey({
      columns: [table.userID],
      foreignColumns: [user.id],
      name: 'viewState_userID_fkey',
    }).onDelete('cascade'),
    foreignKey({
      columns: [table.issueID],
      foreignColumns: [issue.id],
      name: 'viewState_issueID_fkey',
    }).onDelete('cascade'),
    primaryKey({
      columns: [table.userID, table.issueID],
      name: 'viewState_pkey',
    }),
  ],
);

export const userPref = pgTable(
  'userPref',
  {
    key: varchar().notNull(),
    value: varchar().notNull(),
    userID: varchar().notNull(),
  },
  table => [
    foreignKey({
      columns: [table.userID],
      foreignColumns: [user.id],
      name: 'userPref_userID_fkey',
    }).onDelete('cascade'),
    primaryKey({columns: [table.key, table.userID], name: 'userPref_pkey'}),
  ],
);

export const issueNotifications = pgTable(
  'issueNotifications',
  {
    userID: varchar().notNull(),
    issueID: varchar().notNull(),
    subscribed: boolean().default(true),
    created: doublePrecision().default(
      sql`(EXTRACT(epoch FROM CURRENT_TIMESTAMP) * (1000)::numeric)`,
    ),
  },
  table => [
    foreignKey({
      columns: [table.userID],
      foreignColumns: [user.id],
      name: 'issueNotifications_userID_fkey',
    }).onDelete('cascade'),
    foreignKey({
      columns: [table.issueID],
      foreignColumns: [issue.id],
      name: 'issueNotifications_issueID_fkey',
    }).onDelete('cascade'),
    primaryKey({
      columns: [table.userID, table.issueID],
      name: 'issueNotifications_pkey',
    }),
  ],
);
