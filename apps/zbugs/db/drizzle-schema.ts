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

export const project = pgTable(
  'project',
  {
    id: varchar().primaryKey().notNull(),
    name: varchar().notNull(),
    // Populated from name by trigger
    lowerCaseName: varchar().default('').notNull(),
    issueCountEstimate: integer(),
    supportsSearch: boolean().default(true),
    markURL: varchar(),
    logoURL: varchar(),
  },
  table => [
    uniqueIndex('project_lower_case_name_idx').using(
      'btree',
      table.lowerCaseName,
    ),
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
    projectID: varchar().default('iCNlS2qEpzYWEes1RTf-D').notNull(),
    creatorID: varchar().notNull(),
    assigneeID: varchar(),
    description: varchar({length: 10240}).default(''),
    visibility: varchar().default('public').notNull(),
  },
  table => [
    uniqueIndex('issue_project_idx').using('btree', table.id, table.projectID),
    index('issue_shortID_idx').using('btree', table.shortID),
    index('issue_created_idx').using('btree', table.created),
    index('issue_projectID_open_assigneeID_modified_idx').using(
      'btree',
      table.projectID,
      table.open,
      table.assigneeID,
      table.modified,
      table.id,
    ),
    index('issue_projectID_open_creatorID_modified_idx').using(
      'btree',
      table.projectID,
      table.open,
      table.creatorID,
      table.modified,
      table.id,
    ),
    index('issue_projectID_open_modified_idx').using(
      'btree',
      table.projectID,
      table.open,
      table.modified,
      table.id,
    ),
    index('issue_projectID_assigneeID_modified_idx').using(
      'btree',
      table.projectID,
      table.assigneeID,
      table.modified,
      table.id,
    ),
    index('issue_projectID_creatorID_modified_idx').using(
      'btree',
      table.projectID,
      table.creatorID,
      table.modified,
      table.id,
    ),
    index('issue_projectID_modified_idx').using(
      'btree',
      table.projectID,
      table.modified,
      table.id,
    ),
    index('issue_creatorID_idx').using('btree', table.creatorID, table.id),
    index('issue_assigneeID_idx').using('btree', table.assigneeID, table.id),
    foreignKey({
      columns: [table.projectID],
      foreignColumns: [project.id],
      name: 'issue_projectID_fkey',
    }),
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

export const label = pgTable(
  'label',
  {
    id: varchar().primaryKey().notNull(),
    name: varchar().notNull(),
    projectID: varchar().default('iCNlS2qEpzYWEes1RTf-D').notNull(),
  },
  table => [
    uniqueIndex('label_project_idx').using('btree', table.id, table.projectID),
    index('label_name_idx').using('btree', table.name),
    foreignKey({
      columns: [table.projectID],
      foreignColumns: [project.id],
      name: 'label_projectID_fkey',
    }),
  ],
);

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
    projectID: varchar().default('iCNlS2qEpzYWEes1RTf-D').notNull(),
  },
  table => [
    index('issuelabel_issueid_idx').using('btree', table.issueID),
    foreignKey({
      columns: [table.labelID, table.projectID],
      foreignColumns: [label.id, label.projectID],
      name: 'issueLabel_labelID_projectID_fkey',
    }),
    foreignKey({
      columns: [table.issueID, table.projectID],
      foreignColumns: [issue.id, issue.projectID],
      name: 'issueLabel_issueID_projectID_fkey',
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
