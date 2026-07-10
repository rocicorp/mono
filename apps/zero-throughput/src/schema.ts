import {
  boolean,
  createSchema,
  json,
  number,
  relationships,
  string,
  table,
} from '@rocicorp/zero';

export const eventTable = table('event')
  .from('zero_throughput_event')
  .columns({
    id: string(),
    profile: string(),
    shard: number(),
    bucket: number(),
    seq: number(),
    payload: json(),
    writtenAt: number().from('written_at'),
    updatedAt: number().from('updated_at'),
  })
  .primaryKey('id');

export const emailThreadTable = table('emailThread')
  .from('zero_throughput_email_thread')
  .columns({
    id: string(),
    ownerID: string().from('owner_id'),
    mailbox: string(),
    subject: string(),
    participantCount: number().from('participant_count'),
    seq: number(),
    writtenAt: number().from('written_at'),
    updatedAt: number().from('updated_at'),
  })
  .primaryKey('id');

export const emailMessageTable = table('emailMessage')
  .from('zero_throughput_email_message')
  .columns({
    id: string(),
    threadID: string().from('thread_id'),
    ownerID: string().from('owner_id'),
    mailbox: string(),
    senderID: string().from('sender_id'),
    unread: boolean(),
    body: string(),
    seq: number(),
    writtenAt: number().from('written_at'),
    updatedAt: number().from('updated_at'),
  })
  .primaryKey('id');

export const forumUserTable = table('forumUser')
  .from('zero_throughput_forum_user')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

export const forumCategoryTable = table('forumCategory')
  .from('zero_throughput_forum_category')
  .columns({
    id: string(),
    slug: string(),
    title: string(),
    seq: number(),
    writtenAt: number().from('written_at'),
    updatedAt: number().from('updated_at'),
  })
  .primaryKey('id');

export const forumThreadTable = table('forumThread')
  .from('zero_throughput_forum_thread')
  .columns({
    id: string(),
    categoryID: string().from('category_id'),
    authorID: string().from('author_id'),
    title: string(),
    pinned: boolean(),
    seq: number(),
    writtenAt: number().from('written_at'),
    updatedAt: number().from('updated_at'),
  })
  .primaryKey('id');

export const forumPostTable = table('forumPost')
  .from('zero_throughput_forum_post')
  .columns({
    id: string(),
    threadID: string().from('thread_id'),
    categoryID: string().from('category_id'),
    authorID: string().from('author_id'),
    body: string(),
    seq: number(),
    writtenAt: number().from('written_at'),
    updatedAt: number().from('updated_at'),
  })
  .primaryKey('id');

export const relOrgTable = table('relOrg')
  .from('zero_throughput_rel_org')
  .columns({
    id: string(),
    name: string(),
    region: string(),
    seq: number(),
    writtenAt: number().from('written_at'),
    updatedAt: number().from('updated_at'),
  })
  .primaryKey('id');

export const relAccountTable = table('relAccount')
  .from('zero_throughput_rel_account')
  .columns({
    id: string(),
    orgID: string().from('org_id'),
    ownerID: string().from('owner_id'),
    name: string(),
    status: string(),
    seq: number(),
    writtenAt: number().from('written_at'),
    updatedAt: number().from('updated_at'),
  })
  .primaryKey('id');

export const relContactTable = table('relContact')
  .from('zero_throughput_rel_contact')
  .columns({
    id: string(),
    accountID: string().from('account_id'),
    name: string(),
    role: string(),
    seq: number(),
    writtenAt: number().from('written_at'),
    updatedAt: number().from('updated_at'),
  })
  .primaryKey('id');

export const relActivityTable = table('relActivity')
  .from('zero_throughput_rel_activity')
  .columns({
    id: string(),
    orgID: string().from('org_id'),
    accountID: string().from('account_id'),
    contactID: string().from('contact_id'),
    kind: string(),
    body: string(),
    seq: number(),
    writtenAt: number().from('written_at'),
    updatedAt: number().from('updated_at'),
  })
  .primaryKey('id');

const emailThreadRelationships = relationships(emailThreadTable, ({many}) => ({
  messages: many({
    sourceField: ['id'],
    destField: ['threadID'],
    destSchema: emailMessageTable,
  }),
}));

const emailMessageRelationships = relationships(emailMessageTable, ({one}) => ({
  thread: one({
    sourceField: ['threadID'],
    destField: ['id'],
    destSchema: emailThreadTable,
  }),
}));

const forumCategoryRelationships = relationships(
  forumCategoryTable,
  ({many}) => ({
    threads: many({
      sourceField: ['id'],
      destField: ['categoryID'],
      destSchema: forumThreadTable,
    }),
    posts: many({
      sourceField: ['id'],
      destField: ['categoryID'],
      destSchema: forumPostTable,
    }),
  }),
);

const forumThreadRelationships = relationships(
  forumThreadTable,
  ({many, one}) => ({
    category: one({
      sourceField: ['categoryID'],
      destField: ['id'],
      destSchema: forumCategoryTable,
    }),
    author: one({
      sourceField: ['authorID'],
      destField: ['id'],
      destSchema: forumUserTable,
    }),
    posts: many({
      sourceField: ['id'],
      destField: ['threadID'],
      destSchema: forumPostTable,
    }),
  }),
);

const forumPostRelationships = relationships(forumPostTable, ({one}) => ({
  category: one({
    sourceField: ['categoryID'],
    destField: ['id'],
    destSchema: forumCategoryTable,
  }),
  thread: one({
    sourceField: ['threadID'],
    destField: ['id'],
    destSchema: forumThreadTable,
  }),
  author: one({
    sourceField: ['authorID'],
    destField: ['id'],
    destSchema: forumUserTable,
  }),
}));

const relOrgRelationships = relationships(relOrgTable, ({many}) => ({
  accounts: many({
    sourceField: ['id'],
    destField: ['orgID'],
    destSchema: relAccountTable,
  }),
  activities: many({
    sourceField: ['id'],
    destField: ['orgID'],
    destSchema: relActivityTable,
  }),
}));

const relAccountRelationships = relationships(
  relAccountTable,
  ({many, one}) => ({
    org: one({
      sourceField: ['orgID'],
      destField: ['id'],
      destSchema: relOrgTable,
    }),
    contacts: many({
      sourceField: ['id'],
      destField: ['accountID'],
      destSchema: relContactTable,
    }),
    activities: many({
      sourceField: ['id'],
      destField: ['accountID'],
      destSchema: relActivityTable,
    }),
  }),
);

const relContactRelationships = relationships(
  relContactTable,
  ({many, one}) => ({
    account: one({
      sourceField: ['accountID'],
      destField: ['id'],
      destSchema: relAccountTable,
    }),
    activities: many({
      sourceField: ['id'],
      destField: ['contactID'],
      destSchema: relActivityTable,
    }),
  }),
);

const relActivityRelationships = relationships(relActivityTable, ({one}) => ({
  org: one({
    sourceField: ['orgID'],
    destField: ['id'],
    destSchema: relOrgTable,
  }),
  account: one({
    sourceField: ['accountID'],
    destField: ['id'],
    destSchema: relAccountTable,
  }),
  contact: one({
    sourceField: ['contactID'],
    destField: ['id'],
    destSchema: relContactTable,
  }),
}));

export const schema = createSchema({
  tables: [
    eventTable,
    emailThreadTable,
    emailMessageTable,
    forumUserTable,
    forumCategoryTable,
    forumThreadTable,
    forumPostTable,
    relOrgTable,
    relAccountTable,
    relContactTable,
    relActivityTable,
  ],
  relationships: [
    emailThreadRelationships,
    emailMessageRelationships,
    forumCategoryRelationships,
    forumThreadRelationships,
    forumPostRelationships,
    relOrgRelationships,
    relAccountRelationships,
    relContactRelationships,
    relActivityRelationships,
  ],
  enableLegacyQueries: true,
});
