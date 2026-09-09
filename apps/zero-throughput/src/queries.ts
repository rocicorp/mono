import {defineQueries, defineQuery} from '@rocicorp/zero';
import * as z from 'zod/mini';
import {builder} from './schema.ts';

export const queries = defineQueries({
  feedRecentEvents: defineQuery(
    z.object({
      bucket: z.number(),
      limit: z.number(),
    }),
    ({args: {bucket, limit}}) =>
      builder.event.where('bucket', bucket).orderBy('seq', 'desc').limit(limit),
  ),

  emailThreadListWithMessages: defineQuery(
    z.object({
      ownerID: z.string(),
      limit: z.number(),
    }),
    ({args: {ownerID, limit}}) =>
      builder.emailThread
        .where('ownerID', ownerID)
        .where('mailbox', 'inbox')
        .related('messages', q => q.orderBy('seq', 'desc').limit(5))
        .orderBy('seq', 'desc')
        .limit(limit),
  ),

  emailMessageListWithThread: defineQuery(
    z.object({
      ownerID: z.string(),
      limit: z.number(),
    }),
    ({args: {ownerID, limit}}) =>
      builder.emailMessage
        .where('ownerID', ownerID)
        .where('mailbox', 'inbox')
        .related('thread')
        .orderBy('seq', 'desc')
        .limit(limit),
  ),

  emailUnreadThreadList: defineQuery(
    z.object({
      ownerID: z.string(),
      limit: z.number(),
    }),
    ({args: {ownerID, limit}}) =>
      builder.emailThread
        .where('ownerID', ownerID)
        .where('mailbox', 'inbox')
        .related('messages', q =>
          q.where('unread', true).orderBy('seq', 'desc').limit(10),
        )
        .orderBy('seq', 'desc')
        .limit(limit),
  ),

  forumCategoryThreadTree: defineQuery(
    z.object({
      categoryID: z.string(),
      limit: z.number(),
    }),
    ({args: {categoryID, limit}}) =>
      builder.forumCategory.where('id', categoryID).related('threads', q =>
        q
          .orderBy('seq', 'desc')
          .limit(limit)
          .related('author')
          .related('posts', p =>
            p.orderBy('seq', 'desc').limit(3).related('author'),
          ),
      ),
  ),

  forumThreadListWithPosts: defineQuery(
    z.object({
      categoryID: z.string(),
      limit: z.number(),
    }),
    ({args: {categoryID, limit}}) =>
      builder.forumThread
        .where('categoryID', categoryID)
        .related('category')
        .related('author')
        .related('posts', q =>
          q.orderBy('seq', 'desc').limit(5).related('author'),
        )
        .orderBy('seq', 'desc')
        .limit(limit),
  ),

  forumPostListWithThread: defineQuery(
    z.object({
      categoryID: z.string(),
      limit: z.number(),
    }),
    ({args: {categoryID, limit}}) =>
      builder.forumPost
        .where('categoryID', categoryID)
        .related('thread', q => q.related('author').related('category'))
        .related('author')
        .orderBy('seq', 'desc')
        .limit(limit),
  ),

  relationalOrgAccountTree: defineQuery(
    z.object({
      orgID: z.string(),
      limit: z.number(),
    }),
    ({args: {orgID, limit}}) =>
      builder.relOrg
        .where('id', orgID)
        .related('accounts', q =>
          q
            .orderBy('seq', 'desc')
            .limit(limit)
            .related('contacts')
            .related('activities', a =>
              a.orderBy('seq', 'desc').limit(5).related('contact'),
            ),
        )
        .related('activities', q => q.orderBy('seq', 'desc').limit(limit)),
  ),

  relationalAccountList: defineQuery(
    z.object({
      orgID: z.string(),
      limit: z.number(),
    }),
    ({args: {orgID, limit}}) =>
      builder.relAccount
        .where('orgID', orgID)
        .related('org')
        .related('contacts')
        .related('activities', q =>
          q.orderBy('seq', 'desc').limit(5).related('contact'),
        )
        .orderBy('seq', 'desc')
        .limit(limit),
  ),

  relationalActivityList: defineQuery(
    z.object({
      orgID: z.string(),
      limit: z.number(),
    }),
    ({args: {orgID, limit}}) =>
      builder.relActivity
        .where('orgID', orgID)
        .related('org')
        .related('account', q => q.related('contacts'))
        .related('contact')
        .orderBy('seq', 'desc')
        .limit(limit),
  ),
});
