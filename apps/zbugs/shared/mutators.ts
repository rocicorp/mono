import type {Transaction} from '@rocicorp/zero';
import {z} from 'zod/mini';
import {
  assertIsCreatorOrAdmin,
  assertIsLoggedIn,
  assertUserCanSeeComment,
  assertUserCanSeeIssue,
  isAdmin,
  type AuthData,
} from './auth.ts';
import {MutationError, MutationErrorCode} from './error.ts';
import {
  builder,
  defineMutator,
  defineMutators,
  type schema,
  ZERO_PROJECT_ID,
} from './schema.ts';

function projectIDWithDefault(projectID: string | undefined): string {
  return projectID ?? ZERO_PROJECT_ID;
}

const addEmojiSchema = z.object({
  id: z.string(),
  unicode: z.string(),
  annotation: z.string(),
  subjectID: z.string(),
  created: z.number(),
});

export type AddEmojiArgs = z.infer<typeof addEmojiSchema>;

export const createIssueArgsSchema = z.object({
  id: z.string(),
  title: z.string(),
  description: z.optional(z.string()),
  created: z.number(),
  modified: z.number(),
  projectID: z.optional(z.string()),
});

export type CreateIssueArgs = z.infer<typeof createIssueArgsSchema>;

export const updateIssueArgsSchema = z.object({
  id: z.string(),
  title: z.optional(z.string()),
  description: z.optional(z.string()),
  open: z.optional(z.boolean()),
  assigneeID: z.optional(z.nullable(z.string())),
  visibility: z.optional(z.enum(['internal', 'public'])),
  modified: z.number(),
});

export type UpdateIssueArgs = z.infer<typeof updateIssueArgsSchema>;

const addCommentArgsSchema = z.object({
  id: z.string(),
  issueID: z.string(),
  body: z.string(),
  created: z.number(),
});

export type AddCommentArgs = z.infer<typeof addCommentArgsSchema>;

const notificationTypeSchema = z.enum(['subscribe', 'unsubscribe']);

export type NotificationType = z.infer<typeof notificationTypeSchema>;

const notificationUpdateSchema = z.object({
  issueID: z.string(),
  subscribed: notificationTypeSchema,
  created: z.number(),
});

export const mutators = defineMutators({
  issue: {
    create: defineMutator(
      createIssueArgsSchema,
      async ({tx, args, ctx: authData}) => {
        const {id, title, description, created, modified, projectID} = args;
        assertIsLoggedIn(authData);
        const creatorID = authData.sub;
        await tx.mutate.issue.insert({
          id,
          projectID: projectIDWithDefault(projectID),
          title,
          description: description ?? '',
          created,
          creatorID,
          modified,
          open: true,
          visibility: 'public',
        });

        // subscribe to notifications if the user creates the issue
        await updateIssueNotification(tx, {
          userID: creatorID,
          issueID: id,
          subscribed: 'subscribe',
          created,
        });
      },
    ),
    update: defineMutator(
      updateIssueArgsSchema,
      async ({tx, args: change, ctx: authData}) => {
        // Security: Auth check MUST come before existence check to prevent
        // information disclosure about private issue existence
        await assertIsCreatorOrAdmin(tx, authData, builder.issue, change.id);

        // Safe to read now since user is authorized
        const oldIssue = await tx.run(
          builder.issue.where('id', change.id).one(),
        );

        // For admins, assertIsCreatorOrAdmin doesn't verify existence
        // Use generic error to avoid leaking existence info
        if (!oldIssue) {
          throw new MutationError(
            `Issue not found or not authorized`,
            MutationErrorCode.NOT_AUTHORIZED,
            change.id,
          );
        }

        await tx.mutate.issue.update(change);

        const isAssigneeChange =
          change.assigneeID !== undefined &&
          change.assigneeID !== oldIssue.assigneeID;
        const previousAssigneeID = isAssigneeChange
          ? oldIssue.assigneeID
          : undefined;

        // subscribe to notifications if the user is assigned to the issue
        if (change.assigneeID) {
          await updateIssueNotification(tx, {
            userID: change.assigneeID,
            issueID: change.id,
            subscribed: 'subscribe',
            created: change.modified,
          });
        }

        // unsubscribe from notifications if the user is no longer assigned to the issue
        if (previousAssigneeID) {
          await updateIssueNotification(tx, {
            userID: previousAssigneeID,
            issueID: change.id,
            subscribed: 'unsubscribe',
            created: change.modified,
          });
        }
      },
    ),

    delete: defineMutator(z.string(), async ({tx, args: id, ctx: authData}) => {
      await assertIsCreatorOrAdmin(tx, authData, builder.issue, id);
      await tx.mutate.issue.delete({id});
    }),

    addLabel: defineMutator(
      z.object({
        issueID: z.string(),
        labelID: z.string(),
        projectID: z.optional(z.string()),
      }),
      async ({tx, args: {issueID, labelID, projectID}, ctx: authData}) => {
        await assertIsCreatorOrAdmin(tx, authData, builder.issue, issueID);
        await tx.mutate.issueLabel.insert({
          issueID,
          labelID,
          projectID: projectIDWithDefault(projectID),
        });
      },
    ),

    removeLabel: defineMutator(
      z.object({
        issueID: z.string(),
        labelID: z.string(),
      }),
      async ({tx, args: {issueID, labelID}, ctx: authData}) => {
        await assertIsCreatorOrAdmin(tx, authData, builder.issue, issueID);
        await tx.mutate.issueLabel.delete({issueID, labelID});
      },
    ),
  },

  notification: {
    update: defineMutator(
      notificationUpdateSchema,
      async ({tx, args: {issueID, subscribed, created}, ctx: authData}) => {
        assertIsLoggedIn(authData);
        const userID = authData.sub;
        await updateIssueNotification(tx, {
          userID,
          issueID,
          subscribed,
          created,
          forceUpdate: true,
        });
      },
    ),
  },

  emoji: {
    addToIssue: defineMutator(
      addEmojiSchema,
      async ({tx, args, ctx: authData}) => {
        await addEmoji(tx, 'issue', args, authData);
      },
    ),

    addToComment: defineMutator(
      addEmojiSchema,
      async ({tx, args, ctx: authData}) => {
        await addEmoji(tx, 'comment', args, authData);
      },
    ),

    remove: defineMutator(z.string(), async ({tx, args: id, ctx: authData}) => {
      await assertIsCreatorOrAdmin(tx, authData, builder.emoji, id);
      await tx.mutate.emoji.delete({id});
    }),
  },

  comment: {
    add: defineMutator(
      addCommentArgsSchema,
      async ({tx, args: {id, issueID, body, created}, ctx: authData}) => {
        assertIsLoggedIn(authData);
        const creatorID = authData.sub;

        await assertUserCanSeeIssue(tx, creatorID, issueID);

        await tx.mutate.comment.insert({id, issueID, creatorID, body, created});

        await updateIssueNotification(tx, {
          userID: creatorID,
          issueID,
          subscribed: 'subscribe',
          created,
        });
      },
    ),

    edit: defineMutator(
      z.object({
        id: z.string(),
        body: z.string(),
      }),
      async ({tx, args: {id, body}, ctx: authData}) => {
        await assertIsCreatorOrAdmin(tx, authData, builder.comment, id);
        await tx.mutate.comment.update({id, body});
      },
    ),

    remove: defineMutator(z.string(), async ({tx, args: id, ctx: authData}) => {
      await assertIsCreatorOrAdmin(tx, authData, builder.comment, id);
      await tx.mutate.comment.delete({id});
    }),
  },

  label: {
    create: defineMutator(
      z.object({
        id: z.string(),
        name: z.string(),
        projectID: z.optional(z.string()),
      }),
      async ({tx, args: {id, name, projectID}, ctx: authData}) => {
        if (!isAdmin(authData)) {
          throw new MutationError(
            `Only admins can create labels`,
            MutationErrorCode.NOT_AUTHORIZED,
            id,
          );
        }

        await tx.mutate.label.insert({
          id,
          name,
          projectID: projectIDWithDefault(projectID),
        });
      },
    ),

    createAndAddToIssue: defineMutator(
      z.object({
        issueID: z.string(),
        labelID: z.string(),
        labelName: z.string(),
        projectID: z.optional(z.string()),
      }),
      async ({
        tx,
        args: {issueID, labelID, labelName, projectID},
        ctx: authData,
      }) => {
        if (!isAdmin(authData)) {
          throw new MutationError(
            `Only admins can create labels`,
            MutationErrorCode.NOT_AUTHORIZED,
            labelID,
          );
        }

        const finalProjectID = projectIDWithDefault(projectID);
        await tx.mutate.label.insert({
          id: labelID,
          name: labelName,
          projectID: finalProjectID,
        });
        await tx.mutate.issueLabel.insert({
          issueID,
          labelID,
          projectID: finalProjectID,
        });
      },
    ),
  },

  viewState: {
    set: defineMutator(
      z.object({
        issueID: z.string(),
        viewed: z.number(),
      }),
      async ({tx, args: {issueID, viewed}, ctx: authData}) => {
        assertIsLoggedIn(authData);
        const userID = authData.sub;
        await tx.mutate.viewState.upsert({issueID, userID, viewed});
      },
    ),
  },

  userPref: {
    set: defineMutator(
      z.object({
        key: z.string(),
        value: z.string(),
      }),
      async ({tx, args: {key, value}, ctx: authData}) => {
        assertIsLoggedIn(authData);
        const userID = authData.sub;
        await tx.mutate.userPref.upsert({key, value, userID});
      },
    ),
  },
});

async function addEmoji(
  tx: Transaction<typeof schema>,
  subjectType: 'issue' | 'comment',
  {id, unicode, annotation, subjectID, created}: AddEmojiArgs,
  authData: AuthData | undefined,
) {
  assertIsLoggedIn(authData);
  const creatorID = authData.sub;

  if (subjectType === 'issue') {
    await assertUserCanSeeIssue(tx, creatorID, subjectID);
  } else {
    await assertUserCanSeeComment(tx, creatorID, subjectID);
  }

  await tx.mutate.emoji.insert({
    id,
    value: unicode,
    annotation,
    subjectID,
    creatorID,
    created,
  });

  // subscribe to notifications if the user emojis the issue itself
  if (subjectType === 'issue') {
    await updateIssueNotification(tx, {
      userID: creatorID,
      issueID: subjectID,
      subscribed: 'subscribe',
      created,
    });
  }
}

async function updateIssueNotification(
  tx: Transaction<typeof schema>,
  {
    userID,
    issueID,
    subscribed,
    created,
    forceUpdate = false,
  }: {
    userID: string;
    issueID: string;
    subscribed: NotificationType;
    created: number;
    forceUpdate?: boolean;
  },
) {
  await assertUserCanSeeIssue(tx, userID, issueID);

  const existingNotification = builder.issueNotifications
    .where('userID', userID)
    .where('issueID', issueID)
    .one();

  // if the user is subscribing to the issue, and they don't already have a preference
  // or the forceUpdate flag is set, we upsert the notification.
  if (subscribed === 'subscribe' && (!existingNotification || forceUpdate)) {
    await tx.mutate.issueNotifications.upsert({
      userID,
      issueID,
      subscribed: true,
      created,
    });
  } else if (subscribed === 'unsubscribe') {
    await tx.mutate.issueNotifications.upsert({
      userID,
      issueID,
      subscribed: false,
      created,
    });
  }
}
