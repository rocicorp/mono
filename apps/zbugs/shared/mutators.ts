import {
  defineMutators,
  defineMutatorWithContextType,
  type Transaction,
  type UpdateValue,
} from '@rocicorp/zero';
import * as z from 'zod/mini';
import {
  assertIsCreatorOrAdmin,
  assertIsLoggedIn,
  assertUserCanSeeComment,
  assertUserCanSeeIssue,
  isAdmin,
  type AuthData,
} from './auth.ts';
import {MutationError, MutationErrorCode} from './error.ts';
import {builder, ZERO_PROJECT_ID, type Schema} from './schema.ts';

function projectIDWithDefault(projectID: string | undefined): string {
  return projectID ?? ZERO_PROJECT_ID;
}

export type NotificationType = 'subscribe' | 'unsubscribe';

export type MutatorTx = Transaction<Schema, AuthData | undefined>;

const defineMutator = defineMutatorWithContextType<AuthData | undefined>();

// Helper functions that need tx and authData
// Use MutatorTx and cast tx in the mutator callbacks
async function addEmoji(
  tx: MutatorTx,
  authData: AuthData | undefined,
  subjectType: 'issue' | 'comment',
  {
    id,
    unicode,
    annotation,
    subjectID,
    created,
  }: {
    id: string;
    unicode: string;
    annotation: string;
    subjectID: string;
    created: number;
  },
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
    await updateIssueNotification(tx, authData, {
      userID: creatorID,
      issueID: subjectID,
      subscribed: 'subscribe',
      created,
    });
  }
}

async function updateIssueNotification(
  tx: MutatorTx,
  _authData: AuthData | undefined,
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

  const existingNotification = await tx.run(
    builder.issueNotifications
      .where('userID', userID)
      .where('issueID', issueID)
      .one(),
  );

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

export const mutators = defineMutators<Schema, AuthData | undefined>()({
  issue: {
    create: defineMutator(
      z.object({
        id: z.string(),
        title: z.string(),
        description: z.optional(z.string()),
        created: z.number(),
        modified: z.number(),
        projectID: z.optional(z.string()),
      }),
      async (tx, {args, ctx: authData}) => {
        assertIsLoggedIn(authData);
        const creatorID = authData.sub;
        await tx.mutate.issue.insert({
          id: args.id,
          projectID: projectIDWithDefault(args.projectID),
          title: args.title,
          description: args.description ?? '',
          created: args.created,
          creatorID,
          modified: args.modified,
          open: true,
          visibility: 'public',
        });

        // subscribe to notifications if the user creates the issue
        await updateIssueNotification(tx as unknown as MutatorTx, authData, {
          userID: creatorID,
          issueID: args.id,
          subscribed: 'subscribe',
          created: args.created,
        });
      },
    ),

    update: defineMutator(
      z.object({
        id: z.string(),
        title: z.optional(z.string()),
        description: z.optional(z.string()),
        open: z.optional(z.boolean()),
        modified: z.number(),
        assigneeID: z.optional(z.nullable(z.string())),
        visibility: z.optional(z.enum(['public', 'internal'])),
      }),
      async (tx, {args: change, ctx: authData}) => {
        const oldIssue = await tx.run(
          builder.issue.where('id', change.id).one(),
        );

        if (!oldIssue) {
          throw new MutationError(
            `Issue not found`,
            MutationErrorCode.ENTITY_NOT_FOUND,
            change.id,
          );
        }

        await assertIsCreatorOrAdmin(
          tx as unknown as MutatorTx,
          authData,
          builder.issue,
          change.id,
        );
        await tx.mutate.issue.update(
          change as UpdateValue<Schema['tables']['issue']>,
        );

        const isAssigneeChange =
          change.assigneeID !== undefined &&
          change.assigneeID !== oldIssue.assigneeID;
        const previousAssigneeID = isAssigneeChange
          ? oldIssue.assigneeID
          : undefined;

        // subscribe to notifications if the user is assigned to the issue
        if (change.assigneeID) {
          await updateIssueNotification(tx as unknown as MutatorTx, authData, {
            userID: change.assigneeID,
            issueID: change.id,
            subscribed: 'subscribe',
            created: change.modified,
          });
        }

        // unsubscribe from notifications if the user is no longer assigned to the issue
        if (previousAssigneeID) {
          await updateIssueNotification(tx as unknown as MutatorTx, authData, {
            userID: previousAssigneeID,
            issueID: change.id,
            subscribed: 'unsubscribe',
            created: change.modified,
          });
        }
      },
    ),

    delete: defineMutator(z.string(), async (tx, {args: id, ctx: authData}) => {
      await assertIsCreatorOrAdmin(
        tx as unknown as MutatorTx,
        authData,
        builder.issue,
        id,
      );
      await tx.mutate.issue.delete({id});
    }),

    addLabel: defineMutator(
      z.object({
        issueID: z.string(),
        labelID: z.string(),
        projectID: z.optional(z.string()),
      }),
      async (tx, {args, ctx: authData}) => {
        await assertIsCreatorOrAdmin(
          tx as unknown as MutatorTx,
          authData,
          builder.issue,
          args.issueID,
        );
        await tx.mutate.issueLabel.insert({
          issueID: args.issueID,
          labelID: args.labelID,
          projectID: projectIDWithDefault(args.projectID),
        });
      },
    ),

    removeLabel: defineMutator(
      z.object({
        issueID: z.string(),
        labelID: z.string(),
      }),
      async (tx, {args, ctx: authData}) => {
        await assertIsCreatorOrAdmin(
          tx as unknown as MutatorTx,
          authData,
          builder.issue,
          args.issueID,
        );
        await tx.mutate.issueLabel.delete({
          issueID: args.issueID,
          labelID: args.labelID,
        });
      },
    ),
  },

  notification: {
    update: defineMutator(
      z.object({
        issueID: z.string(),
        subscribed: z.enum(['subscribe', 'unsubscribe']),
        created: z.number(),
      }),
      async (tx, {args, ctx: authData}) => {
        assertIsLoggedIn(authData);
        const userID = authData.sub;
        await updateIssueNotification(tx as unknown as MutatorTx, authData, {
          userID,
          issueID: args.issueID,
          subscribed: args.subscribed,
          created: args.created,
          forceUpdate: true,
        });
      },
    ),
  },

  emoji: {
    addToIssue: defineMutator(
      z.object({
        id: z.string(),
        unicode: z.string(),
        annotation: z.string(),
        subjectID: z.string(),
        created: z.number(),
      }),
      async (tx, {args, ctx: authData}) => {
        await addEmoji(tx as unknown as MutatorTx, authData, 'issue', args);
      },
    ),

    addToComment: defineMutator(
      z.object({
        id: z.string(),
        unicode: z.string(),
        annotation: z.string(),
        subjectID: z.string(),
        created: z.number(),
      }),
      async (tx, {args, ctx: authData}) => {
        await addEmoji(tx as unknown as MutatorTx, authData, 'comment', args);
      },
    ),

    remove: defineMutator(z.string(), async (tx, {args: id, ctx: authData}) => {
      await assertIsCreatorOrAdmin(
        tx as unknown as MutatorTx,
        authData,
        builder.emoji,
        id,
      );
      await tx.mutate.emoji.delete({id});
    }),
  },

  comment: {
    add: defineMutator(
      z.object({
        id: z.string(),
        issueID: z.string(),
        body: z.string(),
        created: z.number(),
      }),
      async (tx, {args, ctx: authData}) => {
        assertIsLoggedIn(authData);
        const creatorID = authData.sub;

        await assertUserCanSeeIssue(
          tx as unknown as MutatorTx,
          creatorID,
          args.issueID,
        );

        await tx.mutate.comment.insert({
          id: args.id,
          issueID: args.issueID,
          creatorID,
          body: args.body,
          created: args.created,
        });

        await updateIssueNotification(tx as unknown as MutatorTx, authData, {
          userID: creatorID,
          issueID: args.issueID,
          subscribed: 'subscribe',
          created: args.created,
        });
      },
    ),

    edit: defineMutator(
      z.object({
        id: z.string(),
        body: z.string(),
      }),
      async (tx, {args, ctx: authData}) => {
        await assertIsCreatorOrAdmin(
          tx as unknown as MutatorTx,
          authData,
          builder.comment,
          args.id,
        );
        await tx.mutate.comment.update({id: args.id, body: args.body});
      },
    ),

    remove: defineMutator(z.string(), async (tx, {args: id, ctx: authData}) => {
      await assertIsCreatorOrAdmin(
        tx as unknown as MutatorTx,
        authData,
        builder.comment,
        id,
      );
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
      async (tx, {args, ctx: authData}) => {
        if (!isAdmin(authData)) {
          throw new MutationError(
            `Only admins can create labels`,
            MutationErrorCode.NOT_AUTHORIZED,
            args.id,
          );
        }

        await tx.mutate.label.insert({
          id: args.id,
          name: args.name,
          projectID: projectIDWithDefault(args.projectID),
        });
      },
    ),

    createAndAddToIssue: defineMutator(
      z.object({
        labelID: z.string(),
        issueID: z.string(),
        labelName: z.string(),
        projectID: z.optional(z.string()),
      }),
      async (tx, {args, ctx: authData}) => {
        if (!isAdmin(authData)) {
          throw new MutationError(
            `Only admins can create labels`,
            MutationErrorCode.NOT_AUTHORIZED,
            args.labelID,
          );
        }

        const projectID = projectIDWithDefault(args.projectID);
        await tx.mutate.label.insert({
          id: args.labelID,
          name: args.labelName,
          projectID,
        });
        await tx.mutate.issueLabel.insert({
          issueID: args.issueID,
          labelID: args.labelID,
          projectID,
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
      async (tx, {args, ctx: authData}) => {
        assertIsLoggedIn(authData);
        const userID = authData.sub;
        await tx.mutate.viewState.upsert({
          issueID: args.issueID,
          userID,
          viewed: args.viewed,
        });
      },
    ),
  },

  userPref: {
    set: defineMutator(
      z.object({
        key: z.string(),
        value: z.string(),
      }),
      async (tx, {args, ctx: authData}) => {
        assertIsLoggedIn(authData);
        const userID = authData.sub;
        await tx.mutate.userPref.upsert({
          key: args.key,
          value: args.value,
          userID,
        });
      },
    ),
  },
});

export type Mutators = typeof mutators;
