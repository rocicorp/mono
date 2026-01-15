import {
  defineMutator,
  defineMutators,
  type ServerTransaction,
  type Transaction,
} from '@rocicorp/zero';
import {assert} from 'shared/src/asserts.js';
import {z} from 'zod/mini';
import {MutationError, MutationErrorCode} from '../shared/error.ts';
import {
  createIssueArgsSchema,
  mutators,
  updateIssueArgsSchema,
} from '../shared/mutators.ts';
import {builder} from '../shared/schema.ts';
import {notify} from './notify.ts';

export type PostCommitTask = () => Promise<void>;

function asServerTransaction(tx: Transaction): ServerTransaction {
  assert(tx.location === 'server', 'Transaction is not a server transaction');
  return tx;
}

export function createServerMutators(postCommitTasks: PostCommitTask[]) {
  return defineMutators(mutators, {
    issue: {
      create: defineMutator(
        createIssueArgsSchema,
        async ({
          tx,
          args: {id, projectID, title, description},
          ctx: authData,
        }) => {
          await mutators.issue.create.fn({
            tx,
            args: {
              id,
              projectID,
              title,
              description,
              created: Date.now(),
              modified: Date.now(),
            },
            ctx: authData,
          });

          await notify(
            asServerTransaction(tx),
            authData,
            {kind: 'create-issue', issueID: id},
            postCommitTasks,
          );
        },
      ),

      update: defineMutator(
        updateIssueArgsSchema,
        async ({tx, args, ctx: authData}) => {
          await mutators.issue.update.fn({
            tx,
            args: {
              ...args,
              modified: Date.now(),
            },
            ctx: authData,
          });

          await notify(
            asServerTransaction(tx),
            authData,
            {
              kind: 'update-issue',
              issueID: args.id,
              update: args,
            },
            postCommitTasks,
          );
        },
      ),

      addLabel: defineMutator(
        z.object({
          issueID: z.string(),
          labelID: z.string(),
          projectID: z.optional(z.string()),
        }),
        async ({tx, args: {issueID, labelID, projectID}, ctx: authData}) => {
          await mutators.issue.addLabel.fn({
            tx,
            args: {issueID, labelID, projectID},
            ctx: authData,
          });

          await notify(
            asServerTransaction(tx),
            authData,
            {
              kind: 'update-issue',
              issueID,
              update: {id: issueID},
            },
            postCommitTasks,
          );
        },
      ),

      removeLabel: defineMutator(
        z.object({
          issueID: z.string(),
          labelID: z.string(),
        }),
        async ({tx, args: {issueID, labelID}, ctx: authData}) => {
          await mutators.issue.removeLabel.fn({
            tx,
            args: {issueID, labelID},
            ctx: authData,
          });

          await notify(
            asServerTransaction(tx),
            authData,
            {
              kind: 'update-issue',
              issueID,
              update: {id: issueID},
            },
            postCommitTasks,
          );
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
        }),
        async ({tx, args, ctx: authData}) => {
          await mutators.emoji.addToIssue.fn({
            tx,
            args: {
              ...args,
              created: Date.now(),
            },
            ctx: authData,
          });

          await notify(
            asServerTransaction(tx),
            authData,
            {
              kind: 'add-emoji-to-issue',
              issueID: args.subjectID,
              emoji: args.unicode,
            },
            postCommitTasks,
          );
        },
      ),

      addToComment: defineMutator(
        z.object({
          id: z.string(),
          unicode: z.string(),
          annotation: z.string(),
          subjectID: z.string(),
        }),
        async ({tx, args, ctx: authData}) => {
          await mutators.emoji.addToComment.fn({
            tx,
            args: {
              ...args,
              created: Date.now(),
            },
            ctx: authData,
          });

          const comment = await tx.run(
            builder.comment.where('id', args.subjectID).one(),
          );

          if (!comment) {
            throw new MutationError(
              `Comment not found`,
              MutationErrorCode.NOTIFICATION_FAILED,
              args.subjectID,
            );
          }

          await notify(
            asServerTransaction(tx),
            authData,
            {
              kind: 'add-emoji-to-comment',
              issueID: comment.issueID,
              commentID: args.subjectID,
              emoji: args.unicode,
            },
            postCommitTasks,
          );
        },
      ),
    },

    comment: {
      add: defineMutator(
        z.object({
          id: z.string(),
          issueID: z.string(),
          body: z.string(),
        }),
        async ({tx, args: {id, issueID, body}, ctx: authData}) => {
          await mutators.comment.add.fn({
            tx,
            args: {
              id,
              issueID,
              body,
              created: Date.now(),
            },
            ctx: authData,
          });

          await notify(
            asServerTransaction(tx),
            authData,
            {
              kind: 'add-comment',
              issueID,
              commentID: id,
              comment: body,
            },
            postCommitTasks,
          );
        },
      ),

      edit: defineMutator(
        z.object({
          id: z.string(),
          body: z.string(),
        }),
        async ({tx, args: {id, body}, ctx: authData}) => {
          await mutators.comment.edit.fn({
            tx,
            args: {id, body},
            ctx: authData,
          });

          const comment = await tx.run(builder.comment.where('id', id).one());

          if (!comment) {
            throw new MutationError(
              `Comment not found`,
              MutationErrorCode.NOTIFICATION_FAILED,
              id,
            );
          }

          await notify(
            asServerTransaction(tx),
            authData,
            {
              kind: 'edit-comment',
              issueID: comment.issueID,
              commentID: id,
              comment: body,
            },
            postCommitTasks,
          );
        },
      ),
    },
  });
}
