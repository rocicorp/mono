import {defineMutator, defineMutators} from '@rocicorp/zero';
import {type AuthData} from '../shared/auth.ts';
import {MutationError, MutationErrorCode} from '../shared/error.ts';
import {mutators} from '../shared/mutators.ts';
import type {Schema} from '../shared/schema.ts';
import {builder} from '../shared/schema.ts';
import {notify} from './notify.ts';
import {z} from 'zod';

export type PostCommitTask = () => Promise<void>;

export function createServerMutators(postCommitTasks: PostCommitTask[]) {
  return defineMutators<Schema, AuthData>()({
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
        async (tx, {args, ctx}) => {
          await mutators.issue.create(args)(tx, ctx);

          await notify(
            // TODO: figure out why this cast necessary
            tx as any,
            ctx,
            {kind: 'create-issue', issueID: args.id},
            postCommitTasks,
          );
        },
      ),

      addLabel: defineMutator(
        z.object({
          issueID: z.string(),
          labelID: z.string(),
        }),
        async (tx, {args, ctx}) => {
          await mutators.issue.addLabel(args)(tx, ctx);
          await notify(
            tx as any,
            ctx,
            {
              kind: 'update-issue',
              issueID: args.issueID,
              update: {id: args.issueID},
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
        async (tx, {args, ctx}) => {
          await mutators.issue.removeLabel(args)(tx, ctx);
          await notify(
            tx as any,
            ctx,
            {
              kind: 'update-issue',
              issueID: args.issueID,
              update: {id: args.issueID},
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
          creatorID: z.string(),
          created: z.number(),
        }),
        async (tx, {args, ctx}) => {
          await mutators.emoji.addToIssue(args)(tx, ctx);
          await notify(
            tx as any,
            ctx,
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
          creatorID: z.string(),
          created: z.number(),
        }),
        async (tx, {args, ctx}) => {
          await mutators.emoji.addToComment(args)(tx, ctx);

          await notify(
            tx as any,
            ctx,
            {
              kind: 'add-emoji-to-comment',
              issueID: args.subjectID,
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
          created: z.number(),
        }),
        async (tx, {args, ctx}) => {
          await mutators.comment.add(args)(tx, ctx);

          await notify(
            tx as any,
            ctx,
            {
              kind: 'add-comment',
              issueID: args.issueID,
              commentID: args.id,
              comment: args.body,
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
        async (tx, {args, ctx}) => {
          const comment = await tx.run(
            builder.comment.where('id', args.id).one(),
          );

          if (!comment) {
            throw new MutationError(
              `Comment not found`,
              MutationErrorCode.ENTITY_NOT_FOUND,
              args.id,
            );
          }

          await mutators.comment.edit(args)(tx, ctx);

          await notify(
            tx as any,
            ctx,
            {
              kind: 'edit-comment',
              issueID: comment.issueID,
              commentID: args.id,
              comment: args.body,
            },
            postCommitTasks,
          );
        },
      ),
    },
  });
}
