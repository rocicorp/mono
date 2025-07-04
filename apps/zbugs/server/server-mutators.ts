import {
  createMutators,
  type CreateIssueArgs,
  type AddEmojiArgs,
  type AddCommentArgs,
} from '../shared/mutators.ts';
import {type CustomMutatorDefs, type UpdateValue} from '@rocicorp/zero';
import {schema} from '../shared/schema.ts';
import {notify} from './notify.ts';
import {assert} from '../../../packages/shared/src/asserts.ts';
import {type AuthData} from '../shared/auth.ts';

export type PostCommitTask = () => Promise<void>;

export function createServerMutators(
  authData: AuthData | undefined,
  postCommitTasks: PostCommitTask[],
) {
  const mutators = createMutators(authData);

  return {
    ...mutators,

    issue: {
      ...mutators.issue,

      async create(tx, {id, title, description}: CreateIssueArgs) {
        await mutators.issue.create(tx, {
          id,
          title,
          description,
        });
        await notify(
          tx,
          authData,
          {kind: 'create-issue', issueID: id},
          postCommitTasks,
        );
      },

      async update(
        tx,
        args: {id: string} & UpdateValue<typeof schema.tables.issue>,
      ) {
        const oldIssue = await tx.query.issue.where('id', args.id).one();
        assert(oldIssue);

        const isAssigneeChange =
          args.assigneeID !== undefined &&
          args.assigneeID !== oldIssue.assigneeID;
        const previousAssigneeID = isAssigneeChange
          ? oldIssue.assigneeID
          : undefined;

        await mutators.issue.update(tx, args);

        await notify(
          tx,
          authData,
          {
            kind: 'update-issue',
            issueID: args.id,
            update: args,
          },
          postCommitTasks,
          isAssigneeChange,
          previousAssigneeID,
        );
      },

      async addLabel(
        tx,
        {issueID, labelID}: {issueID: string; labelID: string},
      ) {
        await mutators.issue.addLabel(tx, {issueID, labelID});
        await notify(
          tx,
          authData,
          {
            kind: 'update-issue',
            issueID,
            update: {id: issueID},
          },
          postCommitTasks,
        );
      },

      async removeLabel(
        tx,
        {issueID, labelID}: {issueID: string; labelID: string},
      ) {
        await mutators.issue.removeLabel(tx, {issueID, labelID});
        await notify(
          tx,
          authData,
          {
            kind: 'update-issue',
            issueID,
            update: {id: issueID},
          },
          postCommitTasks,
        );
      },
    },

    emoji: {
      ...mutators.emoji,

      async addToIssue(tx, args: AddEmojiArgs) {
        await mutators.emoji.addToIssue(tx, args);
        await notify(
          tx,
          authData,
          {
            kind: 'add-emoji-to-issue',
            issueID: args.subjectID,
            emoji: args.unicode,
          },
          postCommitTasks,
        );
      },

      async addToComment(tx, args: AddEmojiArgs) {
        await mutators.emoji.addToComment(tx, args);

        const comment = await tx.query.comment
          .where('id', args.subjectID)
          .one();
        assert(comment);
        await notify(
          tx,
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
    },

    comment: {
      ...mutators.comment,

      async add(tx, {id, issueID, body}: AddCommentArgs) {
        await mutators.comment.add(tx, {
          id,
          issueID,
          body,
        });
        await notify(
          tx,
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

      async edit(tx, {id, body}: {id: string; body: string}) {
        await mutators.comment.edit(tx, {id, body});

        const comment = await tx.query.comment.where('id', id).one();
        assert(comment);

        await notify(
          tx,
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
    },
  } as const satisfies CustomMutatorDefs<typeof schema>;
}
