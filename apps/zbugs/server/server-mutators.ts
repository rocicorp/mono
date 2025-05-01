import {
  createMutators,
  type CreateIssueArgs,
  type AddEmojiArgs,
  type AddCommentArgs,
} from '../shared/mutators.ts';
import type {UpdateValue} from '@rocicorp/zero';
import type {schema} from '../shared/schema.ts';
import {notify} from './notify.ts';
import {assert} from '../../../packages/shared/src/asserts.ts';
import {type AuthData} from '../shared/auth.ts';
import type {
  CustomMutatorDefs,
  ServerTransaction,
  PostgresJSTransaction,
} from '@rocicorp/zero/pg';

export function createServerMutators(authData: AuthData | undefined) {
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
          created: Date.now(),
          modified: Date.now(),
        });
        await notify(tx, authData, {kind: 'create-issue', issueID: id});
      },

      async update(tx, update: UpdateValue<typeof schema.tables.issue>) {
        await mutators.issue.update(tx, {
          ...update,
          modified: Date.now(),
        });
        await notify(tx, authData, {
          kind: 'update-issue',
          issueID: update.id,
          update,
        });
      },

      async addLabel(
        tx,
        {issueID, labelID}: {issueID: string; labelID: string},
      ) {
        await mutators.issue.addLabel(tx, {issueID, labelID});
        await notify(tx, authData, {
          kind: 'update-issue',
          issueID,
          update: {id: issueID},
        });
      },

      async removeLabel(
        tx,
        {issueID, labelID}: {issueID: string; labelID: string},
      ) {
        await mutators.issue.removeLabel(tx, {issueID, labelID});
        await notify(tx, authData, {
          kind: 'update-issue',
          issueID,
          update: {id: issueID},
        });
      },
    },

    emoji: {
      ...mutators.emoji,

      async addToIssue(tx, args: AddEmojiArgs) {
        await mutators.emoji.addToIssue(tx, {
          ...args,
          created: Date.now(),
        });
        await notify(tx, authData, {
          kind: 'add-emoji-to-issue',
          issueID: args.subjectID,
          emoji: args.unicode,
        });
      },

      async addToComment(tx, args: AddEmojiArgs) {
        await mutators.emoji.addToComment(tx, {
          ...args,
          created: Date.now(),
        });

        const comment = await tx.query.comment
          .where('id', args.subjectID)
          .one();
        assert(comment);
        await notify(tx, authData, {
          kind: 'add-emoji-to-comment',
          issueID: comment.issueID,
          commentID: args.subjectID,
          emoji: args.unicode,
        });
      },
    },

    comment: {
      ...mutators.comment,

      async add(tx, {id, issueID, body}: AddCommentArgs) {
        await mutators.comment.add(tx, {
          id,
          issueID,
          body,
          created: Date.now(),
        });
        await notify(tx, authData, {
          kind: 'add-comment',
          issueID,
          commentID: id,
          comment: body,
        });
      },

      async edit(tx, {id, body}: {id: string; body: string}) {
        await mutators.comment.edit(tx, {id, body});

        const comment = await tx.query.comment.where('id', id).one();
        assert(comment);

        await notify(tx, authData, {
          kind: 'edit-comment',
          issueID: comment.issueID,
          commentID: id,
          comment: body,
        });
      },
    },
  } as const satisfies CustomMutatorDefs<
    ServerTransaction<typeof schema, PostgresJSTransaction>
  >;
}
