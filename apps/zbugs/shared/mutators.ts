import {schema} from './schema.ts';
import {assert} from '../../../packages/shared/src/asserts.ts';
import type {UpdateValue, Transaction, CustomMutatorDefs} from '@rocicorp/zero';
import {
  assertIsCreatorOrAdmin,
  assertUserCanSeeIssue,
  assertUserCanSeeComment,
  isAdmin,
  type AuthData,
  assertIsLoggedIn,
} from './auth.ts';

export type AddEmojiArgs = {
  id: string;
  unicode: string;
  annotation: string;
  subjectID: string;
};

export type CreateIssueArgs = {
  id: string;
  title: string;
  description?: string | undefined;
};

export type AddCommentArgs = {
  id: string;
  issueID: string;
  body: string;
};

export function createMutators(authData: AuthData | undefined) {
  return {
    issue: {
      async create(tx, {id, title, description}: CreateIssueArgs) {
        assertIsLoggedIn(authData);
        const creatorID = authData.sub;
        await tx.mutate.issue.insert({
          id,
          title,
          description: description ?? '',
          creatorID,
          open: true,
          visibility: 'public',
        });
      },

      async update(tx, change: UpdateValue<typeof schema.tables.issue>) {
        await assertIsCreatorOrAdmin(authData, tx.query.issue, change.id);
        await tx.mutate.issue.update(change);
      },

      async delete(tx, id: string) {
        await assertIsCreatorOrAdmin(authData, tx.query.issue, id);
        await tx.mutate.issue.delete({id});
      },

      async addLabel(
        tx,
        {issueID, labelID}: {issueID: string; labelID: string},
      ) {
        await assertIsCreatorOrAdmin(authData, tx.query.issue, issueID);
        await tx.mutate.issueLabel.insert({issueID, labelID});
      },

      async removeLabel(
        tx,
        {issueID, labelID}: {issueID: string; labelID: string},
      ) {
        await assertIsCreatorOrAdmin(authData, tx.query.issue, issueID);
        await tx.mutate.issueLabel.delete({issueID, labelID});
      },
    },

    emoji: {
      async addToIssue(tx, args: AddEmojiArgs) {
        await addEmoji(tx, 'issue', args);
      },

      async addToComment(tx, args: AddEmojiArgs) {
        await addEmoji(tx, 'comment', args);
      },

      async remove(tx, id: string) {
        await assertIsCreatorOrAdmin(authData, tx.query.emoji, id);
        await tx.mutate.emoji.delete({id});
      },
    },

    comment: {
      async add(tx, {id, issueID, body}: AddCommentArgs) {
        assertIsLoggedIn(authData);
        const creatorID = authData.sub;

        await assertUserCanSeeIssue(tx, authData, issueID);

        await tx.mutate.comment.insert({id, issueID, creatorID, body});
      },

      async edit(tx, {id, body}: {id: string; body: string}) {
        await assertIsCreatorOrAdmin(authData, tx.query.comment, id);
        await tx.mutate.comment.update({id, body});
      },

      async remove(tx, id: string) {
        await assertIsCreatorOrAdmin(authData, tx.query.comment, id);
        await tx.mutate.comment.delete({id});
      },
    },

    label: {
      async create(tx, {id, name}: {id: string; name: string}) {
        assert(isAdmin(authData), 'Only admins can create labels');
        await tx.mutate.label.insert({id, name});
      },

      async createAndAddToIssue(
        tx,
        {
          issueID,
          labelID,
          labelName,
        }: {labelID: string; issueID: string; labelName: string},
      ) {
        assert(isAdmin(authData), 'Only admins can create labels');
        await tx.mutate.label.insert({id: labelID, name: labelName});
        await tx.mutate.issueLabel.insert({issueID, labelID});
      },
    },

    viewState: {
      async set(tx, {issueID, viewed}: {issueID: string; viewed: number}) {
        assertIsLoggedIn(authData);
        const userID = authData.sub;
        await tx.mutate.viewState.upsert({issueID, userID, viewed});
      },
    },

    userPref: {
      async set(tx, {key, value}: {key: string; value: string}) {
        assertIsLoggedIn(authData);
        const userID = authData.sub;
        await tx.mutate.userPref.upsert({key, value, userID});
      },
    },
  } as const satisfies CustomMutatorDefs<typeof schema>;

  async function addEmoji(
    tx: Transaction<typeof schema, unknown>,
    subjectType: 'issue' | 'comment',
    {id, unicode, annotation, subjectID}: AddEmojiArgs,
  ) {
    assertIsLoggedIn(authData);
    const creatorID = authData.sub;

    if (subjectType === 'issue') {
      assertUserCanSeeIssue(tx, authData, subjectID);
    } else {
      assertUserCanSeeComment(tx, authData, subjectID);
    }

    await tx.mutate.emoji.insert({
      id,
      value: unicode,
      annotation,
      subjectID,
      creatorID,
    });
  }
}

export type Mutators = ReturnType<typeof createMutators>;
