import {schema, type Schema} from './schema.ts';
import {assert} from '../../../packages/shared/src/asserts.ts';
import {type UpdateValue, type Transaction, mutator} from '@rocicorp/zero';
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
  created: number;
};

export type CreateIssueArgs = {
  id: string;
  title: string;
  description?: string | undefined;
  created: number;
  modified: number;
};

export type AddCommentArgs = {
  id: string;
  issueID: string;
  body: string;
  created: number;
};

export const createIssue = (authData: AuthData | undefined) =>
  mutator(
    'createIssue',
    async (
      tx: Transaction<Schema>,
      {id, title, description, created, modified}: CreateIssueArgs,
    ) => {
      assertIsLoggedIn(authData);
      const creatorID = authData.sub;
      await tx.mutate.issue.insert({
        id,
        title,
        description: description ?? '',
        created,
        creatorID,
        modified,
        open: true,
        visibility: 'public',
      });
    },
  );

export const updateIssue = (authData: AuthData | undefined) =>
  mutator(
    'updateIssue',
    async (
      tx: Transaction<Schema>,
      change: UpdateValue<typeof schema.tables.issue> & {modified: number},
    ) => {
      await assertIsCreatorOrAdmin(authData, tx.query.issue, change.id);
      await tx.mutate.issue.update(change);
    },
  );

export const deleteIssue = (authData: AuthData | undefined) =>
  mutator('deleteIssue', async (tx: Transaction<Schema>, id: string) => {
    await assertIsCreatorOrAdmin(authData, tx.query.issue, id);
    await tx.mutate.issue.delete({id});
  });

export const addLabelToIssue = (authData: AuthData | undefined) =>
  mutator(
    'addLabelToIssue',
    async (
      tx: Transaction<Schema>,
      {issueID, labelID}: {issueID: string; labelID: string},
    ) => {
      await assertIsCreatorOrAdmin(authData, tx.query.issue, issueID);
      await tx.mutate.issueLabel.insert({issueID, labelID});
    },
  );

export const removeLabelFromIssue = (authData: AuthData | undefined) =>
  mutator(
    'removeLabelFromIssue',
    async (
      tx: Transaction<Schema>,
      {issueID, labelID}: {issueID: string; labelID: string},
    ) => {
      await assertIsCreatorOrAdmin(authData, tx.query.issue, issueID);
      await tx.mutate.issueLabel.delete({issueID, labelID});
    },
  );

export const addEmojiToIssue = (authData: AuthData | undefined) =>
  mutator(
    'addEmojiToIssue',
    async (tx: Transaction<Schema>, args: AddEmojiArgs) => {
      await addEmoji(authData, tx, 'issue', args);
    },
  );

export const addEmojiToComment = (authData: AuthData | undefined) =>
  mutator(
    'addEmojiToComment',
    async (tx: Transaction<Schema>, args: AddEmojiArgs) => {
      await addEmoji(authData, tx, 'comment', args);
    },
  );

export const removeEmoji = (authData: AuthData | undefined) =>
  mutator('removeEmoji', async (tx: Transaction<Schema>, id: string) => {
    await assertIsCreatorOrAdmin(authData, tx.query.emoji, id);
    await tx.mutate.emoji.delete({id});
  });

export const addComment = (authData: AuthData | undefined) =>
  mutator(
    'addComment',
    async (
      tx: Transaction<Schema>,
      {id, issueID, body, created}: AddCommentArgs,
    ) => {
      assertIsLoggedIn(authData);
      const creatorID = authData.sub;

      await assertUserCanSeeIssue(tx, authData, issueID);

      await tx.mutate.comment.insert({id, issueID, creatorID, body, created});
    },
  );

export const editComment = (authData: AuthData | undefined) =>
  mutator(
    'editComment',
    async (tx: Transaction<Schema>, {id, body}: {id: string; body: string}) => {
      await assertIsCreatorOrAdmin(authData, tx.query.comment, id);
      await tx.mutate.comment.update({id, body});
    },
  );

export const removeComment = (authData: AuthData | undefined) =>
  mutator('removeComment', async (tx: Transaction<Schema>, id: string) => {
    await assertIsCreatorOrAdmin(authData, tx.query.comment, id);
    await tx.mutate.comment.delete({id});
  });

export const createLabel = (authData: AuthData | undefined) =>
  mutator(
    'createLabel',
    async (tx: Transaction<Schema>, {id, name}: {id: string; name: string}) => {
      assert(isAdmin(authData), 'Only admins can create labels');
      await tx.mutate.label.insert({id, name});
    },
  );

export const createLabelAndAddToIssue = (authData: AuthData | undefined) =>
  mutator(
    'createLabelAndAddToIssue',
    async (
      tx: Transaction<Schema>,
      {
        issueID,
        labelID,
        labelName,
      }: {labelID: string; issueID: string; labelName: string},
    ) => {
      assert(isAdmin(authData), 'Only admins can create labels');
      await tx.mutate.label.insert({id: labelID, name: labelName});
      await tx.mutate.issueLabel.insert({issueID, labelID});
    },
  );

export const setViewState = (authData: AuthData | undefined) =>
  mutator(
    'setViewState',
    async (
      tx: Transaction<Schema>,
      {issueID, viewed}: {issueID: string; viewed: number},
    ) => {
      assertIsLoggedIn(authData);
      const userID = authData.sub;
      await tx.mutate.viewState.upsert({issueID, userID, viewed});
    },
  );

export const setUserPref = (authData: AuthData | undefined) =>
  mutator(
    'setUserPref',
    async (
      tx: Transaction<Schema>,
      {key, value}: {key: string; value: string},
    ) => {
      assertIsLoggedIn(authData);
      const userID = authData.sub;
      await tx.mutate.userPref.upsert({key, value, userID});
    },
  );

async function addEmoji(
  authData: AuthData | undefined,
  tx: Transaction<typeof schema, unknown>,
  subjectType: 'issue' | 'comment',
  {id, unicode, annotation, subjectID, created}: AddEmojiArgs,
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
    created,
  });
}
