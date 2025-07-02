import {
  type CreateIssueArgs,
  type AddEmojiArgs,
  type AddCommentArgs,
} from '../shared/mutators.ts';
import {type Transaction, type UpdateValue} from '@rocicorp/zero';
import {schema, type Schema} from '../shared/schema.ts';
import {notify} from './notify.ts';
import {assert} from '../../../packages/shared/src/asserts.ts';
import {type AuthData} from '../shared/auth.ts';
import * as sharedMutators from '../shared/mutators.ts';

export type PostCommitTask = () => Promise<void>;

export async function createIssue(
  authData: AuthData | undefined,
  postCommitTasks: PostCommitTask[],
  tx: Transaction<Schema>,
  {id, title, description}: CreateIssueArgs,
) {
  await sharedMutators.createIssue(authData)(tx, {
    id,
    title,
    description,
    created: Date.now(),
    modified: Date.now(),
  });
  await notify(
    tx,
    authData,
    {kind: 'create-issue', issueID: id},
    postCommitTasks,
  );
}

export async function updateIssue(
  authData: AuthData | undefined,
  postCommitTasks: PostCommitTask[],
  tx: Transaction<Schema>,
  args: {id: string} & UpdateValue<typeof schema.tables.issue>,
) {
  const oldIssue = await tx.query.issue.where('id', args.id).one();
  assert(oldIssue);

  const isAssigneeChange =
    args.assigneeID !== undefined && args.assigneeID !== oldIssue.assigneeID;
  const previousAssigneeID = isAssigneeChange ? oldIssue.assigneeID : undefined;

  await sharedMutators.updateIssue(authData)(tx, {
    ...args,
    modified: Date.now(),
  });

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
}

export async function addLabelToIssue(
  authData: AuthData | undefined,
  postCommitTasks: PostCommitTask[],
  tx: Transaction<Schema>,
  {issueID, labelID}: {issueID: string; labelID: string},
) {
  await sharedMutators.addLabelToIssue(authData)(tx, {issueID, labelID});
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
}

export async function removeLabelFromIssue(
  authData: AuthData | undefined,
  postCommitTasks: PostCommitTask[],
  tx: Transaction<Schema>,
  {issueID, labelID}: {issueID: string; labelID: string},
) {
  await sharedMutators.removeLabelFromIssue(authData)(tx, {issueID, labelID});
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
}

export async function addEmojiToIssue(
  authData: AuthData | undefined,
  postCommitTasks: PostCommitTask[],
  tx: Transaction<Schema>,
  args: AddEmojiArgs,
) {
  await sharedMutators.addEmojiToIssue(authData)(tx, {
    ...args,
    created: Date.now(),
  });
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
}

export async function addEmojiToComment(
  authData: AuthData | undefined,
  postCommitTasks: PostCommitTask[],
  tx: Transaction<Schema>,
  args: AddEmojiArgs,
) {
  await sharedMutators.addEmojiToComment(authData)(tx, {
    ...args,
    created: Date.now(),
  });

  const comment = await tx.query.comment.where('id', args.subjectID).one();
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
}

export async function addComment(
  authData: AuthData | undefined,
  postCommitTasks: PostCommitTask[],
  tx: Transaction<Schema>,
  {id, issueID, body}: AddCommentArgs,
) {
  await sharedMutators.addComment(authData)(tx, {
    id,
    issueID,
    body,
    created: Date.now(),
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
}

export async function editComment(
  authData: AuthData | undefined,
  postCommitTasks: PostCommitTask[],
  tx: Transaction<Schema>,
  {id, body}: {id: string; body: string},
) {
  await sharedMutators.editComment(authData)(tx, {id, body});

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
}
