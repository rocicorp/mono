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
import * as Y from 'yjs';

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

export function createMutators(authData: AuthData | undefined) {
  return {
    issue: {
      async create(
        tx,
        {id, title, description, created, modified}: CreateIssueArgs,
      ) {
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

      async update(
        tx,
        change: UpdateValue<typeof schema.tables.issue> & {modified: number},
      ) {
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
      async add(tx, {id, issueID, body, created}: AddCommentArgs) {
        assertIsLoggedIn(authData);
        const creatorID = authData.sub;

        await assertUserCanSeeIssue(tx, authData, issueID);

        await tx.mutate.comment.insert({id, issueID, creatorID, body, created});
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

    document: {
      async applyUpdate(tx, {documentId, update}: {documentId: string; update: string}) {
        assert(isAdmin(authData), 'Only admins can apply updates to documents');

        const doc = await tx.query.document.where('id', documentId).one();
        if (!doc) {
          throw new Error(`Document ${documentId} not found`);
        }

        const stateBuffer = base64ToUint8Array(doc.snapshot);
        const updateBuffer = base64ToUint8Array(update);
        const newStateBuffer = Y.mergeUpdates([stateBuffer, updateBuffer]);
        const newState = uint8ArrayToBase64(newStateBuffer);
        await tx.mutate.document.update({id: documentId, snapshot: newState});
      },
    },
  } as const satisfies CustomMutatorDefs<typeof schema>;

  async function addEmoji(
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

  function base64ToUint8Array(base64: string) {
    const binaryString = atob(base64);
    const length = binaryString.length;
    const uint8Array = new Uint8Array(length);
    for (let i = 0; i < length; i++) {
      uint8Array[i] = binaryString.charCodeAt(i);
    }
    return uint8Array;
  }

  function uint8ArrayToBase64(uint8Array: Uint8Array) {
    const binaryString = String.fromCharCode(...uint8Array);
    return btoa(binaryString);
  }
}

export type Mutators = ReturnType<typeof createMutators>;
