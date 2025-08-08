import {schema} from './schema.ts';
import {assert} from '../../../packages/shared/src/asserts.ts';
import type {UpdateValue, Transaction} from '@rocicorp/zero';
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

export type NotificationType = 'subscribe' | 'unsubscribe';
export type MutatorTx = Transaction<typeof schema>;

export function createMutators(authData: AuthData | undefined) {
  return {
    issue: {
      async create(
        tx: MutatorTx,
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

        // subscribe to notifications if the user creates the issue
        await updateIssueNotification(tx, {
          userID: creatorID,
          issueID: id,
          subscribed: 'subscribe',
          created,
        });
      },

      async update(
        tx: MutatorTx,
        change: UpdateValue<typeof schema.tables.issue> & {modified: number},
      ) {
        const oldIssue = await tx.query.issue.where('id', change.id).one();
        assert(oldIssue);

        await assertIsCreatorOrAdmin(authData, tx.query.issue, change.id);
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

      async delete(tx: MutatorTx, id: string) {
        await assertIsCreatorOrAdmin(authData, tx.query.issue, id);
        await tx.mutate.issue.delete({id});
      },

      async addLabel(
        tx: MutatorTx,
        {issueID, labelID}: {issueID: string; labelID: string},
      ) {
        await assertIsCreatorOrAdmin(authData, tx.query.issue, issueID);
        await tx.mutate.issueLabel.insert({issueID, labelID});
      },

      async removeLabel(
        tx: MutatorTx,
        {issueID, labelID}: {issueID: string; labelID: string},
      ) {
        await assertIsCreatorOrAdmin(authData, tx.query.issue, issueID);
        await tx.mutate.issueLabel.delete({issueID, labelID});
      },
    },

    notification: {
      async update(
        tx: MutatorTx,
        {
          issueID,
          subscribed,
          created,
        }: {issueID: string; subscribed: NotificationType; created: number},
      ) {
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
    },

    emoji: {
      async addToIssue(tx: MutatorTx, args: AddEmojiArgs) {
        await addEmoji(tx, 'issue', args);
      },

      async addToComment(tx: MutatorTx, args: AddEmojiArgs) {
        await addEmoji(tx, 'comment', args);
      },

      async remove(tx: MutatorTx, id: string) {
        await assertIsCreatorOrAdmin(authData, tx.query.emoji, id);
        await tx.mutate.emoji.delete({id});
      },
    },

    comment: {
      async add(tx: MutatorTx, {id, issueID, body, created}: AddCommentArgs) {
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

      async edit(tx: MutatorTx, {id, body}: {id: string; body: string}) {
        await assertIsCreatorOrAdmin(authData, tx.query.comment, id);
        await tx.mutate.comment.update({id, body});
      },

      async remove(tx: MutatorTx, id: string) {
        await assertIsCreatorOrAdmin(authData, tx.query.comment, id);
        await tx.mutate.comment.delete({id});
      },
    },

    label: {
      async create(tx: MutatorTx, {id, name}: {id: string; name: string}) {
        assert(isAdmin(authData), 'Only admins can create labels');
        await tx.mutate.label.insert({id, name});
      },

      async createAndAddToIssue(
        tx: MutatorTx,
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
      async set(
        tx: MutatorTx,
        {issueID, viewed}: {issueID: string; viewed: number},
      ) {
        assertIsLoggedIn(authData);
        const userID = authData.sub;
        await tx.mutate.viewState.upsert({issueID, userID, viewed});
      },
    },

    userPref: {
      async set(tx: MutatorTx, {key, value}: {key: string; value: string}) {
        assertIsLoggedIn(authData);
        const userID = authData.sub;
        await tx.mutate.userPref.upsert({key, value, userID});
      },
    },
  } as const;

  async function addEmoji(
    tx: Transaction<typeof schema, unknown>,
    subjectType: 'issue' | 'comment',
    {id, unicode, annotation, subjectID, created}: AddEmojiArgs,
  ) {
    assertIsLoggedIn(authData);
    const creatorID = authData.sub;

    if (subjectType === 'issue') {
      assertUserCanSeeIssue(tx, creatorID, subjectID);
    } else {
      assertUserCanSeeComment(tx, creatorID, subjectID);
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
    tx: Transaction<typeof schema, unknown>,
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

    const existingNotification = await tx.query.issueNotifications
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
}

export type Mutators = ReturnType<typeof createMutators>;
