import {type Schema} from '../shared/schema.ts';
import {assert} from '../../../packages/shared/src/asserts.ts';
import {
  type ServerTransaction,
  type Transaction,
  type UpdateValue,
} from '@rocicorp/zero';
import {postToDiscord} from './discord.ts';
import {schema} from '../shared/schema.ts';
import {assertIsLoggedIn, type AuthData} from '../shared/auth.ts';
import type {PostCommitTask} from './server-mutators.ts';
import type postgres from 'postgres';
import {sendEmail} from './email.ts';

type CreateIssueNotification = {
  kind: 'create-issue';
};

type UpdateIssueNotification = {
  kind: 'update-issue';
  update: UpdateValue<typeof schema.tables.issue>;
};

type AddEmojiToIssueNotification = {
  kind: 'add-emoji-to-issue';
  emoji: string;
};

type AddEmojiToCommentNotification = {
  kind: 'add-emoji-to-comment';
  commentID: string;
  emoji: string;
};

type AddCommentNotification = {
  kind: 'add-comment';
  commentID: string;
  comment: string;
};

type EditCommentNotification = {
  kind: 'edit-comment';
  commentID: string;
  comment: string;
};

type NotificationArgs = {issueID: string} & (
  | CreateIssueNotification
  | UpdateIssueNotification
  | AddEmojiToIssueNotification
  | AddEmojiToCommentNotification
  | AddCommentNotification
  | EditCommentNotification
);

export async function notify(
  tx: Transaction<Schema>,
  authData: AuthData | undefined,
  args: NotificationArgs,
  postCommitTasks: PostCommitTask[],
): Promise<void> {
  assertIsLoggedIn(authData);

  const {issueID, kind} = args;
  const issue = await tx.query.issue.where('id', issueID).one();
  assert(issue);

  if (issue.visibility !== 'public') {
    console.log('Skipping notification for private issue', issueID);
    return;
  }

  const modifierUserID = authData.sub;
  const modifierUser = await tx.query.user.where('id', modifierUserID).one();
  assert(modifierUser);

  const recipients = await gatherRecipients(
    // TODO: we shouldn't have to assert and cast this given this is called from `server-mutators`.
    tx as ServerTransaction<Schema, postgres.TransactionSql>,
    issueID,
  );

  switch (kind) {
    case 'create-issue': {
      const payload = {
        title: `${modifierUser.login} reported an issue`,
        message: [issue.title, clip(issue.description ?? '')]
          .filter(Boolean)
          .join('\n'),
        link: `https://bugs.rocicorp.dev/issue/${issue.shortID}`,
      };
      for (const recipient of recipients) {
        postCommitTasks.push(async () => {
          await sendEmail({tx, email: recipient, ...payload});
        });
      }
      postCommitTasks.push(() => postToDiscord(payload));
      break;
    }

    case 'update-issue': {
      const {update} = args;
      if (update.open !== undefined) {
        const title = `${modifierUser.login} ${
          update.open ? 'reopened' : 'closed'
        } an issue`;

        const payload = {
          recipients,
          title,
          message: issue.title,
          link: `https://bugs.rocicorp.dev/issue/${issue.shortID}`,
        };
        for (const recipient of recipients) {
          postCommitTasks.push(async () => {
            await sendEmail({tx, email: recipient, ...payload});
          });
        }
        postCommitTasks.push(() => postToDiscord(payload));
      } else {
        const payload = {
          recipients,
          title: `${modifierUser.login} updated an issue`,
          message: [issue.title, clip(issue.description ?? '')]
            .filter(Boolean)
            .join('\n'),
          link: `https://bugs.rocicorp.dev/issue/${issue.shortID}`,
        };

        postCommitTasks.push(() => postToDiscord(payload));
      }
      break;
    }

    case 'add-emoji-to-issue': {
      const {emoji} = args;
      const payload = {
        recipients,
        title: `${modifierUser.login} reacted to an issue`,
        message: [issue.title, emoji].join('\n'),
        link: `https://bugs.rocicorp.dev/issue/${issue.shortID}`,
      };

      postCommitTasks.push(() => postToDiscord(payload));
      break;
    }

    case 'add-emoji-to-comment': {
      const {commentID, emoji} = args;
      const comment = await tx.query.comment.where('id', commentID).one();
      assert(comment);

      const payload = {
        recipients,
        title: `${modifierUser.login} reacted to a comment`,
        message: [clip(comment.body), emoji].filter(Boolean).join('\n'),
        link: `https://bugs.rocicorp.dev/issue/${issue.shortID}`,
      };

      postCommitTasks.push(() => postToDiscord(payload));
      break;
    }

    case 'add-comment': {
      const {commentID, comment} = args;
      const payload = {
        recipients,
        title: `${modifierUser.login} commented on an issue`,
        message: [issue.title, clip(comment)].join('\n'),
        link: `https://bugs.rocicorp.dev/issue/${issue.shortID}#comment-${commentID}`,
      };

      for (const recipient of recipients) {
        postCommitTasks.push(async () => {
          await sendEmail({tx, email: recipient, ...payload});
        });
      }
      postCommitTasks.push(() => postToDiscord(payload));
      break;
    }

    case 'edit-comment': {
      const {commentID, comment} = args;

      const payload = {
        recipients,
        title: `${modifierUser.login} edited a comment`,
        message: [issue.title, clip(comment)].join('\n'),
        link: `https://bugs.rocicorp.dev/issue/${issue.shortID}#comment-${commentID}`,
      };

      postCommitTasks.push(() => postToDiscord(payload));
      break;
    }
  }
}

function clip(s: string) {
  return s.length > 255 ? s.slice(0, 252) + '...' : s;
}

async function gatherRecipients(
  tx: ServerTransaction<Schema, postgres.TransactionSql>,
  issueID: string,
): Promise<string[]> {
  const sql = tx.dbTransaction.wrappedTransaction;

  const recipientRows = await sql`SELECT DISTINCT "user".email
  FROM (
      SELECT DISTINCT "creatorID" AS "userID" FROM "comment" WHERE "issueID" = ${issueID}
      UNION
      SELECT DISTINCT "creatorID" AS "userID" FROM "emoji" WHERE "subjectID" = ${issueID}
      UNION
      SELECT "creatorID" AS "userID" FROM "issue" WHERE "id" = ${issueID}
      UNION
      SELECT "assigneeID" AS "userID" FROM "issue" WHERE "id" = ${issueID}
  ) AS combined
  JOIN "user" ON "user".id = combined."userID" AND "user".email IS NOT NULL;`;

  return recipientRows.map(row => row.email);
}
