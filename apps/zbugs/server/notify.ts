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
  isAssigneeChange?: boolean,
  previousAssigneeID?: string | null | undefined,
): Promise<void> {
  assertIsLoggedIn(authData);

  const {issueID, kind} = args;
  const issue = await tx.query.issue.where('id', issueID).one();
  assert(issue);

  const modifierUserID = authData.sub;
  const modifierUser = await tx.query.user.where('id', modifierUserID).one();
  assert(modifierUser);

  const recipients = await gatherRecipients(
    // TODO: we shouldn't have to assert and cast this given this is called from `server-mutators`.
    tx as ServerTransaction<Schema, postgres.TransactionSql>,
    issueID,
    isAssigneeChange,
    previousAssigneeID,
  );

  // If no recipients, skip notification
  if (recipients.length === 0) {
    console.log('No recipients for notification', issueID);
    return;
  }

  // Only send to Discord for public issues
  const shouldSendToDiscord = issue.visibility === 'public';

  switch (kind) {
    case 'create-issue': {
      const payload = {
        title: `${modifierUser.login} reported an issue`,
        message: [issue.title, clip((await issue.description) ?? '')]
          .filter(Boolean)
          .join('\n'),
        link: `https://bugs.rocicorp.dev/issue/${issue.shortID}`,
      };
      for (const recipient of recipients) {
        postCommitTasks.push(async () => {
          await sendEmail({tx, email: recipient, ...payload, issue});
        });
      }
      if (shouldSendToDiscord) {
        postCommitTasks.push(() => postToDiscord(payload));
      }
      break;
    }

    case 'update-issue': {
      const {update} = args;
      const changes: string[] = [];

      if (update.open !== undefined) {
        changes.push(`Status changed to ${update.open ? 'open' : 'closed'}`);
      }
      if (update.assigneeID !== undefined) {
        if (update.assigneeID === null) {
          changes.push('Assignee was removed');
        } else {
          const newAssignee = await tx.query.user
            .where('id', update.assigneeID)
            .one();
          if (newAssignee) {
            changes.push(`Assignee changed to ${newAssignee.login}`);
          }
        }
      }
      if (update.visibility !== undefined) {
        changes.push(`Visibility changed to ${update.visibility}`);
      }
      if (update.title !== undefined) {
        changes.push(`Title changed to "${update.title}"`);
      }
      if (update.description !== undefined) {
        changes.push('Description was updated');
      }

      const title = `${modifierUser.login} updated an issue`;
      const message = [
        issue.title,
        ...changes,
        clip((await issue.description) ?? ''),
      ]
        .filter(Boolean)
        .join('\n');

      const payload = {
        recipients,
        title,
        message,
        link: `https://bugs.rocicorp.dev/issue/${issue.shortID}`,
      };

      for (const recipient of recipients) {
        postCommitTasks.push(async () => {
          await sendEmail({tx, email: recipient, ...payload, issue});
        });
      }

      if (shouldSendToDiscord) {
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

      for (const recipient of recipients) {
        postCommitTasks.push(async () => {
          await sendEmail({tx, email: recipient, ...payload, issue});
        });
      }

      if (shouldSendToDiscord) {
        postCommitTasks.push(() => postToDiscord(payload));
      }
      break;
    }

    case 'add-emoji-to-comment': {
      const {commentID, emoji} = args;
      const comment = await tx.query.comment.where('id', commentID).one();
      assert(comment);

      const payload = {
        recipients,
        title: `${modifierUser.login} reacted to a comment`,
        message: [clip(await comment.body), emoji].filter(Boolean).join('\n'),
        link: `https://bugs.rocicorp.dev/issue/${issue.shortID}`,
      };

      for (const recipient of recipients) {
        postCommitTasks.push(async () => {
          await sendEmail({tx, email: recipient, ...payload, issue});
        });
      }

      if (shouldSendToDiscord) {
        postCommitTasks.push(() => postToDiscord(payload));
      }
      break;
    }

    case 'add-comment': {
      const {commentID, comment} = args;
      const payload = {
        recipients,
        title: `${modifierUser.login} commented on an issue`,
        message: [issue.title, clip(await comment)].join('\n'),
        link: `https://bugs.rocicorp.dev/issue/${issue.shortID}#comment-${commentID}`,
      };

      for (const recipient of recipients) {
        postCommitTasks.push(async () => {
          await sendEmail({tx, email: recipient, ...payload, issue});
        });
      }

      if (shouldSendToDiscord) {
        postCommitTasks.push(() => postToDiscord(payload));
      }
      break;
    }

    case 'edit-comment': {
      const {commentID, comment} = args;

      const payload = {
        recipients,
        title: `${modifierUser.login} edited a comment`,
        message: [issue.title, clip(await comment)].join('\n'),
        link: `https://bugs.rocicorp.dev/issue/${issue.shortID}#comment-${commentID}`,
      };

      for (const recipient of recipients) {
        postCommitTasks.push(async () => {
          await sendEmail({tx, email: recipient, ...payload, issue});
        });
      }

      if (shouldSendToDiscord) {
        postCommitTasks.push(() => postToDiscord(payload));
      }
      break;
    }
  }
}

function clip(s: string) {
  return s.length > 255 ? s.slice(0, 252) + '...' : s;
}

export async function gatherRecipients(
  tx: ServerTransaction<Schema, postgres.TransactionSql>,
  issueID: string,
  isAssigneeChange = false,
  previousAssigneeID?: string | null | undefined,
): Promise<string[]> {
  const sql = tx.dbTransaction.wrappedTransaction;

  // Get all recipient candidates
  const recipientRows = await sql`
    WITH issue_info AS (
      SELECT 
        "creatorID",
        "assigneeID",
        "visibility"
      FROM "issue"
      WHERE id = ${issueID}
    ),
    recipient_candidates AS (
      -- Issue creator
      SELECT DISTINCT "user".id, "user".email, "user".role
      FROM issue_info
      JOIN "user" ON "user".id = issue_info."creatorID"
      
      UNION
      
      -- Users with emojis on the issue
      SELECT DISTINCT "user".id, "user".email, "user".role
      FROM "emoji"
      JOIN "user" ON "user".id = "emoji"."creatorID"
      WHERE "emoji"."subjectID" = ${issueID}
      
      UNION
      
      -- Users who have commented
      SELECT DISTINCT "user".id, "user".email, "user".role
      FROM "comment"
      JOIN "user" ON "user".id = "comment"."creatorID"
      WHERE "comment"."issueID" = ${issueID}
      
      UNION
      
      -- Users with emojis on comments
      SELECT DISTINCT "user".id, "user".email, "user".role
      FROM "emoji"
      JOIN "comment" ON "comment".id = "emoji"."subjectID"
      JOIN "user" ON "user".id = "emoji"."creatorID"
      WHERE "comment"."issueID" = ${issueID}
      
      UNION
      
      -- Current assignee (if exists)
      SELECT DISTINCT "user".id, "user".email, "user".role
      FROM issue_info
      JOIN "user" ON "user".id = issue_info."assigneeID"
      WHERE issue_info."assigneeID" IS NOT NULL
      
      UNION
      
      -- Previous assignee (if this is an assignee change and previous assignee exists)
      SELECT DISTINCT "user".id, "user".email, "user".role
      FROM "user"
      WHERE ${isAssigneeChange} 
        AND ${previousAssigneeID ? sql`"user".id = ${previousAssigneeID}` : sql`FALSE`}
    )
    SELECT DISTINCT email
    FROM recipient_candidates
    WHERE email IS NOT NULL
    AND (
      -- If issue is public, include all candidates
      EXISTS (
        SELECT 1 FROM issue_info WHERE visibility = 'public'
      )
      OR
      -- If issue is not public, only include crew members
      role = 'crew'
    );`;

  return recipientRows.map(row => row.email);
}
