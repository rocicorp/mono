import {type UpdateValue} from '@rocicorp/zero';
import {assertIsLoggedIn, type AuthData} from '../shared/auth.ts';
import {MutationError, MutationErrorCode} from '../shared/error.ts';
import type {ZbugsServerTransaction} from '../shared/mutators.ts';
import {builder, type schema} from '../shared/schema.ts';
import {postToDiscord} from './discord.ts';
import {sendEmail} from './email.ts';
import type {PostCommitTask} from './server-mutators.ts';

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
  tx: ZbugsServerTransaction,
  authData: AuthData | undefined,
  args: NotificationArgs,
  postCommitTasks: PostCommitTask[],
): Promise<void> {
  assertIsLoggedIn(authData);

  const {issueID, kind} = args;

  const issue = await tx.run(builder.issue.where('id', issueID).one());
  if (!issue) {
    throw new MutationError(
      `Issue not found`,
      MutationErrorCode.NOTIFICATION_FAILED,
      issueID,
    );
  }

  const modifierUserID = authData.sub;
  const modifierUser = await tx.run(
    builder.user.where('id', modifierUserID).one(),
  );
  if (!modifierUser) {
    throw new MutationError(
      `Modifier user not found`,
      MutationErrorCode.NOTIFICATION_FAILED,
      modifierUserID,
    );
  }

  // include the actor only for the initial `create-issue` action
  // exclude them for all other actions
  const excludeActor = kind !== 'create-issue';
  const recipientEmails = await gatherRecipients(
    tx,
    issueID,
    modifierUserID,
    excludeActor,
  );

  if (!issue.shortID) {
    throw new MutationError(
      `Issue short ID not found`,
      MutationErrorCode.NOTIFICATION_FAILED,
      issueID,
    );
  }

  // Only send to Discord for public issues
  const shouldSendToDiscord = issue.visibility === 'public';

  const baseIssueLink = `https://bugs.rocicorp.dev/issue/${issue.shortID}`;

  const sendNotifications = ({
    title,
    message,
    link,
  }: {
    title: string;
    message: string;
    link?: string;
  }) => {
    const resolvedLink = link ?? baseIssueLink;

    for (const email of recipientEmails) {
      const unsubscribeLink = `https://bugs.rocicorp.dev/api/unsubscribe?id=${issue.shortID}&email=${encodeURIComponent(email)}`;

      postCommitTasks.push(async () => {
        await sendEmail({
          tx,
          email,
          title,
          message,
          link: resolvedLink,
          issue,
          unsubscribeLink,
        });
      });
    }

    if (shouldSendToDiscord) {
      postCommitTasks.push(() =>
        postToDiscord({
          title,
          message,
          link: resolvedLink,
        }),
      );
    }
  };

  switch (kind) {
    case 'create-issue': {
      sendNotifications({
        title: `${modifierUser.login} reported an issue`,
        message: [issue.title, clip(issue.description ?? '')]
          .filter(Boolean)
          .join('\n'),
      });
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
          const newAssignee = await tx.run(
            builder.user.where('id', update.assigneeID).one(),
          );
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

      sendNotifications({
        title: `${modifierUser.login} updated an issue`,
        message: [issue.title, ...changes, clip(issue.description ?? '')]
          .filter(Boolean)
          .join('\n'),
      });
      break;
    }

    case 'add-emoji-to-issue': {
      const {emoji} = args;

      sendNotifications({
        title: `${modifierUser.login} reacted to an issue`,
        message: [issue.title, emoji].join('\n'),
      });

      break;
    }

    case 'add-emoji-to-comment': {
      const {commentID, emoji} = args;
      const comment = await tx.run(
        builder.comment.where('id', commentID).one(),
      );

      if (!comment) {
        throw new MutationError(
          `Comment not found`,
          MutationErrorCode.NOTIFICATION_FAILED,
          commentID,
        );
      }

      sendNotifications({
        title: `${modifierUser.login} reacted to a comment`,
        message: [clip(comment.body), emoji].filter(Boolean).join('\n'),
      });

      break;
    }

    case 'add-comment': {
      const {commentID, comment} = args;

      sendNotifications({
        title: `${modifierUser.login} commented on an issue`,
        message: [issue.title, clip(comment)].join('\n'),
        link: `${baseIssueLink}#comment-${commentID}`,
      });

      break;
    }

    case 'edit-comment': {
      const {commentID, comment} = args;

      sendNotifications({
        title: `${modifierUser.login} edited a comment`,
        message: [issue.title, clip(comment)].join('\n'),
        link: `${baseIssueLink}#comment-${commentID}`,
      });

      break;
    }
  }
}

function clip(s: string) {
  return s.length > 255 ? s.slice(0, 252) + '...' : s;
}

// From https://github.com/colinhacks/zod/blob/2c333e268c316deef829c736b8c46ec95ee03e39/packages/zod/src/v4/core/regexes.ts#L33C34-L35C102
const emailRegex =
  /^(?!\.)(?!.*\.\.)([A-Za-z0-9_'+\-.]*)[A-Za-z0-9_+-]@([A-Za-z0-9][A-Za-z0-9-]*\.)+[A-Za-z]{2,}$/;

export async function gatherRecipients(
  tx: ZbugsServerTransaction,
  issueID: string,
  actorID: string,
  excludeActor = true,
): Promise<string[]> {
  const sql = tx.dbTransaction.wrappedTransaction;

  // conditionally filter out the actor to not send them notifications on their own actions
  const actorFilter = excludeActor ? sql`AND n."userID" != ${actorID}` : sql``;

  // filter by issue visibility - only crew members get notifications for internal issues
  const recipientRows = await sql`
    SELECT DISTINCT u.email
    FROM "issueNotifications" n
    JOIN "user" u ON u.id = n."userID"
    JOIN "issue" i ON i.id = n."issueID"
    WHERE n."issueID" = ${issueID} 
      AND n."subscribed" = true
      ${actorFilter}
      AND u.email IS NOT NULL
      AND (
        -- If issue is public, include all candidates
        i.visibility = 'public'
        OR
        -- If issue is not public, only include crew members
        u.role = 'crew'
      );`;

  return recipientRows
    .map(row => row.email?.trim())
    .filter(email => email && emailRegex.test(email));
}
