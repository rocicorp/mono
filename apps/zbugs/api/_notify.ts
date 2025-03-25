import {type Schema} from '../shared/schema.ts';
import {type Validators} from '../shared/validators.ts';
import {must} from '../../../packages/shared/src/must.ts';
import {assert} from '../../../packages/shared/src/asserts.ts';
import {type Transaction, type UpdateValue} from '@rocicorp/zero';
import {postToDiscord} from './_discord.ts';
import {schema} from '../shared/schema.ts';

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
  v: Validators,
  args: NotificationArgs,
) {
  const {issueID, kind} = args;
  const issue = await tx.query.issue.where('id', issueID).one().run();
  assert(issue);

  if (issue.visibility !== 'public') {
    console.log('Skipping notification for private issue', issueID);
    return;
  }

  const modifierUserID = must((await v.verifyToken(tx)).sub);
  const modifierUser = await tx.query.user
    .where('id', modifierUserID)
    .one()
    .run();
  console.log('modifierUser', modifierUser);
  assert(modifierUser);

  switch (kind) {
    case 'create-issue': {
      await postToDiscord({
        title: `${modifierUser.login} reported an issue`,
        message: [issue.title, clip(issue.description ?? '')]
          .filter(Boolean)
          .join('\n'),
        link: `https://bugs.rocicorp.dev/issue/${issue.shortID}`,
      });
      break;
    }

    case 'update-issue': {
      const {update} = args;
      if (update.open !== undefined) {
        const title = `${modifierUser.login} ${
          update.open ? 'reopened' : 'closed'
        } an issue`;
        await postToDiscord({
          title,
          message: issue.title,
          link: `https://bugs.rocicorp.dev/issue/${issue.shortID}`,
        });
      } else {
        await postToDiscord({
          title: `${modifierUser.login} updated an issue`,
          message: [issue.title, clip(issue.description ?? '')]
            .filter(Boolean)
            .join('\n'),
          link: `https://bugs.rocicorp.dev/issue/${issue.shortID}`,
        });
      }
      break;
    }

    case 'add-emoji-to-issue': {
      const {emoji} = args;
      await postToDiscord({
        title: `${modifierUser.login} reacted to an issue`,
        message: [issue.title, emoji].join('\n'),
        link: `https://bugs.rocicorp.dev/issue/${issue.shortID}`,
      });
      break;
    }

    case 'add-emoji-to-comment': {
      const {commentID, emoji} = args;
      const comment = await tx.query.comment.where('id', commentID).one().run();
      assert(comment);
      await postToDiscord({
        title: `${modifierUser.login} reacted to a comment`,
        message: [clip(comment.body), emoji].filter(Boolean).join('\n'),
        link: `https://bugs.rocicorp.dev/issue/${issue.shortID}`,
      });
      break;
    }

    case 'add-comment': {
      const {commentID, comment} = args;
      await postToDiscord({
        title: `${modifierUser.login} commented on an issue`,
        message: [issue.title, clip(comment)].join('\n'),
        link: `https://bugs.rocicorp.dev/issue/${issue.shortID}#comment-${commentID}`,
      });
      break;
    }

    case 'edit-comment': {
      const {commentID, comment} = args;
      await postToDiscord({
        title: `${modifierUser.login} edited a comment`,
        message: [issue.title, clip(comment)].join('\n'),
        link: `https://bugs.rocicorp.dev/issue/${issue.shortID}#comment-${commentID}`,
      });
      break;
    }
  }
}

function clip(s: string) {
  return s.length > 255 ? s.slice(0, 255) + '...' : s;
}
