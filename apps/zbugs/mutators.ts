import {jwtVerify} from 'jose';
import {schema} from './schema.ts';
import {must} from '../../packages/shared/src/must.ts';
import {assert} from '../../packages/shared/src/asserts.ts';
import type {
  Query,
  UpdateValue,
  ServerTransaction,
  Transaction,
  CustomMutatorDefs,
} from '@rocicorp/zero';

type AddEmojiArgs = {
  id: string;
  unicode: string;
  annotation: string;
  subjectID: string;
  creatorID: string;
  created: number;
};

export const mutators = {
  issue: {
    async create(
      tx,
      {
        id,
        title,
        description,
        creatorID,
        created,
        modified,
      }: {
        id: string;
        title: string;
        description?: string;
        creatorID: string;
        created: number;
        modified: number;
      },
    ) {
      if (tx.location === 'server') {
        creatorID = await getUserIDFromToken(tx);
        created = modified = Date.now();
      }

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

    async update(tx, change: UpdateValue<typeof schema.tables.issue>) {
      if (tx.location === 'server') {
        const [userID, issue] = await Promise.all([
          getUserIDFromToken(tx),
          tx.query.issue.where('id', change.id).one().run(),
        ]);

        if (!issue) {
          throw new Error('issue not found');
        }

        if (issue.creatorID !== userID) {
          if (!(await userIsAdmin(tx, userID))) {
            throw new Error(
              'Only admins or issue creators may update an issue',
            );
          }
        }
      }

      await tx.mutate.issue.update(change);
    },

    async delete(tx, id: string) {
      if (tx.location === 'server') {
        if (!(await userIsAdmin(tx, await getUserIDFromToken(tx)))) {
          throw new Error('Only admins may delete issues');
        }
      }

      await tx.mutate.issue.delete({id});
    },

    async addLabel(
      tx,
      {
        issueID,
        labelID,
      }: {
        issueID: string;
        labelID: string;
      },
    ) {
      if (tx.location === 'server') {
        if (!(await userIsAdminOrCreator(tx, tx.query.issue, issueID))) {
          throw new Error(
            'Users may not add labels to issues they cannot modify',
          );
        }
      }

      await tx.mutate.issueLabel.insert({
        issueID,
        labelID,
      });
    },

    async removeLabel(
      tx,
      {
        issueID,
        labelID,
      }: {
        issueID: string;
        labelID: string;
      },
    ) {
      if (tx.location === 'server') {
        if (!(await userIsAdminOrCreator(tx, tx.query.issue, issueID))) {
          throw new Error(
            'Users may not remove labels from issues they cannot modify',
          );
        }
      }

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
      if (tx.location === 'server') {
        if (!(await userIsAdminOrCreator(tx, tx.query.emoji, id))) {
          throw new Error('Only admins or emoji creators may remove an emoji');
        }
      }

      await tx.mutate.emoji.delete({id});
    },
  },

  comment: {
    async add(
      tx,
      {
        id,
        issueID,
        creatorID,
        body,
        created,
      }: {
        id: string;
        issueID: string;
        creatorID: string;
        body: string;
        created: number;
      },
    ) {
      if (tx.location === 'server') {
        created = Date.now();
        const userID = await getUserIDFromToken(tx);

        assert(
          creatorID === userID,
          'The creatorID of a comment must match the logged in user',
        );
        if (!(await userCanSeeIssue(tx, userID, issueID))) {
          throw new Error(
            'Users may not add comments to issues they cannot see',
          );
        }
      }

      await tx.mutate.comment.insert({
        id,
        issueID,
        creatorID,
        body,
        created,
      });
    },

    async edit(
      tx,
      {
        id,
        body,
      }: {
        id: string;
        body: string;
      },
    ) {
      if (tx.location === 'server') {
        if (!(await userIsAdminOrCreator(tx, tx.query.comment, id))) {
          throw new Error(
            'Only admins or comment creators may update a comment',
          );
        }
      }

      await tx.mutate.comment.update({id, body});
    },

    async remove(tx, id: string) {
      if (tx.location === 'server') {
        if (!(await userIsAdminOrCreator(tx, tx.query.comment, id))) {
          throw new Error(
            'Only admins or comment creators may delete a comment',
          );
        }
      }

      await tx.mutate.comment.delete({id});
    },
  },

  label: {
    async create(tx, {id, name}: {id: string; name: string}) {
      if (tx.location === 'server') {
        if (!(await userIsAdmin(tx, await getUserIDFromToken(tx)))) {
          throw new Error('Only admins may create labels');
        }
      }
      await tx.mutate.label.insert({id, name});
    },
  },

  viewState: {
    async set(
      tx,
      {
        issueID,
        userID,
        viewed,
      }: {issueID: string; userID: string; viewed: number},
    ) {
      if (tx.location === 'server') {
        const loggedInUser = await getUserIDFromToken(tx);
        if (loggedInUser !== userID) {
          throw new Error('Cannot set view state for another user');
        }
      }
      await tx.mutate.viewState.upsert({issueID, userID, viewed});
    },
  },

  userPref: {
    async set(
      tx,
      {key, value, userID}: {key: string; value: string; userID: string},
    ) {
      if (tx.location === 'server') {
        if ((await getUserIDFromToken(tx)) !== userID) {
          throw new Error('Cannot set preferences for another user');
        }
      }
      await tx.mutate.userPref.upsert({key, value, userID});
    },
  },
} as const satisfies CustomMutatorDefs<typeof schema>;

async function userIsAdminOrCreator(
  tx: ServerTransaction<typeof schema, unknown>,
  query: Query<typeof schema, 'comment' | 'issue' | 'emoji'>,
  id: string,
) {
  const userID = await getUserIDFromToken(tx);
  if (await userIsAdmin(tx, userID)) {
    return true;
  }

  const existingRow = await query.where('id', id).one().run();
  if (!existingRow) {
    return false;
  }

  if (existingRow.creatorID === userID) {
    return true;
  }

  return false;
}

async function addEmoji(
  tx: Transaction<typeof schema, unknown>,
  subjectType: 'issue' | 'comment',
  {id, unicode, annotation, subjectID, creatorID, created}: AddEmojiArgs,
) {
  if (tx.location === 'server') {
    created = Date.now();
    const userID = await getUserIDFromToken(tx);
    assert(
      userID === creatorID,
      'emoji creatorID must match the logged in user',
    );

    if (subjectType === 'issue') {
      if (!(await userCanSeeIssue(tx, userID, subjectID))) {
        throw new Error('Users may not add emojis to issues they cannot see');
      }
    } else {
      if (!(await userCanSeeComment(tx, userID, subjectID))) {
        throw new Error('Users may not add emojis to comments they cannot see');
      }
    }
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

async function userIsAdmin(
  tx: ServerTransaction<typeof schema, unknown>,
  userID: string,
) {
  return (
    (await tx.query.user
      .where('id', userID)
      .where('role', 'crew')
      .one()
      .run()) !== undefined
  );
}

async function userCanSeeIssue(
  tx: ServerTransaction<typeof schema, unknown>,
  userID: string,
  issueID: string,
) {
  const issue = await tx.query.issue.where('id', issueID).one().run();
  if (!issue) {
    return false;
  }

  if (issue.visibility === 'public') {
    return true;
  }

  if (issue.creatorID === userID) {
    return true;
  }

  if (await userIsAdmin(tx, userID)) {
    return true;
  }

  return false;
}

async function userCanSeeComment(
  tx: ServerTransaction<typeof schema, unknown>,
  userID: string,
  subjectID: string,
) {
  const comment = await tx.query.comment.where('id', subjectID).one().run();
  if (!comment) {
    return false;
  }

  return userCanSeeIssue(tx, userID, comment.issueID);
}

async function verifyToken(tx: ServerTransaction<typeof schema, unknown>) {
  return (
    await jwtVerify(
      must(tx.token, 'user must be logged in for this operation'),
      new TextEncoder().encode(
        must(process.env.ZERO_AUTH_SECRET, 'no secret set to verify the JWT'),
      ),
    )
  ).payload;
}

async function getUserIDFromToken(
  tx: ServerTransaction<typeof schema, unknown>,
) {
  return must(
    (await verifyToken(tx)).sub,
    'user must be logged in for this operation',
  );
}

export type Mutators = typeof mutators;
