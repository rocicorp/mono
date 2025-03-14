import {schema} from './schema.ts';
import {must} from '../../../packages/shared/src/must.ts';
import {assert} from '../../../packages/shared/src/asserts.ts';
import type {UpdateValue, Transaction, CustomMutatorDefs} from '@rocicorp/zero';
import type {JWK} from 'jose';
import {Validators} from './validators.ts';

type AddEmojiArgs = {
  id: string;
  unicode: string;
  annotation: string;
  subjectID: string;
  creatorID: string;
  created: number;
};

export function createMutators(publicJwk: string) {
  const v = new Validators(JSON.parse(publicJwk) as JWK);
  return {
    issue: {
      async create(
        tx,
        {
          id,
          title,
          description,
          created,
          modified,
        }: {
          id: string;
          title: string;
          description?: string;
          created: number;
          modified: number;
        },
      ) {
        if (tx.location === 'server') {
          created = modified = Date.now();
        }
        const creatorID = must((await v.verifyToken(tx)).sub);

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
        await v.assertIsCreatorOrAdmin(tx, tx.query.issue, change.id);
        await tx.mutate.issue.update(change);
      },

      async delete(tx, id: string) {
        await v.assertIsCreatorOrAdmin(tx, tx.query.issue, id);
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
        await v.assertIsCreatorOrAdmin(tx, tx.query.issue, issueID);
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
        await v.assertIsCreatorOrAdmin(tx, tx.query.issue, issueID);
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
        await v.assertIsCreatorOrAdmin(tx, tx.query.emoji, id);
        await tx.mutate.emoji.delete({id});
      },
    },

    comment: {
      async add(
        tx,
        {
          id,
          issueID,
          body,
          created,
        }: {
          id: string;
          issueID: string;
          body: string;
          created: number;
        },
      ) {
        if (tx.location === 'server') {
          created = Date.now();
        }

        const jwt = await v.verifyToken(tx);
        const creatorID = must(jwt.sub);

        await v.assertUserCanSeeIssue(tx, jwt, issueID);

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
        await v.assertIsCreatorOrAdmin(tx, tx.query.comment, id);
        await tx.mutate.comment.update({id, body});
      },

      async remove(tx, id: string) {
        await v.assertIsCreatorOrAdmin(tx, tx.query.comment, id);
        await tx.mutate.comment.delete({id});
      },
    },

    label: {
      async create(tx, {id, name}: {id: string; name: string}) {
        const jwt = await v.verifyToken(tx);
        assert(v.isAdmin(jwt), 'Only admins can create labels');
        await tx.mutate.label.insert({id, name});
      },
    },

    viewState: {
      async set(tx, {issueID, viewed}: {issueID: string; viewed: number}) {
        const userID = must((await v.verifyToken(tx)).sub);
        await tx.mutate.viewState.upsert({issueID, userID, viewed});
      },
    },

    userPref: {
      async set(tx, {key, value}: {key: string; value: string}) {
        const userID = must((await v.verifyToken(tx)).sub);
        await tx.mutate.userPref.upsert({key, value, userID});
      },
    },
  } as const satisfies CustomMutatorDefs<typeof schema>;

  async function addEmoji(
    tx: Transaction<typeof schema, unknown>,
    subjectType: 'issue' | 'comment',
    {id, unicode, annotation, subjectID, creatorID, created}: AddEmojiArgs,
  ) {
    if (tx.location === 'server') {
      created = Date.now();
    }

    const jwt = await v.verifyToken(tx);
    creatorID = must(jwt.sub);

    if (subjectType === 'issue') {
      v.assertUserCanSeeIssue(tx, jwt, subjectID);
    } else {
      v.assertUserCanSeeComment(tx, jwt, subjectID);
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
}

export type Mutators = ReturnType<typeof createMutators>;
