import type {Query, Transaction} from '@rocicorp/zero';
import type {schema} from './schema.ts';
import {must} from '../../../packages/shared/src/must.ts';
import {assert} from '../../../packages/shared/src/asserts.ts';
import * as v from '../../../packages/shared/src/valita.ts';

/** The contents of the zbugs JWT */
export const authDataSchema = v.object({
  sub: v.string(),
  role: v.union(v.literal('crew'), v.literal('user')),
  name: v.string(),
  iat: v.number(),
  exp: v.number(),
});

export type AuthData = v.Infer<typeof authDataSchema>;

export function assertIsLoggedIn(
  authData: AuthData | undefined,
): asserts authData {
  assert(authData, 'user must be logged in for this operation');
}

export function isAdmin(token: AuthData | undefined) {
  assertIsLoggedIn(token);
  return token.role === 'crew';
}

export async function assertIsCreatorOrAdmin(
  authData: AuthData | undefined,
  query: Query<typeof schema, 'comment' | 'issue' | 'emoji'>,
  id: string,
) {
  assertIsLoggedIn(authData);
  if (isAdmin(authData)) {
    return;
  }
  const creatorID = must(
    await query.where('id', id).one(),
    `entity ${id} does not exist`,
  ).creatorID;
  assert(
    authData.sub === creatorID,
    `User ${authData.sub} is not an admin or the creator of the target entity`,
  );
}

export async function assertUserCanSeeIssue(
  tx: Transaction<typeof schema, unknown>,
  authData: AuthData,
  issueID: string,
) {
  const issue = must(await tx.query.issue.where('id', issueID).one());

  assert(
    issue.visibility === 'public' ||
      authData.sub === issue.creatorID ||
      authData.role === 'crew',
    'User does not have permission to view this issue',
  );
}

export async function assertUserCanSeeComment(
  tx: Transaction<typeof schema, unknown>,
  authData: AuthData,
  commentID: string,
) {
  const comment = must(await tx.query.comment.where('id', commentID).one());

  await assertUserCanSeeIssue(tx, authData, comment.issueID);
}
