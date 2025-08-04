import type {Query, Transaction} from '@rocicorp/zero';
import {assert} from '../../../packages/shared/src/asserts.ts';
import {must} from '../../../packages/shared/src/must.ts';
import * as v from '../../../packages/shared/src/valita.ts';
import type {schema} from './schema.ts';

/** The contents of the zbugs JWT */
export const jwtDataSchema = v.object({
  sub: v.string(),
  role: v.literalUnion('crew', 'user'),
  name: v.string(),
  iat: v.number(),
  exp: v.number(),
});

export type JWTData = v.Infer<typeof jwtDataSchema>;

export type AuthData = Pick<JWTData, 'sub' | 'role'>;
export type Role = AuthData['role'];

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
  userID: string,
  issueID: string,
) {
  const issue = must(await tx.query.issue.where('id', issueID).one());
  const user = must(await tx.query.user.where('id', userID).one());

  assert(
    issue.visibility === 'public' ||
      userID === issue.creatorID ||
      user.role === 'crew',
    'User does not have permission to view this issue',
  );
}

export async function assertUserCanSeeComment(
  tx: Transaction<typeof schema, unknown>,
  userID: string,
  commentID: string,
) {
  const comment = must(await tx.query.comment.where('id', commentID).one());

  await assertUserCanSeeIssue(tx, userID, comment.issueID);
}
