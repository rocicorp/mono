import type {Query} from '@rocicorp/zero';
import {must} from '../../../packages/shared/src/must.ts';
import * as v from '../../../packages/shared/src/valita.ts';
import {MutationError, MutationErrorCode} from './error.ts';
import type {ZbugsTransaction} from './mutators.ts';
import {builder, type schema} from './schema.ts';

// TDOO(arv): Use zod-mini here too

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
  if (!authData) {
    throw new MutationError(
      'User must be logged in for this operation',
      MutationErrorCode.NOT_LOGGED_IN,
    );
  }
}

export function isAdmin(token: AuthData | undefined) {
  assertIsLoggedIn(token);
  return token.role === 'crew';
}

export async function assertIsCreatorOrAdmin(
  tx: ZbugsTransaction,
  authData: AuthData | undefined,
  query: Query<'comment' | 'issue' | 'emoji', typeof schema>,
  id: string,
) {
  assertIsLoggedIn(authData);
  if (isAdmin(authData)) {
    return;
  }
  const entity = await tx.run(query.where('id', id).one());
  // Security: Use generic error message to avoid leaking entity existence
  if (!entity) {
    throw new MutationError(
      `Not authorized to access this resource`,
      MutationErrorCode.NOT_AUTHORIZED,
      id,
    );
  }
  if (authData.sub !== entity.creatorID) {
    throw new MutationError(
      `Not authorized to access this resource`,
      MutationErrorCode.NOT_AUTHORIZED,
      id,
    );
  }
}

export async function assertUserCanSeeIssue(
  tx: ZbugsTransaction,
  userID: string,
  issueID: string,
) {
  const issue = must(await tx.run(builder.issue.where('id', issueID).one()));
  const user = must(await tx.run(builder.user.where('id', userID).one()));

  if (
    issue.visibility !== 'public' &&
    userID !== issue.creatorID &&
    user.role !== 'crew'
  ) {
    throw new MutationError(
      'User does not have permission to view this issue',
      MutationErrorCode.NOT_AUTHORIZED,
      issueID,
    );
  }
}

export async function assertUserCanSeeComment(
  tx: ZbugsTransaction,
  userID: string,
  commentID: string,
) {
  const comment = must(
    await tx.run(builder.comment.where('id', commentID).one()),
  );

  await assertUserCanSeeIssue(tx, userID, comment.issueID);
}
