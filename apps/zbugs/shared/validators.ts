import type {Query, Transaction} from '@rocicorp/zero';
import {jwtVerify, type JWK, type JWTPayload} from 'jose';
import type {schema} from './schema.ts';
import {must} from '../../../packages/shared/src/must.ts';
import {assert} from '../../../packages/shared/src/asserts.ts';

const publicJwk = JSON.parse(
  process?.env?.VITE_PUBLIC_JWK ?? import.meta.env.VITE_PUBLIC_JWK,
) as JWK;

export async function verifyToken(
  tx: Transaction<typeof schema, unknown>,
): Promise<JWTPayload> {
  return (
    await jwtVerify(
      must(tx.token, 'user must be logged in for this operation'),
      publicJwk,
    )
  ).payload;
}

export function isAdmin(token: JWTPayload) {
  return token.role === 'crew';
}

export async function assertIsCreatorOrAdmin(
  tx: Transaction<typeof schema, unknown>,
  query: Query<typeof schema, 'comment' | 'issue' | 'emoji'>,
  id: string,
) {
  const jwt = await verifyToken(tx);
  if (isAdmin(jwt)) {
    return;
  }
  const creatorID = must(
    await query.where('id', id).one().run(),
    `entity ${id} does not exist`,
  ).creatorID;
  assert(
    jwt.sub === creatorID,
    `User ${jwt.sub} is not an admin or the creator of the target entity`,
  );
}

export async function assertUserCanSeeIssue(
  tx: Transaction<typeof schema, unknown>,
  jwt: JWTPayload,
  issueID: string,
) {
  const issue = must(await tx.query.issue.where('id', issueID).one().run());

  assert(
    issue.visibility === 'public' ||
      jwt.sub === issue.creatorID ||
      jwt.role === 'crew',
    'User does not have permission to view this issue',
  );
}

export async function assertUserCanSeeComment(
  tx: Transaction<typeof schema, unknown>,
  jwt: JWTPayload,
  commentID: string,
) {
  const comment = must(
    await tx.query.comment.where('id', commentID).one().run(),
  );

  await assertUserCanSeeIssue(tx, jwt, comment.issueID);
}
