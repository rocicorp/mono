import type {Query, Transaction} from '@rocicorp/zero';
import {jwtVerify, type JWK, type JWTPayload} from 'jose';
import type {schema} from './schema.ts';
import {must} from '../../../packages/shared/src/must.ts';
import {assert} from '../../../packages/shared/src/asserts.ts';

export class Validators {
  readonly #publicJwk: JWK;
  readonly #tokenCache: Map<string, JWTPayload>;

  constructor(publicJwk: JWK) {
    this.#publicJwk = publicJwk;
    this.#tokenCache = new Map();
  }

  async verifyToken(
    tx: Transaction<typeof schema, unknown>,
  ): Promise<JWTPayload> {
    const token = tx.token;
    assert(token, 'user must be logged in for this operation');
    if (this.#tokenCache.size > 1000) {
      let i = 0;
      for (const key of this.#tokenCache.keys()) {
        this.#tokenCache.delete(key);
        ++i;
        if (i > 100) {
          break;
        }
      }
    }
    if (this.#tokenCache.has(token)) {
      return this.#tokenCache.get(token) as JWTPayload;
    }

    const payload = (await jwtVerify(token, this.#publicJwk)).payload;
    this.#tokenCache.set(token, payload);
    return payload;
  }

  isAdmin(token: JWTPayload) {
    return token.role === 'crew';
  }

  async assertIsCreatorOrAdmin(
    tx: Transaction<typeof schema, unknown>,
    query: Query<typeof schema, 'comment' | 'issue' | 'emoji'>,
    id: string,
  ) {
    const jwt = await this.verifyToken(tx);
    if (this.isAdmin(jwt)) {
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

  async assertUserCanSeeIssue(
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

  async assertUserCanSeeComment(
    tx: Transaction<typeof schema, unknown>,
    jwt: JWTPayload,
    commentID: string,
  ) {
    const comment = must(
      await tx.query.comment.where('id', commentID).one().run(),
    );

    await this.assertUserCanSeeIssue(tx, jwt, comment.issueID);
  }
}
