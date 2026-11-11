import {createHmac, timingSafeEqual} from 'node:crypto';

/**
 * The token in an emailed unsubscribe link. It proves that the link was issued
 * by us for this email address and issue, so that knowing someone's email is
 * not enough to unsubscribe them.
 *
 * It is an HMAC rather than a JWT signed with `PRIVATE_JWK` so that it can
 * never be mistaken for a session token.
 */
export function signUnsubscribeToken(email: string, shortID: number): string {
  const secret = process.env.PRIVATE_JWK;
  // An empty key would make the token forgeable.
  if (!secret) {
    throw new Error('PRIVATE_JWK is not set');
  }
  return sign(secret, email, shortID);
}

export function verifyUnsubscribeToken(
  token: string,
  email: string,
  shortID: number,
): boolean {
  const secret = process.env.PRIVATE_JWK;
  // Without a secret we cannot have issued any token, so none is valid.
  if (!secret) {
    return false;
  }
  const expected = Buffer.from(sign(secret, email, shortID));
  const actual = Buffer.from(token);
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

function sign(secret: string, email: string, shortID: number): string {
  return createHmac('sha256', secret)
    .update(JSON.stringify(['unsubscribe', email, shortID]))
    .digest('base64url');
}
