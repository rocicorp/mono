/**
 * Prints a zbugs JWT for a user, signed with `PRIVATE_JWK` from `.env`, for
 * a client that can't log in through GitHub in development, such as the
 * Swift app (apps/zbugs-swift).
 *
 *   pnpm dev-token <userID> [crew|user]
 */
import '../../../packages/shared/src/dotenv.ts';

import {SignJWT, type JWK} from 'jose';

const [sub, role = 'user'] = process.argv.slice(2);
if (!sub || (role !== 'crew' && role !== 'user')) {
  process.stderr.write('Usage: pnpm dev-token <userID> [crew|user]\n');
  process.exit(2);
}
if (!process.env.PRIVATE_JWK) {
  process.stderr.write('PRIVATE_JWK is not set; see .env.example\n');
  process.exit(1);
}
const privateJwk = JSON.parse(process.env.PRIVATE_JWK) as JWK;
// The claims of `jwtDataSchema` in shared/auth.ts.
const payload = {
  sub,
  role,
  name: sub,
  iat: Math.floor(Date.now() / 1000),
  exp: 0, // setExpirationTime below sets it
};
const jwt = await new SignJWT(payload)
  .setProtectedHeader({alg: privateJwk.alg ?? 'EdDSA'})
  .setExpirationTime('30days')
  .sign(privateJwk);
process.stdout.write(jwt + '\n');
