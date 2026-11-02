import type {LogContext} from '@rocicorp/logger';
import auth from 'basic-auth';
import type {FastifyReply, FastifyRequest} from 'fastify';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import {
  getOperatorAccess,
  isAdminPasswordValid,
  type AdminAccess,
} from '../config/zero-config.ts';
import {AuthAttemptCounter} from './auth-attempt-counter.ts';

/**
 * Failed attempts allowed before the process blocks further attempts. All
 * HTTP endpoints guarded by the admin (or operator) password share this one
 * counter, since the password is the same for all of them.
 */
const FREE_ATTEMPTS = 5;

const attempts = new AuthAttemptCounter(FREE_ATTEMPTS);

/**
 * Checks the HTTP basic-auth credentials of a request to an endpoint guarded
 * by the admin password, and limits how often a wrong password can be tried.
 *
 * Returns the access the credentials grant. When they grant none, the
 * response has been sent (401 with a `WWW-Authenticate` challenge for
 * `realm`) and `undefined` is returned. While attempts are blocked after
 * repeated failures, every request with credentials is refused with 429 and a
 * `Retry-After` header without checking the password, so that the response
 * does not reveal whether it was right. A request without credentials is
 * never counted as an attempt.
 *
 * @param required `'admin'` accepts only the admin password (see
 *        {@link isAdminPasswordValid}); `'operator'` accepts the operator
 *        password too (see {@link getOperatorAccess}).
 */
export function authorizeAdminRequest(
  lc: LogContext,
  config: Pick<NormalizedZeroConfig, 'adminPassword' | 'operatorPassword'>,
  req: FastifyRequest,
  res: FastifyReply,
  realm: string,
  required: AdminAccess,
): AdminAccess | undefined {
  const password = auth(req)?.pass;
  const attempted = password !== undefined;

  if (attempted && attempts.blockedFor() > 0) {
    attempts.recordFailure();
    lc.warn?.('Refusing admin password attempt while blocked');
    void res
      .code(429)
      .header('Retry-After', Math.ceil(attempts.blockedFor() / 1000))
      .send('Too Many Requests');
    return undefined;
  }

  const access =
    required === 'admin'
      ? isAdminPasswordValid(lc, config, password)
        ? 'admin'
        : undefined
      : getOperatorAccess(lc, config, password);
  if (access !== undefined) {
    attempts.reset();
    return access;
  }

  if (attempted) {
    attempts.recordFailure();
  }
  void res
    .code(401)
    .header('WWW-Authenticate', `Basic realm="${realm}"`)
    .send('Unauthorized');
  return undefined;
}
