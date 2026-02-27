import type {LogContext} from '@rocicorp/logger';
import auth from 'basic-auth';
import type {FastifyReply, FastifyRequest} from 'fastify';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import {isAdminPasswordValid} from '../config/zero-config.ts';
import type {Message} from '../types/processes.ts';

export type ResetClientGroupsPayload = {ids: string[] | undefined};
export type ResetClientGroupsMessage = Message<ResetClientGroupsPayload>;

export function handleResetzRequest(
  lc: LogContext,
  config: NormalizedZeroConfig,
  req: FastifyRequest,
  res: FastifyReply,
  getWorker: () => Promise<{
    send(message: ResetClientGroupsMessage): boolean;
  }>,
) {
  const credentials = auth(req);
  if (!isAdminPasswordValid(lc, config, credentials?.pass)) {
    void res
      .code(401)
      .header('WWW-Authenticate', 'Basic realm="Resetz Protected Area"')
      .send('Unauthorized');
    return;
  }

  const query = req.query as Record<string, unknown>;
  const ids =
    typeof query.id === 'string'
      ? query.id.split(',').filter(s => s.length > 0)
      : undefined;

  void getWorker().then(
    worker => {
      worker.send(['resetClientGroups', {ids}]);
      lc.info?.(
        ids
          ? `resetz: resetting client groups: ${ids.join(', ')}`
          : 'resetz: resetting all client groups',
      );
    },
    err => lc.error?.('resetz: failed to get worker', err),
  );

  void res
    .code(200)
    .header('Content-Type', 'application/json')
    .send(
      JSON.stringify({
        status: 'ok',
        resetting: ids ?? 'all',
      }),
    );
}
