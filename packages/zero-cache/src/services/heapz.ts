import type {LogContext} from '@rocicorp/logger';
import auth from 'basic-auth';
import type {FastifyReply, FastifyRequest} from 'fastify';
import fs from 'fs';
import v8 from 'v8';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import {isAdminPasswordValid} from '../config/zero-config.ts';

export function handleHeapzRequest(
  lc: LogContext,
  config: NormalizedZeroConfig,
  req: FastifyRequest,
  res: FastifyReply,
) {
  const credentials = auth(req);
  if (!isAdminPasswordValid(lc, config, credentials?.pass)) {
    void res
      .code(401)
      .header('WWW-Authenticate', 'Basic realm="Heapz Protected Area"')
      .send('Unauthorized');
    return;
  }

  const filename = v8.writeHeapSnapshot();
  const stream = fs.createReadStream(filename);
  void res
    .header('Content-Type', 'application/octet-stream')
    .header('Content-Disposition', `attachment; filename=${filename}`)
    .send(stream);

  // Clean up temp file after streaming
  stream.on('end', () => {
    fs.unlink(filename, err => {
      if (err) {
        lc.error?.('Error deleting heap snapshot:', err);
      }
    });
  });
}
