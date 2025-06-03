import auth from 'basic-auth';
import type {FastifyReply, FastifyRequest} from 'fastify';
import fs from 'fs';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import type {LogContext} from '@rocicorp/logger';
import v8 from 'v8';

export function handleHeapzRequest(
  lc: LogContext,
  config: NormalizedZeroConfig,
  req: FastifyRequest,
  res: FastifyReply,
) {
  const credentials = auth(req);
  const expectedPassword = config.adminPassword;
  if (!expectedPassword || credentials?.pass !== expectedPassword) {
    void res
      .code(401)
      .header('WWW-Authenticate', 'Basic realm="Heapz Protected Area"')
      .send('Unauthorized');
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
