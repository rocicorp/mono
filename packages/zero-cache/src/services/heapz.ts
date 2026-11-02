import fs from 'fs';
import v8 from 'v8';
import type {LogContext} from '@rocicorp/logger';
import type {FastifyReply, FastifyRequest} from 'fastify';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import {authorizeAdminRequest} from './admin-auth.ts';

export function handleHeapzRequest(
  lc: LogContext,
  config: NormalizedZeroConfig,
  req: FastifyRequest,
  res: FastifyReply,
) {
  if (!config.enableHeapz) {
    void res.code(404).send('Not Found');
    return;
  }
  if (
    authorizeAdminRequest(
      lc,
      config,
      req,
      res,
      'Heapz Protected Area',
      'admin',
    ) === undefined
  ) {
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
