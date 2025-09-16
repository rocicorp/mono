/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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
