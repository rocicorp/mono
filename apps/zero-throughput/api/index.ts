import type {FastifyReply, FastifyRequest} from 'fastify';
import {fastify} from '../src/server.ts';

export {fastify};

export default async function handler(
  req: FastifyRequest,
  reply: FastifyReply,
) {
  await fastify.ready();
  fastify.server.emit('request', req, reply);
}
