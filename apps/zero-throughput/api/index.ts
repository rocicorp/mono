import {mustGetQuery, type ReadonlyJSONValue} from '@rocicorp/zero';
import {
  handleQueryRequest,
  type QueryRequestHandler,
} from '@rocicorp/zero/server';
import Fastify, {type FastifyReply, type FastifyRequest} from 'fastify';
import {queries} from '../src/queries.ts';
import {schema} from '../src/schema.ts';

export const fastify = Fastify({
  logger: process.env.NODE_ENV !== 'test',
});

fastify.get('/health', (_req, reply) => {
  reply.send({status: 'ok'});
});

fastify.get('/', (_req, reply) => {
  reply.send({status: 'ok', service: 'zero-throughput-api'});
});

fastify.post('/api/push', mutateHandler);
fastify.post('/api/mutate', mutateHandler);

function mutateHandler(_request: FastifyRequest, reply: FastifyReply) {
  reply.status(501).send({error: 'Mutations not supported'});
}

fastify.post<{
  Querystring: Record<string, string>;
  Body: ReadonlyJSONValue;
}>('/api/get-queries', queryHandler);

fastify.post<{
  Querystring: Record<string, string>;
  Body: ReadonlyJSONValue;
}>('/api/query', queryHandler);

type AnyQuery = ReturnType<QueryRequestHandler>;

const queryTransformHandler: QueryRequestHandler = (name, args) => {
  const query = mustGetQuery(queries, name);
  return query.fn({args, ctx: undefined}) as unknown as AnyQuery;
};

function extractUserID(
  headers: Record<string, string | string[] | undefined>,
  query: Record<string, string>,
): string | undefined {
  const authHeader = headers['authorization'];
  if (typeof authHeader === 'string') {
    if (authHeader.startsWith('Bearer ')) {
      return authHeader.slice('Bearer '.length).trim();
    }
    return authHeader.trim();
  }
  return (
    (headers['x-user-id'] as string | undefined) ?? query.userID ?? undefined
  );
}

async function queryHandler(
  request: FastifyRequest<{
    Querystring: Record<string, string>;
    Body: ReadonlyJSONValue;
  }>,
  reply: FastifyReply,
) {
  const authUserID = extractUserID(request.headers, request.query);

  const response = await handleQueryRequest({
    handler: queryTransformHandler,
    schema,
    query: request.query,
    body: request.body,
    userID: authUserID,
    logLevel: 'info',
  });
  reply.send(response);
}

export default async function handler(
  req: FastifyRequest,
  reply: FastifyReply,
) {
  await fastify.ready();
  fastify.server.emit('request', req, reply);
}
