// https://vercel.com/templates/other/fastify-serverless-function
import Fastify from 'fastify';

export const app = Fastify({
  logger: true,
});

app.get('/api', async (req, reply) => {
  return reply.status(200).send({hello: 'world'});
});

export default async function handler(req, reply) {
  await app.ready();
  app.server.emit('request', req, reply);
}
