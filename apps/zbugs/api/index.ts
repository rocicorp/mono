// https://vercel.com/templates/other/fastify-serverless-function
import Fastify from 'fastify';
import cookie from '@fastify/cookie';
import oauthPlugin from '@fastify/oauth2';
import 'dotenv/config';
import {Octokit} from '@octokit/core';
import postgres from 'postgres';
import {SignJWT} from 'jose';

import {OAuth2Namespace} from '@fastify/oauth2';

declare module 'fastify' {
  interface FastifyInstance {
    githubOAuth2: OAuth2Namespace;
  }
}

const sql = postgres(process.env.UPSTREAM_URI);

export const fastify = Fastify({
  logger: true,
});

fastify.register(cookie);

fastify.register(oauthPlugin, {
  name: 'githubOAuth2',
  credentials: {
    client: {
      id: process.env.GITHUB_CLIENT_ID,
      secret: process.env.GITHUB_CLIENT_SECRET,
    },
    auth: oauthPlugin.GITHUB_CONFIGURATION,
  },
  startRedirectPath: '/api/login/github',
  callbackUri: req =>
    `${req.protocol}://${req.hostname}:${req.port}/api/login/github/callback`,
});

fastify.get('/api', async (_req, reply) => {
  return reply.status(200).send({hello: 'world'});
});

// The service provider redirect the user here after successful login
fastify.get('/api/login/github/callback', async function (request, reply) {
  const {token} =
    await this.githubOAuth2.getAccessTokenFromAuthorizationCodeFlow(request);

  const octokit = new Octokit({
    auth: token.access_token,
  });

  const userDetails = await octokit.request('GET /user', {
    headers: {
      'X-GitHub-Api-Version': '2022-11-28',
    },
  });

  await sql`INSERT INTO "user"
    ("id", "login", "name", "avatar") VALUES (
      ${userDetails.data.id},
      ${userDetails.data.login},
      ${userDetails.data.name},
      ${userDetails.data.avatar_url}
    ) ON CONFLICT ("id") DO NOTHING`;

  const jwtPayload = {
    sub: userDetails.data.id.toString(),
    iat: Math.floor(Date.now() / 1000),
    name: userDetails.data.login,
  };

  const jwt = await new SignJWT(jwtPayload)
    .setProtectedHeader({alg: 'HS256'})
    .setExpirationTime('30days')
    .sign(new TextEncoder().encode(process.env.JWT_SECRET));

  reply
    .cookie('jwt', jwt, {
      path: '/',
    })
    .redirect('/');
});

export default async function handler(req, reply) {
  await fastify.ready();
  fastify.server.emit('request', req, reply);
}
