// https://vercel.com/templates/other/fastify-serverless-function
import cookie from '@fastify/cookie';
import oauthPlugin, {type OAuth2Namespace} from '@fastify/oauth2';
import {Octokit} from '@octokit/core';
import 'dotenv/config';
import Fastify, {type FastifyReply, type FastifyRequest} from 'fastify';
import {SignJWT} from 'jose';
import {nanoid} from 'nanoid';
import postgres from 'postgres';

declare module 'fastify' {
  interface FastifyInstance {
    githubOAuth2: OAuth2Namespace;
  }
}

const sql = postgres(process.env.UPSTREAM_URI as string);
type QueryParams = {redirect: string};

export const fastify = Fastify({
  logger: true,
});

fastify.register(cookie);

fastify.register(oauthPlugin, {
  name: 'githubOAuth2',
  credentials: {
    client: {
      id: process.env.GITHUB_CLIENT_ID as string,
      secret: process.env.GITHUB_CLIENT_SECRET as string,
    },
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore Not clear why this is not working when type checking with tsconfig.node.ts
    auth: oauthPlugin.GITHUB_CONFIGURATION,
  },
  startRedirectPath: '/api/login/github',
  callbackUri: req =>
    `${req.protocol}://${req.hostname}${
      req.port != null ? ':' + req.port : ''
    }/api/login/github/callback?redirect=${
      (req.query as QueryParams).redirect
    }`,
});

fastify.get<{
  Querystring: QueryParams;
}>('/api/login/github/callback', async function (request, reply) {
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

  let userId = nanoid();
  const existingUserId =
    await sql`SELECT id FROM "user" WHERE "githubID" = ${userDetails.data.id}`;
  if (existingUserId.length > 0) {
    userId = existingUserId[0].id;
  } else {
    await sql`INSERT INTO "user"
    ("id", "login", "name", "avatar", "githubID") VALUES (
      ${userId},
      ${userDetails.data.login},
      ${userDetails.data.name},
      ${userDetails.data.avatar_url},
      ${userDetails.data.id}
    )`;
  }

  const jwtPayload = {
    sub: userId,
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
    .redirect(
      request.query.redirect ? decodeURIComponent(request.query.redirect) : '/',
    );
});

export default async function handler(
  req: FastifyRequest,
  reply: FastifyReply,
) {
  await fastify.ready();
  fastify.server.emit('request', req, reply);
}
