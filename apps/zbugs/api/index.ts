// https://vercel.com/templates/other/fastify-serverless-function
import cookie from '@fastify/cookie';
import oauthPlugin, {type OAuth2Namespace} from '@fastify/oauth2';
import {Octokit} from '@octokit/core';
import '@dotenvx/dotenvx/config';
import Fastify, {type FastifyReply, type FastifyRequest} from 'fastify';
import {jwtVerify, SignJWT, type JWK} from 'jose';
import {nanoid} from 'nanoid';
import postgres from 'postgres';
import {handlePush} from '../server/push-handler.ts';
import type {ReadonlyJSONObject} from '@rocicorp/zero';
import {must} from '../../../packages/shared/src/must.ts';
import assert from 'assert';
import {authDataSchema, type AuthData} from '../shared/auth.ts';

declare module 'fastify' {
  interface FastifyInstance {
    githubOAuth2: OAuth2Namespace;
  }
}

const sql = postgres(process.env.ZERO_UPSTREAM_DB as string);
type QueryParams = {redirect?: string | undefined};
let privateJwk: JWK | undefined;

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
    }/api/login/github/callback${
      (req.query as QueryParams).redirect
        ? `?redirect=${(req.query as QueryParams).redirect}`
        : ''
    }`,
});

fastify.get<{
  Querystring: QueryParams;
}>('/api/login/github/callback', async function (request, reply) {
  if (!privateJwk) {
    privateJwk = JSON.parse(process.env.PRIVATE_JWK as string) as JWK;
  }
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

  const userRows = await sql`SELECT * FROM "user" WHERE "id" = ${userId}`;

  const jwtPayload: AuthData = {
    sub: userId,
    iat: Math.floor(Date.now() / 1000),
    role: userRows[0].role,
    name: userDetails.data.login,
    exp: 0, // setExpirationTime below sets it
  };

  const jwt = await new SignJWT(jwtPayload)
    .setProtectedHeader({alg: must(privateJwk.alg)})
    .setExpirationTime('30days')
    .sign(privateJwk);

  reply
    .cookie('jwt', jwt, {
      path: '/',
      expires: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000),
    })
    .redirect(
      request.query.redirect ? decodeURIComponent(request.query.redirect) : '/',
    );
});

fastify.post<{
  Querystring: {
    schema: string;
    appID: string;
  };
}>('/api/push', async function (request, reply) {
  let {authorization} = request.headers;
  if (authorization !== undefined) {
    assert(authorization.toLowerCase().startsWith('bearer '));
    authorization = authorization.substring('Bearer '.length);
  }

  const jwk = process.env.VITE_PUBLIC_JWK;
  const authData: AuthData | undefined =
    jwk && authorization
      ? authDataSchema.parse(
          (await jwtVerify(authorization, JSON.parse(jwk))).payload,
        )
      : undefined;

  const response = handlePush(
    authData,
    request.query,
    request.body as ReadonlyJSONObject,
  );
  reply.send(response);
});

export default async function handler(
  req: FastifyRequest,
  reply: FastifyReply,
) {
  await fastify.ready();
  fastify.server.emit('request', req, reply);
}
