// https://vercel.com/templates/other/fastify-serverless-function

import '../../../packages/shared/src/dotenv.ts';

import cookie from '@fastify/cookie';
import oauthPlugin, {type OAuth2Namespace} from '@fastify/oauth2';
import {Octokit} from '@octokit/core';
import {
  mustGetMutator,
  mustGetQuery,
  type ReadonlyJSONValue,
} from '@rocicorp/zero';
import {handleMutateRequest, handleQueryRequest} from '@rocicorp/zero/server';
import Fastify, {type FastifyReply, type FastifyRequest} from 'fastify';
import type {IncomingHttpHeaders} from 'http';
import {jwtVerify, SignJWT, type JWK} from 'jose';
import {nanoid} from 'nanoid';
import {assert} from '../../../packages/shared/src/asserts.ts';
import {must} from '../../../packages/shared/src/must.ts';
import {dbProvider, sql} from '../server/db.ts';
import {createServerMutators} from '../server/server-mutators.ts';
import {jwtDataSchema, type JWTData} from '../shared/auth.ts';
import {queries} from '../shared/queries.ts';
import {schema} from '../shared/schema.ts';
import {getPresignedUrl} from '../src/server/upload.ts';

declare module 'fastify' {
  interface FastifyInstance {
    githubOAuth2: OAuth2Namespace;
  }
}

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
    // @ts-ignore Not clear why this is not working when type checking with tsconfig.node.ts
    auth: oauthPlugin.GITHUB_CONFIGURATION,
  },
  startRedirectPath: '/api/login/github',
  callbackUri: req =>
    `${req.hostname.includes('localhost') ? 'http' : 'https'}://${req.hostname}${
      // oxlint-disable-next-line eqeqeq
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
  const existingUser =
    await sql`SELECT id, email FROM "user" WHERE "githubID" = ${userDetails.data.id}`;
  if (existingUser.length > 0) {
    userId = existingUser[0].id;
    // update email on login if it has changed
    if (existingUser[0].email !== userDetails.data.email) {
      await sql`UPDATE "user" SET "email" = ${userDetails.data.email} WHERE "id" = ${userId}`;
    }
  } else {
    await sql`INSERT INTO "user"
      ("id", "login", "name", "avatar", "githubID", "email") VALUES (
        ${userId},
        ${userDetails.data.login},
        ${userDetails.data.name},
        ${userDetails.data.avatar_url},
        ${userDetails.data.id},
        ${userDetails.data.email}
      )`;
  }

  const userRows = await sql`SELECT * FROM "user" WHERE "id" = ${userId}`;

  const jwtPayload: JWTData = {
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

async function withAuth<T extends {headers: IncomingHttpHeaders}>(
  request: T,
  reply: FastifyReply,
  handler: (authData: JWTData | undefined) => Promise<void>,
) {
  let authData: JWTData | undefined;
  try {
    authData = await maybeVerifyAuth(request.headers);
  } catch (e) {
    if (e instanceof Error) {
      reply.status(401).send(e.message);
      return;
    }
    throw e;
  }

  await handler(authData);
}

fastify.post<{
  Querystring: Record<string, string>;
  Body: ReadonlyJSONValue;
}>('/api/push', mutateHandler);

fastify.post<{
  Querystring: Record<string, string>;
  Body: ReadonlyJSONValue;
}>('/api/mutate', mutateHandler);

async function mutateHandler(
  request: FastifyRequest<{
    Querystring: Record<string, string>;
    Body: ReadonlyJSONValue;
  }>,
  reply: FastifyReply,
) {
  let jwtData: JWTData | undefined;
  try {
    jwtData = await maybeVerifyAuth(request.headers);
  } catch (e) {
    if (e instanceof Error) {
      reply.status(401).send(e.message);
      return;
    }
    throw e;
  }

  const postCommitTasks: (() => Promise<void>)[] = [];
  const mutators = createServerMutators(postCommitTasks);

  const response = await handleMutateRequest(
    dbProvider,
    (transact, _mutation) =>
      transact((tx, name, args) => {
        const mutator = mustGetMutator(mutators, name);
        return mutator.fn({tx, args, ctx: jwtData});
      }),
    request.query,
    request.body,
    'info',
  );

  // we don't yet handle errors here, since Loops emails return 429 very often
  // and we don't want to block the mutation
  await Promise.allSettled(postCommitTasks.map(task => task()));

  reply.send(response);
}

// this endpoint was kept for backwards compatibility
// in the transition to /query
fastify.post<{
  Querystring: Record<string, string>;
  Body: ReadonlyJSONValue;
}>('/api/get-queries', queryHandler);

fastify.post<{
  Querystring: Record<string, string>;
  Body: ReadonlyJSONValue;
}>('/api/query', queryHandler);

async function queryHandler(
  request: FastifyRequest<{
    Querystring: Record<string, string>;
    Body: ReadonlyJSONValue;
  }>,
  reply: FastifyReply,
) {
  await withAuth(request, reply, async authData => {
    reply.send(
      await handleQueryRequest(
        (name: string, args: ReadonlyJSONValue | undefined) => {
          const query = mustGetQuery(queries, name);
          return query.fn({args, ctx: authData});
        },
        schema,
        request.body,
      ),
    );
  });
}

fastify.post<{
  Body: {contentType: string};
}>('/api/upload/presigned-url', async (request, reply) => {
  await withAuth(request, reply, async authData => {
    if (!authData) {
      reply.status(401).send('Authentication required');
      return;
    }

    try {
      const result = await getPresignedUrl(request.body.contentType);
      reply.send(result);
    } catch (error) {
      if (error instanceof Error) {
        reply.status(500).send(error.message);
        return;
      }
      reply.status(500).send('Failed to generate presigned URL');
    }
  });
});

fastify.get<{
  Querystring: {id: string; email: string};
}>('/api/unsubscribe', async (request, reply) => {
  if (!request.query.email) {
    reply.status(400).send('Email is required');
    return;
  }

  // Look up the actual issue ID from the shortID
  const shortID = parseInt(request.query.id);

  if (isNaN(shortID)) {
    reply.status(400).send('Invalid issue ID');
    return;
  }

  const existingUserResult =
    await sql`SELECT id, email FROM "user" WHERE "email" = ${request.query.email}`;

  const existingUser = existingUserResult[0];

  if (!existingUser) {
    reply.status(401).send('Unauthorized');
    return;
  }

  const issueResult =
    await sql`SELECT id, title FROM "issue" WHERE "shortID" = ${shortID}`;

  const issue = issueResult[0];

  if (!issue) {
    reply.status(404).send('Issue not found');
    return;
  }

  await sql`INSERT INTO "issueNotifications" ("userID", "issueID", "subscribed") 
    VALUES (${existingUser.id}, ${issue.id}, false)
    ON CONFLICT ("userID", "issueID") 
    DO UPDATE SET "subscribed" = false`;
  reply
    .type('text/html')
    .send(
      `OK! You are unsubscribed from <a href="https://bugs.rocicorp.dev/issue/${shortID}">${issue.title}</a>.`,
    );
});

async function maybeVerifyAuth(
  headers: IncomingHttpHeaders,
): Promise<JWTData | undefined> {
  let {authorization} = headers;
  if (!authorization) {
    return undefined;
  }

  assert(authorization.toLowerCase().startsWith('bearer '));
  authorization = authorization.substring('Bearer '.length);

  const jwk = process.env.VITE_PUBLIC_JWK;
  if (!jwk) {
    throw new Error('VITE_PUBLIC_JWK is not set');
  }

  return jwtDataSchema.parse(
    (await jwtVerify(authorization, JSON.parse(jwk))).payload,
  );
}

export default async function handler(
  req: FastifyRequest,
  reply: FastifyReply,
) {
  await fastify.ready();
  fastify.server.emit('request', req, reply);
}
