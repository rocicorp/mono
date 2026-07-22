import type {IncomingHttpHeaders} from 'http';
import type {FastifyInstance, FastifyReply} from 'fastify';
import type {JWTData} from '../shared/auth.ts';
import {queries} from '../shared/queries.ts';
import {builder} from '../shared/schema.ts';
import {dbProvider} from './db.ts';
import {issueMdPath, renderErrorMd, renderIssueMd} from './machine-md.ts';

/**
 * The markdown view caps huge (synthetic) threads at the most recent comments
 * rather than rendering unboundedly.
 */
const MAX_COMMENTS = 1000;

type AuthFn = (headers: IncomingHttpHeaders) => Promise<JWTData | undefined>;

// Mirrors getIDFromString in src/pages/issue/get-id.tsx, which is not imported
// here because it is part of the client bundle. Returns undefined for ids that
// cannot possibly match (empty, or digits beyond the integer range).
function parseIssueID(idStr: string) {
  if (idStr === '') {
    return undefined;
  }
  if (/[^\d]/.test(idStr)) {
    return {idField: 'id', idValue: idStr} as const;
  }
  const shortID = parseInt(idStr);
  return Number.isSafeInteger(shortID)
    ? ({idField: 'shortID', idValue: shortID} as const)
    : undefined;
}

async function fetchIssueForMd(
  authData: JWTData | undefined,
  idField: 'shortID' | 'id',
  idValue: string | number,
) {
  return dbProvider.transaction(async tx => {
    const issue = await tx.run(
      queries.issueDetail.fn({args: {idField, id: idValue}, ctx: authData}),
    );
    if (!issue) {
      return undefined;
    }
    // The full thread rather than issueDetail's 50-comment preload. This
    // query has no visibility gate of its own, which is safe only because it
    // runs after the permission-gated issueDetail query above returned the
    // issue, and never for a synced query.
    const commentsDesc = await tx.run(
      builder.comment
        .where('issueID', issue.id)
        .orderBy('created', 'desc')
        .orderBy('id', 'desc')
        .related('creator')
        .related('emoji', e => e.related('creator'))
        .limit(MAX_COMMENTS),
    );
    return {
      issue,
      comments: commentsDesc.slice().reverse(),
      commentsCapped: commentsDesc.length === MAX_COMMENTS,
    };
  });
}

function sendMarkdown(reply: FastifyReply, body: string): void {
  reply.type('text/markdown; charset=utf-8').send(body);
}

function setCacheControl(
  request: {headers: IncomingHttpHeaders},
  reply: FastifyReply,
  publicDirectives: string,
): void {
  // Responses to authenticated requests may include internal issues and must
  // never be cached by the CDN.
  reply.header(
    'cache-control',
    request.headers.authorization
      ? 'private, no-store'
      : `public, ${publicDirectives}`,
  );
}

export function registerMachineRoutes(
  fastify: FastifyInstance,
  auth: AuthFn,
): void {
  fastify.get<{Params: {projectName: string; id: string}}>(
    '/p/:projectName/issue/:id.md',
    async (request, reply) => {
      let authData: JWTData | undefined;
      try {
        authData = await auth(request.headers);
      } catch (e) {
        reply.status(401).header('cache-control', 'private, no-store');
        sendMarkdown(
          reply,
          renderErrorMd(
            'Unauthorized',
            e instanceof Error ? e.message : 'Invalid authorization header.',
          ),
        );
        return;
      }

      const parsed = parseIssueID(request.params.id);
      const result =
        parsed &&
        (await fetchIssueForMd(authData, parsed.idField, parsed.idValue));
      if (!result) {
        setCacheControl(request, reply, 's-maxage=60');
        reply.status(404);
        sendMarkdown(
          reply,
          renderErrorMd(
            'Not Found',
            'No such issue, or you do not have permission to view it.',
          ),
        );
        return;
      }

      const {issue, comments, commentsCapped} = result;
      if (issue.project) {
        const ref = String(issue.shortID ?? issue.id);
        if (
          request.params.projectName.toLocaleLowerCase() !==
            issue.project.lowerCaseName ||
          request.params.id !== ref
        ) {
          setCacheControl(request, reply, 's-maxage=300');
          reply.redirect(issueMdPath(issue.project.lowerCaseName, ref), 308);
          return;
        }
      }

      setCacheControl(
        request,
        reply,
        's-maxage=300, stale-while-revalidate=3600',
      );
      sendMarkdown(reply, renderIssueMd(issue, comments, {commentsCapped}));
    },
  );
}
