import type {IncomingHttpHeaders} from 'http';
import type {FastifyInstance, FastifyReply} from 'fastify';
import type {JWTData, Role} from '../shared/auth.ts';
import {getIDFromString} from '../shared/issue-id.ts';
import {applyIssuePermissions} from '../shared/queries.ts';
import {builder} from '../shared/schema.ts';
import {dbProvider} from './db.ts';
import {issueMdPath, renderErrorMd, renderIssueMd} from './machine-md.ts';

/**
 * The markdown view caps huge (synthetic) threads at the most recent comments
 * rather than rendering unboundedly.
 */
const MAX_COMMENTS = 1000;

type AuthFn = (headers: IncomingHttpHeaders) => Promise<JWTData | undefined>;

// Returns undefined for ids that cannot possibly match (empty, or digit
// strings beyond the safe-integer range).
function parseIssueID(idStr: string) {
  if (idStr === '') {
    return undefined;
  }
  const parsed = getIDFromString(idStr);
  return parsed.idField === 'shortID' && !Number.isSafeInteger(parsed.idValue)
    ? undefined
    : parsed;
}

// The issue tree the markdown view renders. This is issueDetail minus the
// per-user relationships (viewState, notificationState) and the comment
// preload: the markdown view has no per-user state, and comments are fetched
// separately below. Uses the same applyIssuePermissions gate as issueDetail.
export function issueForMdQuery(
  idField: 'shortID' | 'id',
  idValue: string | number,
  role: Role | undefined,
) {
  return applyIssuePermissions(
    builder.issue
      .where(idField, idValue)
      .related('project')
      .related('creator')
      .related('assignee')
      .related('labels')
      .related('emoji', e => e.related('creator')),
    role,
  ).one();
}

function fetchIssueForMd(
  authData: JWTData | undefined,
  idField: 'shortID' | 'id',
  idValue: string | number,
) {
  return dbProvider.transaction(async tx => {
    const issue = await tx.run(
      issueForMdQuery(idField, idValue, authData?.role),
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
      comments: commentsDesc.toReversed(),
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
          request.params.projectName.toLowerCase() !==
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
