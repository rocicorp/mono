import type {AnyQuery, ReadonlyJSONValue, Row} from '@rocicorp/zero';
import type {Schema} from '../shared/schema.ts';
import type {Role} from '../shared/auth.ts';
import type {ListContext} from '../shared/queries.ts';
import {queries as sharedQueries} from '../shared/queries.ts';
import {applyIssuePermissions} from './read-permissions.ts';

export type ServerContext = {
  role: Role | undefined;
};

export type ServerQuery = (
  context: ServerContext,
  ...args: readonly ReadonlyJSONValue[]
) => AnyQuery;

export function issuePreload(c: ServerContext, userID: string) {
  return applyIssuePermissions(sharedQueries.issuePreload(userID), c.role);
}

export function prevNext(
  c: ServerContext,
  listContext: ListContext['params'] | null,
  issue: Row<Schema['tables']['issue']> | null,
  dir: 'next' | 'prev',
) {
  return applyIssuePermissions(
    sharedQueries.prevNext(listContext, issue, dir),
    c.role,
  );
}

export function issueList(
  c: ServerContext,
  listContext: ListContext['params'],
  userID: string,
  limit: number,
) {
  return applyIssuePermissions(
    sharedQueries.issueList(listContext, userID, limit),
    c.role,
  );
}

export function issueDetail(
  c: ServerContext,
  idField: 'shortID' | 'id',
  id: string | number,
  userID: string,
) {
  return applyIssuePermissions(
    sharedQueries.issueDetail(idField, id, userID),
    c.role,
  );
}
