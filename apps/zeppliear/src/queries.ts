import {QueryRowType, Zero} from 'zero-client';
import {Schema} from './schema.js';
import {Order, orderQuery} from './issue.js';

export type IssueListQuery = ReturnType<typeof getIssueListQuery>;
export type IssueListRow = QueryRowType<IssueListQuery>;
export function getIssueListQuery(zero: Zero<Schema>) {
  return zero.query.issue
    .related('labels')
    .related('comments', q => q.related('creator').limit(10));
}

export function getIssuePreloadQuery(
  zero: Zero<Schema>,
  sort: 'modified' | 'created' | 'priority' | 'status',
) {
  return zero.query.issue.related('labels').orderBy(sort, 'desc').limit(1_000);
}

export const crewNames = ['holden', 'naomi', 'alex', 'amos', 'bobbie'];
export function getCrewQuery(zero: Zero<Schema>) {
  return zero.query.member.where('name', 'IN', crewNames);
}

type IssueDetailQuery = ReturnType<typeof getIssueDetailQuery>;
export type IssueWithDetails = QueryRowType<IssueDetailQuery>;
export function getIssueDetailQuery(
  zero: Zero<Schema>,
  issueID: string | null,
) {
  return zero.query.issue
    .related('comments', q => q.related('creator'))
    .where('id', '=', issueID ?? '');
}

export function getNextIssueQuery(
  q: IssueListQuery,
  issue: IssueWithDetails | null,
  order: Order,
  direction: 'fwd' | 'prev',
) {
  return issue
    ? orderQuery(q, order, direction === 'prev')
        .start(issue)
        .limit(1)
    : undefined;
}
