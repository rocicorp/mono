import {QueryRowType, Zero} from 'zero-client';
import {Schema} from './schema.js';

export type IssueListQuery = ReturnType<typeof getIssueListQuery>;
export type IssueListRow = QueryRowType<IssueListQuery>;
export function getIssueListQuery(zero: Zero<Schema>) {
  return zero.query.issue
    .related('labels', q => q)
    .related('comments', q => q.related('creator', q => q).limit(10));
}

export function getIssuePreloadQuery(
  zero: Zero<Schema>,
  sort: 'modified' | 'created' | 'priority' | 'status',
) {
  return zero.query.issue
    .related('labels', q => q)
    .orderBy(sort, 'desc')
    .limit(10_000);
}

export const crewNames = ['holden', 'naomi', 'alex', 'amos', 'bobbie'];
export function getCrewQuery(zero: Zero<Schema>) {
  return zero.query.member.where('name', 'IN', crewNames);
}

export type IssueWithDetails = QueryRowType<
  ReturnType<typeof getIssueDetailQuery>
>;

export function getIssueDetailQuery(
  zero: Zero<Schema>,
  issueID: string | null,
) {
  return zero.query.issue
    .related('comments', q => q.related('creator', q => q))
    .where('id', '=', issueID ?? '');
}
