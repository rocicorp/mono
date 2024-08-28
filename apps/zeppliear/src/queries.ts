import {QueryRowType, Zero} from 'zero-client';
import {Schema} from './schema.js';

export type IssueListQuery = ReturnType<typeof getIssueListQuery>;
export type IssueListRow = QueryRowType<ReturnType<typeof getIssueListQuery>>;
export function getIssueListQuery(zero: Zero<Schema>) {
  return zero.query.issue
    .select(
      'created',
      'creatorID',
      'description',
      'id',
      'kanbanOrder',
      'modified',
      'priority',
      'status',
      'title',
    )
    .related('labels', q => q.select('id', 'name'))
    .related('comments', q =>
      q
        .select('id', 'body', 'created', 'creatorID')
        .related('creator', q => q.select('id', 'name'))
        .limit(10),
    );
}

export function getIssuePreloadQuery(
  zero: Zero<Schema>,
  sort: 'modified' | 'created' | 'priority' | 'status',
) {
  return zero.query.issue
    .select(
      'created',
      'creatorID',
      'description',
      'id',
      'kanbanOrder',
      'modified',
      'priority',
      'status',
      'title',
    )
    .related('labels', q => q.select('id', 'name'))
    .orderBy(sort, 'desc')
    .limit(2000);
}

export const crewNames = ['holden', 'naomi', 'alex', 'amos', 'bobbie'];
export function getCrewQuery(zero: Zero<Schema>) {
  return zero.query.member.select('id', 'name').where('name', 'IN', crewNames);
}

export type IssueWithDetails = QueryRowType<
  ReturnType<typeof getIssueDetailQuery>
>;

export function getIssueDetailQuery(
  zero: Zero<Schema>,
  issueID: string | null,
) {
  return (
    zero.query.issue
      .select(
        'created',
        'creatorID',
        'description',
        'id',
        'kanbanOrder',
        'modified',
        'priority',
        'status',
        'title',
      )
      // labels?
      // owner?
      .related('comments', q =>
        q
          .select('id', 'body', 'created')
          .related('creator', q => q.select('name')),
      )
      .where('id', '=', issueID ?? '')
  );
}
