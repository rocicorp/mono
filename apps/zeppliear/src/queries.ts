import {QueryRowType, Zero} from 'zero-client';
import {Schema} from './schema.js';

export type IssueWithLabels = QueryRowType<
  ReturnType<typeof getIssueWithLabelsQuery>
>;
export type IssueWithLabelsQuery = ReturnType<typeof getIssueWithLabelsQuery>;

export function getIssueWithLabelsQuery(zero: Zero<Schema>) {
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
    .related('labels', q => q.select('id', 'name'));
}

export const crewNames = ['holden', 'naomi', 'alex', 'amos', 'bobbie'];
export function getCrewQuery(zero: Zero<Schema>) {
  return zero.query.member.select('id', 'name').where('name', 'IN', crewNames);
}

export type IssueWithDetails = QueryRowType<
  ReturnType<typeof getIssueDetailQuery>
>;

export function getIssueDetailQuery(zero: Zero<Schema>, issueID: string) {
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
    .related('comments', q =>
      q
        .select('id', 'body', 'created')
        .related('creator', q => q.select('name')),
    )
    .where('id', '=', issueID);
}
