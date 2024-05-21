import {useMemo} from 'react';
import {useQuery} from './hooks/use-zql.js';
import {
  orderQuery,
  type Issue,
  type IssueWithLabels,
  type Priority,
  type Status,
} from './issue';
import type {IssuesProps} from './issues-props.js';

export type ListData = {
  getIssue(index: number): IssueWithLabels;
  readonly onChangePriority: (issue: Issue, priority: Priority) => void;
  readonly onChangeStatus: (issue: Issue, status: Status) => void;
  readonly onOpenDetail: (issue: Issue) => void;
  readonly count: number;
};

export function useListData({
  issuesProps,
  onChangePriority,
  onChangeStatus,
  onOpenDetail,
  limit = 200,
}: {
  issuesProps: IssuesProps;

  onChangePriority: (issue: Issue, priority: Priority) => void;
  onChangeStatus: (issue: Issue, status: Status) => void;
  onOpenDetail: (issue: Issue) => void;
  limit?: number;
}): ListData {
  const {query, queryDeps, order} = issuesProps;
  const issueQueryOrdered = orderQuery(query, order, false);
  const issues = useQuery(issueQueryOrdered.limit(limit), queryDeps);

  return useMemo(
    () =>
      new ListDataImpl(issues, onChangePriority, onChangeStatus, onOpenDetail),
    [issues, onChangePriority, onChangeStatus, onOpenDetail],
  );
}

class ListDataImpl implements ListData {
  readonly #issues: readonly IssueWithLabels[];
  readonly onChangePriority: (issue: Issue, priority: Priority) => void;
  readonly onChangeStatus: (issue: Issue, status: Status) => void;
  readonly onOpenDetail: (issue: Issue) => void;
  readonly count: number;

  constructor(
    issues: IssueWithLabels[],
    onChangePriority: (issue: Issue, priority: Priority) => void,
    onChangeStatus: (issue: Issue, status: Status) => void,
    onOpenDetail: (issue: Issue) => void,
  ) {
    this.#issues = issues;
    this.onChangePriority = onChangePriority;
    this.onChangeStatus = onChangeStatus;
    this.onOpenDetail = onOpenDetail;
    this.count = issues.length;
  }

  getIssue(index: number): IssueWithLabels {
    return this.#issues[index];
  }
}
