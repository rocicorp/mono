import {useMemo} from 'react';
import {useQuery} from './hooks/use-query.js';
import {
  orderQuery,
  type Issue,
  type IssueWithLabels,
  type Priority,
  type Status,
} from './issue';
import type {IssuesProps} from './issues-props.js';

export type ListData = {
  getIssue(index: number): IssueWithLabels | undefined;
  mustGetIssue(index: number): IssueWithLabels;
  iterateIssuesAfter(issue: Issue): Iterable<IssueWithLabels>;
  iterateIssuesBefore(issue: Issue): Iterable<IssueWithLabels>;
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

  getIssue(index: number): IssueWithLabels | undefined {
    return this.#issues[index];
  }

  mustGetIssue(index: number): IssueWithLabels {
    if (index < 0 || index >= this.#issues.length) {
      throw new Error(`Invalid index: ${index}`);
    }
    return this.#issues[index];
  }

  #findIndex(issue: Issue): number {
    const issueID = issue.id;
    return this.#issues.findIndex(issue => issue.issue.id === issueID);
  }

  *iterateIssuesAfter(issue: Issue): Iterable<IssueWithLabels> {
    const index = this.#findIndex(issue);
    if (index === -1) {
      return;
    }
    for (let i = index + 1; i < this.#issues.length; i++) {
      yield this.#issues[i];
    }
  }

  *iterateIssuesBefore(issue: Issue): Iterable<IssueWithLabels> {
    const index = this.#findIndex(issue);
    for (let i = index - 1; i >= 0; i--) {
      yield this.#issues[i];
    }
  }
}
