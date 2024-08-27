import {useCallback, useMemo, useState} from 'react';
import type {ListOnItemsRenderedProps} from 'react-window';
import {orderQuery, type Issue, type Priority, type Status} from './issue.js';
import type {IssuesProps} from './issues-props.js';
import {assert} from './util/asserts.js';
import {ResultType} from 'zero-client';
import {useQueryWithResultType} from './hooks/use-query2.js';
import {IssueWithLabels} from './queries.js';

export type ListData = {
  getIssue(index: number): IssueWithLabels | undefined;
  mustGetIssue(index: number): IssueWithLabels;
  isLoadingIndicator(index: number): boolean;
  iterateIssuesAfter(issue: Issue): Iterable<IssueWithLabels>;
  iterateIssuesBefore(issue: Issue): Iterable<IssueWithLabels>;
  onItemsRendered: (props: ListOnItemsRenderedProps) => void;
  readonly onChangePriority: (issue: Issue, priority: Priority) => void;
  readonly onChangeStatus: (issue: Issue, status: Status) => void;
  readonly onOpenDetail: (issue: Issue) => void;
  readonly count: number;
  readonly resultType: ResultType;
};

export function useListData({
  issuesProps,
  onChangePriority,
  onChangeStatus,
  onOpenDetail,
}: {
  issuesProps: IssuesProps;
  onChangePriority: (issue: Issue, priority: Priority) => void;
  onChangeStatus: (issue: Issue, status: Status) => void;
  onOpenDetail: (issue: Issue) => void;
}): ListData {
  const pageSize = 500;
  const {query, queryDeps, order} = issuesProps;
  const issueQueryOrdered = orderQuery(query, order, false);
  const [limit, setLimit] = useState(pageSize);
  const {snapshot: issues, resultType} = useQueryWithResultType(
    issueQueryOrdered.limit(limit),
    queryDeps.concat(limit),
  );
  const onItemsRendered = useCallback(
    ({overscanStopIndex}: ListOnItemsRenderedProps) => {
      if (overscanStopIndex > limit - pageSize / 2) {
        setLimit(Math.ceil(overscanStopIndex / pageSize + 1) * pageSize);
      }
    },
    [limit],
  );

  return useMemo(
    () =>
      new ListDataImpl(
        issues,
        onChangePriority,
        onChangeStatus,
        onOpenDetail,
        onItemsRendered,
        resultType,
      ),
    [
      issues,
      onChangePriority,
      onChangeStatus,
      onItemsRendered,
      onOpenDetail,
      resultType,
    ],
  );
}

class ListDataImpl implements ListData {
  readonly #issues: readonly IssueWithLabels[];
  readonly onChangePriority: (issue: Issue, priority: Priority) => void;
  readonly onChangeStatus: (issue: Issue, status: Status) => void;
  readonly onOpenDetail: (issue: Issue) => void;
  readonly count: number;
  readonly onItemsRendered: (props: ListOnItemsRenderedProps) => void;
  readonly resultType: ResultType;

  constructor(
    issues: IssueWithLabels[],
    onChangePriority: (issue: Issue, priority: Priority) => void,
    onChangeStatus: (issue: Issue, status: Status) => void,
    onOpenDetail: (issue: Issue) => void,
    onItemsRendered: (props: ListOnItemsRenderedProps) => void,
    resultType: ResultType,
  ) {
    this.#issues = issues;
    this.onChangePriority = onChangePriority;
    this.onChangeStatus = onChangeStatus;
    this.onOpenDetail = onOpenDetail;
    this.count = issues.length + (resultType === 'complete' ? 0 : 1);
    this.onItemsRendered = onItemsRendered;
    this.resultType = resultType;
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

  isLoadingIndicator(index: number): boolean {
    if (index === this.#issues.length) {
      assert(this.resultType !== 'complete');
      return true;
    }
    return false;
  }

  #findIndex(issue: Issue): number {
    const issueID = issue.id;
    return this.#issues.findIndex(issue => issue.id === issueID);
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
