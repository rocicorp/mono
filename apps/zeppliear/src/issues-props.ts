import {useMemo} from 'react';
import type {Order} from './issue.js';
import {IssueListQuery} from './queries.js';

export type IssuesProps = {
  query: IssueListQuery;
  queryDeps: readonly unknown[];
  order: Order;
};

export function useIssuesProps(
  query: IssueListQuery,
  queryDeps: readonly unknown[],
  order: Order,
): IssuesProps {
  return useMemo(
    () => ({
      query,
      queryDeps,
      order,
    }),
    [query, queryDeps, order],
  );
}
