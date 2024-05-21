import {useMemo} from 'react';
import type {IssueQuery, Order} from './issue.js';

export type IssuesProps = {
  query: IssueQuery;
  queryDeps: readonly unknown[];
  order: Order;
};

export function useIssuesProps(
  query: IssueQuery,
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
