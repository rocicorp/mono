import {useMemo} from 'react';
import type {Order} from './issue.js';
import {IssueWithLabelsQuery} from './queries.js';

export type IssuesProps = {
  query: IssueWithLabelsQuery;
  queryDeps: readonly unknown[];
  order: Order;
};

export function useIssuesProps(
  query: IssueWithLabelsQuery,
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
