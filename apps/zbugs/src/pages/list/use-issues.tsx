import type {
  DefaultContext,
  DefaultSchema,
  QueryOrQueryRequest,
} from '@rocicorp/zero';
import {useQuery, type UseQueryOptions} from '@rocicorp/zero/react';
import {assert} from 'shared/src/asserts.ts';

export type Anchor<TIssueRowSort> =
  | Readonly<{
      index: number;
      kind: 'forward';
      startRow?: TIssueRowSort | undefined;
    }>
  | Readonly<{
      index: number;
      kind: 'backward';
      startRow: TIssueRowSort;
    }>
  | Readonly<{
      index: number;
      kind: 'permalink';
      id: string;
    }>;

export type GetPageQuery<TIssue, TIssueRowSort> = (
  limit: number,
  start: TIssueRowSort | null,
  dir: 'forward' | 'backward',
) => GetQueryReturnType<TIssue>;

export type GetSingleQuery<TIssue> = (
  id: string,
) => GetQueryReturnType<TIssue | undefined>;

export type GetQueryReturnType<TReturn> = QueryOrQueryRequest<
  keyof DefaultSchema['tables'],
  // oxlint-disable-next-line no-explicit-any
  any, // input
  // oxlint-disable-next-line no-explicit-any
  any, // output
  DefaultSchema,
  TReturn,
  DefaultContext
>;

export function useIssues<TIssue, TIssueRowSort>({
  pageSize,
  anchor,
  options,
  getPageQuery,
  getSingleQuery,
  toStartRow,
}: {
  pageSize: number;
  anchor: Anchor<TIssueRowSort>;
  options?: UseQueryOptions | undefined;

  getPageQuery: GetPageQuery<TIssue, TIssueRowSort>;
  getSingleQuery: GetSingleQuery<TIssue>;
  toStartRow: (row: TIssue) => TIssueRowSort;
}): {
  issueAt: (index: number) => TIssue | undefined;
  issuesLength: number;
  complete: boolean;
  issuesEmpty: boolean;
  atStart: boolean;
  atEnd: boolean;
  firstIssueIndex: number;
  permalinkNotFound: boolean;
} {
  const {kind, index: anchorIndex} = anchor;
  let permalinkNotFound = false;

  if (kind === 'permalink') {
    const {id} = anchor;
    assert(id);
    assert(pageSize % 2 === 0);

    const halfPageSize = pageSize / 2;

    const qItem = getSingleQuery(id);

    const [issue, resultIssue] = useQuery(qItem, options);
    const completeIssue = resultIssue.type === 'complete';

    const typedIssue = issue as TIssue | undefined;
    const start = typedIssue && toStartRow(typedIssue);

    const qBefore = start && getPageQuery(halfPageSize + 1, start, 'backward');
    const qAfter = start && getPageQuery(halfPageSize, start, 'forward');

    const [issuesBefore, resultBefore] = useQuery(qBefore, options);
    const [issuesAfter, resultAfter] = useQuery(qAfter, options);
    const completeBefore = resultBefore.type === 'complete';
    const completeAfter = resultAfter.type === 'complete';

    const typedIssuesBefore = issuesBefore as unknown as TIssue[] | undefined;
    const typedIssuesAfter = issuesAfter as unknown as TIssue[] | undefined;
    const issuesBeforeLength = typedIssuesBefore?.length ?? 0;
    const issuesAfterLength = typedIssuesAfter?.length ?? 0;
    const issuesBeforeSize = Math.min(issuesBeforeLength, halfPageSize);
    const issuesAfterSize = Math.min(issuesAfterLength, halfPageSize - 1);

    const firstIssueIndex = anchorIndex - issuesBeforeSize;

    if (completeIssue && typedIssue === undefined) {
      // Permalink issue not found
      permalinkNotFound = true;
    }

    return {
      issueAt: (index: number) => {
        if (index === anchorIndex) {
          return typedIssue;
        }
        if (index > anchorIndex) {
          if (typedIssuesAfter === undefined) {
            return undefined;
          }
          const i = index - anchorIndex - 1;
          if (i >= issuesAfterSize) {
            return undefined;
          }
          return typedIssuesAfter[i];
        }
        assert(index < anchorIndex);
        if (typedIssuesBefore === undefined) {
          return undefined;
        }
        const i = anchorIndex - index - 1;
        if (i >= issuesBeforeSize) {
          return undefined;
        }
        return typedIssuesBefore[i];
      },
      issuesLength: issuesBeforeSize + issuesAfterSize + (typedIssue ? 1 : 0),
      complete: completeIssue && completeBefore && completeAfter,
      issuesEmpty:
        typedIssue === undefined ||
        (issuesBeforeSize === 0 && issuesAfterSize === 0),
      atStart: completeBefore && issuesBeforeLength <= halfPageSize,
      atEnd: completeAfter && issuesAfterLength <= halfPageSize - 1,
      firstIssueIndex,
      permalinkNotFound,
    };
  }

  kind satisfies 'forward' | 'backward';

  const {startRow: start = null} = anchor;

  const q = getPageQuery(pageSize + 1, start, kind);

  const [issues, result] = useQuery(q, options);
  // not used but needed to follow rules of hooks
  void useQuery(null, options);
  void useQuery(null, options);

  const typedIssues = issues as unknown as TIssue[];
  const complete = result.type === 'complete';
  const hasMoreIssues = typedIssues.length > pageSize;
  const issuesLength = hasMoreIssues ? pageSize : typedIssues.length;
  const issuesEmpty = typedIssues.length === 0;

  if (kind === 'forward') {
    return {
      issueAt: (index: number) =>
        index - anchorIndex < issuesLength
          ? typedIssues[index - anchorIndex]
          : undefined,
      issuesLength,
      complete,
      issuesEmpty,
      atStart: start === null || anchorIndex === 0,
      atEnd: complete && !hasMoreIssues,
      firstIssueIndex: anchorIndex,
      permalinkNotFound,
    };
  }

  kind satisfies 'backward';
  assert(start !== null);

  return {
    issueAt: (index: number) => {
      if (anchorIndex - index - 1 >= issuesLength) {
        return undefined;
      }
      return typedIssues[anchorIndex - index - 1];
    },
    issuesLength,
    complete,
    issuesEmpty,
    atStart: complete && !hasMoreIssues,
    atEnd: false,
    firstIssueIndex: anchorIndex - issuesLength,
    permalinkNotFound,
  };
}
