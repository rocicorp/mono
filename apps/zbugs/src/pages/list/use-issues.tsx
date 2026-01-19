import type {Row} from '@rocicorp/zero';
import {useQuery, type UseQueryOptions} from '@rocicorp/zero/react';
import {assert} from 'shared/src/asserts.ts';
import * as zod from 'zod/mini';
import {
  issueRowSortSchema,
  type ListContextParams,
  queries,
} from '../../../shared/queries.ts';
import {getIDFromString} from '../issue/get-id.tsx';

export function useIssues(
  listContext: ListContextParams,
  userID: string,
  pageSize: number,
  anchor: Anchor,
  options: UseQueryOptions,
): {
  issueAt: (index: number) => Issue | undefined;
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

    // Allow short ID too.
    const {idField, id: idValue} = getIDFromString(id);

    const qItem = queries.issueByID({idField, id: idValue, listContext});

    const [issue, resultIssue] = useQuery(qItem, options);
    const completeIssue = resultIssue.type === 'complete';

    const start = issue && {
      id: issue.id,
      modified: issue.modified,
      created: issue.created,
    };

    const qBefore =
      start &&
      queries.issueListV2({
        listContext,
        userID,
        limit: halfPageSize + 1,
        start,
        dir: 'backward',
        inclusive: false,
      });
    const qAfter =
      start &&
      queries.issueListV2({
        listContext,
        userID,
        limit: halfPageSize,
        start,
        dir: 'forward',
        inclusive: false,
      });

    const [issuesBefore, resultBefore] = useQuery(qBefore, options);
    const [issuesAfter, resultAfter] = useQuery(qAfter, options);
    const completeBefore = resultBefore.type === 'complete';
    const completeAfter = resultAfter.type === 'complete';

    const issuesBeforeLength = issuesBefore?.length ?? 0;
    const issuesAfterLength = issuesAfter?.length ?? 0;
    const issuesBeforeSize = Math.min(issuesBeforeLength, halfPageSize);
    const issuesAfterSize = Math.min(issuesAfterLength, halfPageSize - 1);

    const firstIssueIndex = anchorIndex - issuesBeforeSize;

    if (completeIssue && issue === undefined) {
      // Permalink issue not found
      permalinkNotFound = true;
    }

    return {
      issueAt: (index: number) => {
        if (index === anchorIndex) {
          return issue;
        }
        if (index > anchorIndex) {
          if (issuesAfter === undefined) {
            return undefined;
          }
          const i = index - anchorIndex - 1;
          if (i >= issuesAfterSize) {
            return undefined;
          }
          return issuesAfter[i];
        }
        assert(index < anchorIndex);
        if (issuesBefore === undefined) {
          return undefined;
        }
        const i = anchorIndex - index - 1;
        if (i >= issuesBeforeSize) {
          return undefined;
        }
        return issuesBefore[i];
      },
      issuesLength: issuesBeforeSize + issuesAfterSize + (issue ? 1 : 0),
      complete: completeIssue && completeBefore && completeAfter,
      issuesEmpty:
        issue === undefined ||
        (issuesBeforeSize === 0 && issuesAfterSize === 0),
      atStart: completeBefore && issuesBeforeLength <= halfPageSize,
      atEnd: completeAfter && issuesAfterLength <= halfPageSize - 1,
      firstIssueIndex,
      permalinkNotFound,
    };
  }

  kind satisfies 'forward' | 'backward';

  const {startRow: start = null} = anchor;

  const q = queries.issueListV2({
    listContext,
    userID,
    limit: pageSize + 1,
    start,
    dir: kind,
    inclusive: start === null,
  });
  const [issues, result]: [Issues, {type: string}] = useQuery(
    q,
    options,
  ) as unknown as [Issues, {type: string}];
  // not used but needed to follow rules of hooks
  void useQuery(null, options);
  void useQuery(null, options);

  const complete = result.type === 'complete';
  const hasMoreIssues = issues.length > pageSize;
  const issuesLength = hasMoreIssues ? pageSize : issues.length;
  const issuesEmpty = issues.length === 0;

  if (kind === 'forward') {
    return {
      issueAt: (index: number) =>
        index - anchorIndex < issuesLength
          ? issues[index - anchorIndex]
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
      return issues[anchorIndex - index - 1];
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

export const anchorSchema = zod.discriminatedUnion('kind', [
  zod.readonly(
    zod.object({
      index: zod.number(),
      kind: zod.literal('forward'),
      startRow: zod.optional(issueRowSortSchema),
    }),
  ),
  zod.readonly(
    zod.object({
      index: zod.number(),
      kind: zod.literal('backward'),
      startRow: issueRowSortSchema,
    }),
  ),
  zod.readonly(
    zod.object({
      index: zod.number(),
      kind: zod.literal('permalink'),
      id: zod.string(),
    }),
  ),
]);

export type Anchor = zod.infer<typeof anchorSchema>;

export type Issue = Row<ReturnType<typeof queries.issueListV2>>;

export type Issues = Issue[];
export const TOP_ANCHOR = Object.freeze({
  index: 0,
  kind: 'forward',
  startRow: undefined,
});
