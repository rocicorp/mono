// oxlint-disable no-console
import type {Row} from '@rocicorp/zero';
import {
  setupSlowQuery,
  useQuery,
  useSlowQuery,
  useZero,
  type UseQueryOptions,
} from '@rocicorp/zero/react';
import {useVirtualizer} from '@tanstack/react-virtual';
import classNames from 'classnames';
import Cookies from 'js-cookie';
import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type KeyboardEvent,
} from 'react';
import {assert} from 'shared/src/asserts.ts';
import {useDebouncedCallback} from 'use-debounce';
import {useLocation, useParams, useSearch} from 'wouter';
import {navigate} from 'wouter/use-browser-location';
import {must} from '../../../../../packages/shared/src/must.ts';
import {
  queries,
  type IssueRowSort,
  type ListContext,
  type ListContextParams,
} from '../../../shared/queries.ts';
import InfoIcon from '../../assets/images/icon-info.svg?react';
import {Button} from '../../components/button.tsx';
import {Filter, type Selection} from '../../components/filter.tsx';
import {IssueLink} from '../../components/issue-link.tsx';
import {Link} from '../../components/link.tsx';
import {OnboardingModal} from '../../components/onboarding-modal.tsx';
import {RelativeTime} from '../../components/relative-time.tsx';
import {useClickOutside} from '../../hooks/use-click-outside.ts';
import {useElementSize} from '../../hooks/use-element-size.ts';
import {useKeypress} from '../../hooks/use-keypress.ts';
import {useLogin} from '../../hooks/use-login.tsx';
import {recordPageLoad} from '../../page-load-stats.ts';
import {mark} from '../../perf-log.ts';
import {CACHE_NAV, CACHE_NONE} from '../../query-cache-policy.ts';
import {isGigabugs, useListContext} from '../../routes.tsx';
import {preload} from '../../zero-preload.ts';

// Set to true to enable slow query simulation (half data → full data after 5s)
const USE_SLOW_QUERY = true;
if (USE_SLOW_QUERY) {
  setupSlowQuery({delayMs: 1000, unknownDataPercentage: 70});
}

let firstRowRendered = false;
const ITEM_SIZE = 56;
// Make sure this is even since we half it for permalink loading
const MIN_PAGE_SIZE = 100;
const NUM_ROWS_FOR_LOADING_SKELETON = 1;

type StartRow = IssueRowSort;

type Anchor =
  | {
      readonly startRow: StartRow | undefined;
      readonly type: 'forward' | 'backward';
      readonly index: number;
    }
  | {
      readonly startRow: StartRow;
      readonly type: 'permalink';
      readonly index: number;
    };

type QueryAnchor = {
  readonly anchor: Anchor;
  /**
   * Associates an anchor with list query params.  This is for managing the
   * transition when query params change.  When this happens the list should
   * scroll to 0, the anchor reset to top, and estimate/total counts reset.
   * During this transition, some renders has a mix of new list query params and
   * list results and old anchor (as anchor reset is async via setState), it is
   * important to:
   * 1. avoid creating a query with the new query params but the old anchor, as
   *    that would be loading a query that is not the correct one to display,
   *    accomplished by using TOP_ANCHOR when
   *    listContextParams !== queryAnchor.listContextParams
   * 2. avoid calculating counts based on a mix of new list results and old
   *    anchor, avoided by not updating counts when
   *    listContextParams !== queryAnchor.listContextParams
   * 3. avoid updating anchor for paging based on a mix of new list results and
   *    old anchor, avoided by not doing paging updates when
   *    listContextParams !== queryAnchor.listContextParams
   */
  readonly listContextParams: ListContextParams;
};

const TOP_ANCHOR = Object.freeze({
  index: 0,
  type: 'forward',
  startRow: undefined,
});

const START_ANCHOR: Anchor =
  // TOP_ANCHOR;

  {
    index: NUM_ROWS_FOR_LOADING_SKELETON,
    type: 'permalink',

    // // First issue
    // // index: 0
    // // subject: Leaking listeners on AbortSignal
    // startRow: {
    //   id: 'HdpMkgbHpK3_OcOIiQOuW',
    //   modified: 1765697824305,
    //   created: 1726473756000,
    // },

    // // Early issue
    // // index: 45
    // // subject:RFE: enumerateCaches
    // startRow: {
    //   id: 'X-TwNXBDwTeQB0Mav31bU',
    //   modified: 1709537728000,
    //   created: 1669918206000,
    // },

    // Middle issue
    // index: 260
    // title: Evaluate if we should return a ClientStateNotFoundResponse ...
    startRow: {
      id: '0zTrvA-6aVO8eNdHBoW7G',
      modified: 1678220708000,
      created: 1671231873000,
    },

    // // Close to bottom
    // // index: 500
    // // Subject: Add DevTools
    // startRow: {
    //   id: 'mrh64by3B9b6MRHbzkLQP',
    //   modified: 1677090672000,
    //   created: 1666168138000,
    // },

    // // Even closer to bottom
    // // title: Can we support...
    // // index: 512
    // startRow: {
    //   id: '8tyDj9FUJWQ5qd2JEP3KS',
    //   modified: 1677090541000,
    //   created: 1665587844000,
    // },

    // // Last issue
    // // index: 515
    // // subject: docs: Add something about offline ...
    // startRow: {
    //   id: '4wBDlh9b774qfGD3pWe6d',
    //   modified: 1677090538000,
    //   created: 1667987251000,
    // },
  };

const getNearPageEdgeThreshold = (pageSize: number) => Math.ceil(pageSize / 10);

const toIssueArrayIndex = (index: number, anchor: Anchor) =>
  anchor.type !== 'backward' ? index - anchor.index : anchor.index - index;

const toBoundIssueArrayIndex = (
  index: number,
  anchor: Anchor,
  length: number,
) => Math.min(length - 1, Math.max(0, toIssueArrayIndex(index, anchor)));

const toIndex = (issueArrayIndex: number, anchor: Anchor) =>
  anchor.type !== 'backward'
    ? issueArrayIndex + anchor.index
    : anchor.index - issueArrayIndex;

function RowDebugInfo({
  index,
  issueArrayIndex,
  issue,
}: {
  index: number;
  issueArrayIndex: number;
  issue?: Row['issue'] | undefined;
}) {
  const handleMouseDown = async (e: React.MouseEvent) => {
    if (!issue) {
      return;
    }
    e.stopPropagation();
    const permalinkAnchor = {
      index: 1,
      type: 'permalink' as const,
      startRow: {
        id: issue.id,
        modified: issue.modified,
        created: issue.created,
      },
    };
    await navigator.clipboard.writeText(
      JSON.stringify(permalinkAnchor, null, 2),
    );
  };

  return (
    <span
      style={{
        fontSize: '0.8em',
        marginRight: '0.5em',
        color: '#888',
        cursor: 'pointer',
      }}
      onMouseDownCapture={handleMouseDown}
      title="Click to copy permalink anchor"
    >
      <span title="index">{index}</span>,
      <span title="issueArrayIndex">{issueArrayIndex}</span>,
      {issue && <span title="issue.id">{issue.id.slice(0, 5)}</span>}
    </span>
  );
}

export function ListPage({onReady}: {onReady: () => void}) {
  const login = useLogin();
  const search = useSearch();
  const qs = useMemo(() => new URLSearchParams(search), [search]);
  const z = useZero();

  const params = useParams();
  const projectName = must(params.projectName);

  const [showOnboarding, setShowOnboarding] = useState(false);

  useEffect(() => {
    if (isGigabugs(projectName) && !Cookies.get('onboardingDismissed')) {
      setShowOnboarding(true);
    }
  }, [projectName]);

  const [projects] = useQuery(queries.allProjects());
  const project = projects.find(
    p => p.lowerCaseName === projectName.toLocaleLowerCase(),
  );

  const status = qs.get('status')?.toLowerCase() ?? 'open';
  const creator = qs.get('creator') ?? null;
  const assignee = qs.get('assignee') ?? null;
  const labels = useMemo(() => qs.getAll('label'), [qs]);

  // Cannot drive entirely by URL params because we need to debounce the changes
  // while typing into input box.
  const textFilterQuery = qs.get('q');
  const [textFilter, setTextFilter] = useState(textFilterQuery);
  useEffect(() => {
    setTextFilter(textFilterQuery);
  }, [textFilterQuery]);

  const sortField =
    qs.get('sort')?.toLowerCase() === 'created' ? 'created' : 'modified';
  const sortDirection =
    qs.get('sortDir')?.toLowerCase() === 'asc' ? 'asc' : 'desc';

  const open = status === 'open' ? true : status === 'closed' ? false : null;

  const listContextParams = useMemo(
    () =>
      ({
        projectName,
        sortDirection,
        sortField,
        assignee,
        creator,
        labels,
        open,
        textFilter,
      }) as const,
    [
      projectName,
      sortDirection,
      sortField,
      assignee,
      creator,
      open,
      textFilter,
      labels,
    ],
  );

  const [queryAnchor, setQueryAnchor] = useState<QueryAnchor>({
    anchor: START_ANCHOR,
    listContextParams,
  });

  const listRef = useRef<HTMLDivElement>(null);
  const tableWrapperRef = useRef<HTMLDivElement>(null);
  const size = useElementSize(tableWrapperRef);

  const [pageSize, setPageSize] = useState(MIN_PAGE_SIZE);
  useEffect(() => {
    // Make sure page size is enough to fill the scroll element at least
    // 3 times.  Don't shrink page size.
    const newPageSize = size
      ? Math.max(
          MIN_PAGE_SIZE,
          makeEven(Math.ceil(size?.height / ITEM_SIZE) * 3),
        )
      : MIN_PAGE_SIZE;
    if (newPageSize > pageSize) {
      setPageSize(newPageSize);
    }
  }, [pageSize, size]);

  const anchor =
    queryAnchor.listContextParams === listContextParams
      ? queryAnchor.anchor
      : START_ANCHOR;

  // Track when queryAnchor/anchor changes
  useEffect(() => {
    console.log('anchor changed', {
      'anchor.index': anchor.index,
      'anchor.type': anchor.type,
      'queryAnchor === reference': anchor === queryAnchor.anchor,
    });
  }, [anchor, queryAnchor]);

  // TODO(arv): Maybe use 4* like before?
  const [estimatedTotal, setEstimatedTotal] = useState(
    NUM_ROWS_FOR_LOADING_SKELETON,
  );

  // const [total, setTotal] = useState<number | undefined>(undefined);

  const [hasScrolledToPermalink, setHasScrolledToPermalink] = useState(false);
  const [pendingScrollShift, setPendingScrollShift] = useState<{
    oldAnchor: Anchor;
    oldIssuesLength: number;
    newAnchor: Anchor;
  } | null>(null);
  const [skipPagingLogic, setSkipPagingLogic] = useState(false);
  const [hasReachedEnd, setHasReachedEnd] = useState(false);
  const [hasReachedStart, setHasReachedStart] = useState(false);

  const total = hasReachedStart && hasReachedEnd ? estimatedTotal : undefined;

  // Create a stable identifier for the current anchor to track which anchor generated the issues
  const anchorId = useMemo(
    () => `${anchor.index}-${anchor.type}-${anchor.startRow?.id ?? 'null'}`,
    [anchor.index, anchor.type, anchor.startRow?.id],
  );

  // Track the previous anchor to detect changes
  const prevAnchorIdRef = useRef<string>(anchorId);
  const maxIssuesLengthRef = useRef<number>(0);

  // We don't want to cache every single keystroke. We already debounce
  // keystrokes for the URL, so we just reuse that.
  const {issues, complete, atStart, atEnd} = useIssues(
    listContextParams,
    z.userID,
    pageSize,
    anchor.startRow ?? null,
    anchor.type,
    anchor.index,
    textFilterQuery === textFilter ? CACHE_NAV : CACHE_NONE,
  );

  // Update tracking when issues changes
  useEffect(() => {
    // Track the max issues length we've seen
    if (issues.length > maxIssuesLengthRef.current) {
      console.log('issues length increased', {
        'issues.length': issues.length,
        'old max': maxIssuesLengthRef.current,
        'anchor.index': anchor.index,
        anchorId,
      });
      maxIssuesLengthRef.current = issues.length;
    }
  }, [issues]);

  console.debug(
    'Render',
    'estimatedTotal',
    estimatedTotal,
    'issues.length',
    issues.length,
    'complete',
    complete,
    'atStart',
    atStart,
    'atEnd',
    atEnd,
    'anchor.type',
    anchor.type,
    'anchor.index',
    anchor.index,
    'scrollTop',
    listRef.current?.scrollTop,
    'scrollHeight',
    listRef.current?.scrollHeight,
  );

  useEffect(() => {
    if (issues.length > 0 || complete) {
      onReady();
    }
  }, [issues.length, complete, onReady]);

  useEffect(() => {
    if (complete) {
      recordPageLoad('list-page');
      preload(z, projectName);
    }
  }, [login.loginState?.decoded, complete, z]);

  let title;
  let shortTitle;
  if (creator || assignee || labels.length > 0 || textFilter) {
    title = 'Filtered Issues';
    shortTitle = 'Filtered';
  } else {
    const statusCapitalized =
      status.slice(0, 1).toUpperCase() + status.slice(1);
    title = statusCapitalized + ' Issues';
    shortTitle = statusCapitalized;
  }

  const [location] = useLocation();
  const listContext: ListContext = useMemo(
    () => ({
      href: `${location}?${search}`,
      title,
      params: listContextParams,
    }),
    [location, search, title, listContextParams],
  );

  const {setListContext} = useListContext();
  useEffect(() => {
    setListContext(listContext);
  }, [listContext]);

  const onDeleteFilter = (e: React.MouseEvent) => {
    const target = e.currentTarget;
    const key = target.getAttribute('data-key');
    const value = target.getAttribute('data-value');
    if (key && value) {
      navigate(removeParam(qs, key, value));
    }
  };

  const onFilter = useCallback(
    (selection: Selection) => {
      if ('creator' in selection) {
        navigate(addParam(qs, 'creator', selection.creator, 'exclusive'));
      } else if ('assignee' in selection) {
        navigate(addParam(qs, 'assignee', selection.assignee, 'exclusive'));
      } else {
        navigate(addParam(qs, 'label', selection.label));
      }
    },
    [qs],
  );

  const toggleSortField = useCallback(() => {
    navigate(
      addParam(
        qs,
        'sort',
        sortField === 'created' ? 'modified' : 'created',
        'exclusive',
      ),
    );
  }, [qs, sortField]);

  const toggleSortDirection = useCallback(() => {
    navigate(
      addParam(
        qs,
        'sortDir',
        sortDirection === 'asc' ? 'desc' : 'asc',
        'exclusive',
      ),
    );
  }, [qs, sortDirection]);

  const updateTextFilterQueryString = useDebouncedCallback((text: string) => {
    navigate(addParam(qs, 'q', text, 'exclusive'));
  }, 500);

  const onTextFilterChange = (text: string) => {
    setTextFilter(text);
    updateTextFilterQueryString(text);
  };

  const clearAndHideSearch = () => {
    setTextFilter(null);
    setForceSearchMode(false);
    navigate(removeParam(qs, 'q'));
  };

  const Row = ({index, style}: {index: number; style: CSSProperties}) => {
    const issueArrayIndex = toIssueArrayIndex(index, anchor);
    if (issueArrayIndex < 0 || issueArrayIndex >= issues.length) {
      return (
        <div
          className={classNames('row', 'skeleton-shimmer')}
          style={{
            ...style,
          }}
        >
          {' '}
          <RowDebugInfo index={index} issueArrayIndex={issueArrayIndex} />
        </div>
      );
    }
    const issue = issues[issueArrayIndex];
    if (firstRowRendered === false) {
      mark('first issue row rendered');
      firstRowRendered = true;
    }

    const timestamp = sortField === 'modified' ? issue.modified : issue.created;
    return (
      <div
        key={issue.id}
        className={classNames(
          'row',
          issue.modified > (issue.viewState?.viewed ?? 0) &&
            login.loginState !== undefined
            ? 'unread'
            : null,
          {permalink: issue.id === START_ANCHOR.startRow?.id},
        )}
        style={{
          ...style,
        }}
      >
        <IssueLink
          className={classNames('issue-title', {'issue-closed': !issue.open})}
          issue={{projectName, id: issue.id, shortID: issue.shortID}}
          title={issue.title}
          listContext={listContext}
        >
          <RowDebugInfo
            index={index}
            issueArrayIndex={issueArrayIndex}
            issue={issue}
          />
          {issue.title}
        </IssueLink>
        <div className="issue-taglist">
          {issue.labels.map(label => (
            <Link
              key={label.id}
              className="pill label"
              href={`?label=${label.name}`}
            >
              {label.name}
            </Link>
          ))}
        </div>
        <div className="issue-timestamp">
          <RelativeTime timestamp={timestamp} />
        </div>
      </div>
    );
  };

  // #region Computing new estimated total
  // Update estimated total - should only increase unless we reach the end
  useEffect(() => {
    if (queryAnchor.listContextParams !== listContextParams) {
      return;
    }

    const anchorJustChanged = prevAnchorIdRef.current !== anchorId;

    // Guard: Skip if anchor just changed - issues might be stale cached data
    if (anchorJustChanged) {
      console.log('skipping estimated total calc - anchor just changed', {
        'prevAnchorId': prevAnchorIdRef.current,
        'currentAnchorId': anchorId,
        'anchor.index': anchor.index,
        'issues.length': issues.length,
      });
      // Update prevAnchorIdRef for next render
      prevAnchorIdRef.current = anchorId;
      // Reset max when anchor changes
      maxIssuesLengthRef.current = issues.length;
      return;
    }

    // // Guard: Only calculate when we have new/more data
    // // This prevents recalculating with partial data from useSlowQuery (100 → 50 → 100)
    // if (issues.length < maxIssuesLengthRef.current) {
    //   console.log('skipping estimated total calc - partial data', {
    //     'issues.length': issues.length,
    //     'maxIssuesLength': maxIssuesLengthRef.current,
    //     'anchor.index': anchor.index,
    //   });
    //   return;
    // }

    console.log('estimated total effect running', {
      'queryAnchor.anchor': queryAnchor.anchor,
      anchor,
      'anchor === queryAnchor.anchor': anchor === queryAnchor.anchor,
      'issues.length': issues.length,
      anchorId,
      'prevAnchorId': prevAnchorIdRef.current,
    });

    // TODO(arv): Should work for incomplete lists too
    // if (!complete) {
    //   return;
    // }

    let newEstimate: number;
    if (atStart && atEnd) {
      // We have the complete list
      newEstimate = issues.length;
      setHasReachedStart(true);
      setHasReachedEnd(true);
    } else if (atStart) {
      // We know the start, estimate the rest
      const extra = hasReachedEnd ? 0 : NUM_ROWS_FOR_LOADING_SKELETON;
      newEstimate = Math.max(
        estimatedTotal,
        anchor.index + issues.length + extra,
      );
      setHasReachedStart(true);
    } else if (atEnd) {
      // We know the end, we have everything
      console.log('setting estimated total at end', {
        'anchor.index': anchor.index,
        'issues.length': issues.length,
      });
      if (hasReachedEnd) {
        newEstimate = Math.max(estimatedTotal, anchor.index + issues.length);
      } else {
        newEstimate = anchor.index + issues.length;
      }
      setHasReachedEnd(true);
    } else {
      newEstimate = Math.max(
        estimatedTotal,
        anchor.type === 'backward'
          ? anchor.index + NUM_ROWS_FOR_LOADING_SKELETON
          : anchor.index + issues.length + NUM_ROWS_FOR_LOADING_SKELETON,
      );

      console.log('setting estimated total in middle', {
        newEstimate,
        estimatedTotal,
        'anchor.type': anchor.type,
        'anchor.index': anchor.index,
        'issues.length': issues.length,
      });
    }

    if (newEstimate > estimatedTotal || atEnd) {
      console.log('setting estimated total?', {
        newEstimate,
        estimatedTotal,
        complete,
        atEnd,
        atStart,
      });

      setEstimatedTotal(newEstimate);
    }
  }, [
    complete,
    atStart,
    atEnd,
    anchor.index,
    anchor.type,
    anchor.startRow?.id, // Ensure effect re-runs when queryAnchor changes to detect stale issues
    issues.length,
    estimatedTotal,
    hasReachedEnd,
  ]);
  //#endregion

  const virtualizer = useVirtualizer({
    // count: total ?? estimatedTotal + NUM_ROWS_FOR_LOADING_SKELETON,
    count: estimatedTotal,
    estimateSize: () => ITEM_SIZE,
    overscan: 5,
    getScrollElement: () => listRef.current,
    initialOffset: () => {
      if (anchor.type === 'permalink') {
        // TODO(arv): Also measure
        return (anchor.index + pageSize / 2) * ITEM_SIZE;
      }
      return 0;
    },
  });

  // If the anchor is a permalink we need to adjust the scroll position once after we got the complete list.
  useEffect(() => {
    if (hasScrolledToPermalink) {
      return;
    }

    const anchorJustChanged = prevAnchorIdRef.current !== anchorId;

    if (anchor.type === 'permalink' && complete) {
      assert(!anchorJustChanged);
      // Find the actual index of the permalink item in the list. It might not be in the middle because there might not be full
      // pages before or after it.
      const issueArrayIndex = issues.findIndex(
        issue => issue.id === anchor.startRow.id,
      );
      console.log(
        'permalink',
        'scrolling to issueArrayIndex',
        anchor.index,
        issueArrayIndex,
        atStart,
      );
      virtualizer.scrollToIndex(
        (atStart ? 0 : anchor.index) + issueArrayIndex,
        {
          align: 'start',
        },
      );
      setHasScrolledToPermalink(!atStart);
    }
  }, [
    hasScrolledToPermalink,
    anchor.type,
    anchor.startRow?.id,
    anchor.index,
    complete,
    atStart,
    issues,
    virtualizer,
  ]);

  const virtualItems = virtualizer.getVirtualItems();

  // Clear skip paging flag after scroll shift is applied and virtualizer has updated
  useEffect(() => {
    if (skipPagingLogic && !pendingScrollShift) {
      setSkipPagingLogic(false);
    }
  }, [skipPagingLogic, pendingScrollShift, virtualizer.scrollOffset]);

  // Apply pending scroll shift after new issues have loaded
  useEffect(() => {
    if (!pendingScrollShift || !complete) {
      return;
    }

    const {oldAnchor, oldIssuesLength, newAnchor} = pendingScrollShift;

    // When converting from forward/permalink to backward anchor, we need to calculate
    // shift differently because the indexing semantics change:
    // - Forward: issues[i] at virtualIndex = anchor.index + i
    // - Backward: issues[i] at virtualIndex = anchor.index - i
    const isConvertingToBackward =
      oldAnchor.type !== 'backward' && newAnchor.type === 'backward';

    let shift: number;
    if (isConvertingToBackward) {
      // For forward->backward conversion:
      // The new anchor.index should be issues.length + pageSize
      // After loading new issues, we'll have more issues, so the actual anchor.index
      // needs to be adjusted to issues.length to keep visual position
      shift = issues.length - newAnchor.index;
    } else {
      // Normal shift calculation for backward->backward paging
      shift =
        oldAnchor.type !== 'backward'
          ? oldAnchor.index + oldIssuesLength - newAnchor.index
          : oldAnchor.index - newAnchor.index;
    }

    if (atStart) {
      if (oldAnchor.type === 'backward') {
        shift = issues.length - anchor.index - 1; //newAnchor.index - oldAnchor.index + issues.length + 0;
      } else {
        shift = oldAnchor.index - issues.length;
      }
    }

    console.log('applying scroll shift', {
      shift,
      oldAnchorIndex: oldAnchor.index,
      oldAnchorType: oldAnchor.type,
      oldIssuesLength,
      newIssuesLength: issues.length,
      newAnchorIndex: newAnchor.index,
      newAnchorType: newAnchor.type,
      isConvertingToBackward,
      scrollOffset: virtualizer.scrollOffset,
    });

    // Apply scroll shift and adjust anchor
    const newScrollOffset = (virtualizer.scrollOffset ?? 0) + shift * ITEM_SIZE;

    console.log('adjusting scroll and anchor for shift', {
      oldScrollOffset: virtualizer.scrollOffset,
      newScrollOffset,
      shiftPixels: shift * ITEM_SIZE,
      oldAnchorIndex: newAnchor.index,
      newAnchorIndex: isConvertingToBackward
        ? issues.length
        : newAnchor.index + shift,
    });

    if (atStart) {
      setQueryAnchor({
        anchor: {
          index: 0,
          type: 'forward',
          startRow: undefined,
        },
        listContextParams,
      });
    } else {
      // Adjust the anchor index
      setQueryAnchor({
        anchor: {
          ...newAnchor,
          // For backward anchor after conversion, set to actual issues.length
          // For continued backward paging, shift by the loaded amount
          index: isConvertingToBackward
            ? issues.length
            : newAnchor.index + shift,
        },
        listContextParams,
      });
    }

    virtualizer.scrollToOffset(newScrollOffset);
    setEstimatedTotal(estimatedTotal + shift);
    setPendingScrollShift(null);
    // skipPagingLogic is already set when pendingScrollShift was created
  }, [
    pendingScrollShift,
    complete,
    issues.length,
    anchor,
    atStart,
    listContextParams,
    virtualizer,
  ]);

  // #region Scrolling
  useEffect(() => {
    if (queryAnchor.listContextParams !== listContextParams) {
      return;
    }

    if (virtualItems.length === 0) {
      return;
    }

    // Skip paging logic temporarily after a scroll shift to avoid
    // triggering page down from the scroll offset change
    if (skipPagingLogic) {
      return;
    }

    const firstItem = virtualItems[0];
    const lastItem = virtualItems[virtualItems.length - 1];

    const nearPageEdgeThreshold = getNearPageEdgeThreshold(pageSize);

    if (!complete) {
      return;
    }

    // Should we load more backward (page up)?
    const distanceFromStart =
      anchor.type === 'backward'
        ? firstItem.index - (anchor.index - issues.length)
        : firstItem.index - anchor.index;

    console.debug(
      'distanceFromStart',
      distanceFromStart,
      'anchor.index',
      anchor.index,
      'anchor.type',
      anchor.type,
      'issues.length',
      issues.length,
      'lastItem.index',
      lastItem.index,
      'atEnd',
      atEnd,
      'firstItem.index',
      firstItem.index,
    );

    if (atStart) {
      // When at start with backward anchor, we need to convert to forward anchor with index 0
      if (anchor.type === 'backward') {
        setQueryAnchor({
          anchor: TOP_ANCHOR,
          listContextParams,
        });
        return;
      }
      // When at start with forward/permalink anchor, just ensure index is 0
      if (anchor.index !== 0) {
        setQueryAnchor({
          anchor: {
            ...anchor,
            index: 0,
          },
          listContextParams,
        });
        return;
      }
    }

    // TODO(arv): I don't think we need to overfetch. It seems like we can use
    // anchor.index === 0 like before
    if (
      // virtualizer.scrollDirection === 'backward' &&
      complete &&
      !atStart &&
      distanceFromStart <= nearPageEdgeThreshold
    ) {
      // if (anchor.index !== 0 && distanceFromStart <= nearPageEdgeThreshold) {
      const issueArrayIndex = toBoundIssueArrayIndex(
        lastItem.index + nearPageEdgeThreshold * 2,
        anchor,
        issues.length,
      );

      // When converting from forward/permalink to backward anchor,
      // the backward anchor index should be issues.length + headroom
      // because for backward anchors: issues[i] renders at virtualIndex = anchor.index - i
      const isConvertingToBackward = anchor.type !== 'backward';
      const index = toIndex(issueArrayIndex, anchor) - 1;

      // If index is now less than pageSize then we need shifting to ensure headroom
      if (index < pageSize) {
        const newAnchor: Anchor = {
          index,
          type: 'backward',
          startRow: toStartRow(issues[issueArrayIndex]),
        };
        console.log('page up - setting new anchor with pending shift', {
          index: newAnchor.index,
          complete,
          estimatedTotal,
          type: newAnchor.type,
          startRow: newAnchor.startRow,
          oldAnchorIndex: anchor.index,
          oldAnchorType: anchor.type,
          oldIssuesLength: issues.length,
          isConvertingToBackward,
        });

        setPendingScrollShift({
          oldAnchor: anchor,
          oldIssuesLength: issues.length,
          newAnchor,
        });
        setSkipPagingLogic(true);
        setQueryAnchor({
          anchor: newAnchor,
          listContextParams,
        });

        return;
      }

      const newAnchor: Anchor = {
        index,
        type: 'backward',
        startRow: toStartRow(issues[issueArrayIndex]),
      };

      console.log('page up - setting new anchor without pending shift', {
        index: newAnchor.index,
        complete,
        estimatedTotal,
        type: newAnchor.type,
        startRow: newAnchor.startRow,
        oldAnchorIndex: anchor.index,
        oldAnchorType: anchor.type,
        oldIssuesLength: issues.length,
        isConvertingToBackward,
      });

      setQueryAnchor({
        anchor: newAnchor,
        listContextParams,
      });

      return;
    }

    // Should we load more forward?
    const distanceFromEnd =
      anchor.type !== 'backward'
        ? anchor.index + issues.length - lastItem.index
        : anchor.index - lastItem.index;

    if (atEnd || !complete) {
      return;
    }

    if (
      // virtualizer.scrollDirection === 'forward' &&
      distanceFromEnd <= nearPageEdgeThreshold
    ) {
      const issueArrayIndex = toBoundIssueArrayIndex(
        firstItem.index - nearPageEdgeThreshold * 2,
        anchor,
        issues.length,
      );
      const index = toIndex(issueArrayIndex, anchor) + 1;
      // oxlint-disable-next-line no-console -- Debug logging in demo app
      console.log('page down', {
        index,
        complete,
        estimatedTotal,
        direction: 'forward',
        startRow: toStartRow(issues[issueArrayIndex]),
        currentAnchorIndex: anchor.index,
        currentAnchorType: anchor.type,
        issuesLength: issues.length,
        firstItemIndex: firstItem.index,
        lastItemIndex: lastItem.index,
        distanceFromEnd,
        nearPageEdgeThreshold,
      });

      setQueryAnchor({
        anchor: {
          index,
          type: 'forward',
          startRow: toStartRow(issues[issueArrayIndex]),
        },
        listContextParams,
      });
      console.log('increasing estimated total for page down', {
        index,
        pageSize,
        estimatedTotal,
        'anchor.type': anchor.type,
        'anchor.index': anchor.index,
      });
      if (!hasReachedEnd) {
        // setEstimatedTotal(estimatedTotal + NUM_ROWS_FOR_LOADING_SKELETON);
        //   Math.max(
        //     estimatedTotal,
        //     anchor.type !== 'backward'
        //       ? index + issues.length + NUM_ROWS_FOR_LOADING_SKELETON
        //       : index + NUM_ROWS_FOR_LOADING_SKELETON,
        //   ),
        // );
      }
    }
  }, [
    listContextParams,
    queryAnchor,
    issues,
    complete,
    pageSize,
    virtualItems,
    skipPagingLogic,
    virtualizer,
    anchor,
  ]);
  // #endregion

  const [forceSearchMode, setForceSearchMode] = useState(false);
  const searchMode = forceSearchMode || Boolean(textFilter);
  const searchBox = useRef<HTMLHeadingElement>(null);
  const startSearchButton = useRef<HTMLButtonElement>(null);

  useKeypress('/', () => {
    if (project?.supportsSearch) {
      setForceSearchMode(true);
    }
  });
  useClickOutside([searchBox, startSearchButton], () => {
    if (textFilter) {
      setForceSearchMode(false);
    } else {
      clearAndHideSearch();
    }
  });
  const handleSearchKeyUp = (e: KeyboardEvent) => {
    if (e.key === 'Escape') {
      clearAndHideSearch();
    }
  };
  const toggleSearchMode = () => {
    if (searchMode) {
      clearAndHideSearch();
    } else {
      setForceSearchMode(true);
    }
  };

  return (
    <>
      <div className="list-view-header-container">
        <h1
          className={classNames('list-view-header', {
            'search-mode': searchMode,
          })}
          ref={searchBox}
        >
          {searchMode ? (
            <div className="search-input-container">
              <input
                type="text"
                className="search-input"
                value={textFilter ?? ''}
                onChange={e => onTextFilterChange(e.target.value)}
                onFocus={() => setForceSearchMode(true)}
                onBlur={() => setForceSearchMode(false)}
                onKeyUp={handleSearchKeyUp}
                placeholder="Search…"
                autoFocus={true}
              />
              {textFilter && (
                <Button
                  className="clear-search"
                  onAction={() => setTextFilter('')} // Clear the search field
                  aria-label="Clear search"
                >
                  &times;
                </Button>
              )}
            </div>
          ) : (
            <>
              <span className="list-view-title list-view-title-full">
                {title}
              </span>
              <span className="list-view-title list-view-title-short">
                {shortTitle}
              </span>
            </>
          )}
          {complete || total || estimatedTotal ? (
            <>
              <span className="issue-count">
                {project?.issueCountEstimate
                  ? `${(total ?? roundEstimatedTotal(estimatedTotal)).toLocaleString()} of ${formatIssueCountEstimate(project.issueCountEstimate)}`
                  : (total?.toLocaleString() ??
                    `${roundEstimatedTotal(estimatedTotal).toLocaleString()}+`)}
              </span>
              {isGigabugs(projectName) && (
                <button
                  className="info-button"
                  onMouseDown={() => setShowOnboarding(true)}
                  aria-label="Show onboarding information"
                  title="Show onboarding information"
                >
                  <InfoIcon />
                </button>
              )}
            </>
          ) : null}
        </h1>
        <Button
          ref={startSearchButton}
          style={{visibility: project?.supportsSearch ? 'visible' : 'hidden'}}
          className="search-toggle"
          eventName="Toggle Search"
          onAction={toggleSearchMode}
        ></Button>
      </div>
      <div className="list-view-filter-container">
        <span className="filter-label">Filtered by:</span>
        <div className="set-filter-container">
          {[...qs.entries()].map(([key, val]) => {
            if (key === 'label' || key === 'creator' || key === 'assignee') {
              return (
                <span
                  className={classNames('pill', {
                    label: key === 'label',
                    user: key === 'creator' || key === 'assignee',
                  })}
                  onMouseDown={onDeleteFilter}
                  data-key={key}
                  data-value={val}
                  key={key + '-' + val}
                >
                  {key}: {val}
                </span>
              );
            }
            return null;
          })}
        </div>
        <Filter projectName={projectName} onSelect={onFilter} />
        <div className="sort-control-container">
          <Button
            enabledOffline
            className="sort-control"
            eventName="Toggle sort type"
            onAction={toggleSortField}
          >
            {sortField === 'modified' ? 'Modified' : 'Created'}
          </Button>
          <Button
            enabledOffline
            className={classNames('sort-direction', sortDirection)}
            eventName="Toggle sort direction"
            onAction={toggleSortDirection}
          ></Button>
        </div>
      </div>

      <div className="issue-list" ref={tableWrapperRef}>
        {size && issues.length > 0 ? (
          <div
            style={{
              width: size.width,
              height: size.height,
              overflow: 'auto',
              overflowAnchor: 'none',
            }}
            ref={listRef}
          >
            <div
              className="virtual-list"
              style={{height: virtualizer.getTotalSize()}}
            >
              {virtualItems.map(virtualRow => (
                <Row
                  key={virtualRow.key + ''}
                  index={virtualRow.index}
                  style={{
                    transform: `translateY(${virtualRow.start}px)`,
                  }}
                />
              ))}
            </div>
          </div>
        ) : null}
      </div>
      <OnboardingModal
        isOpen={showOnboarding}
        onDismiss={() => {
          Cookies.set('onboardingDismissed', 'true', {expires: 365});
          setShowOnboarding(false);
        }}
      />
    </>
  );
}

const addParam = (
  qs: URLSearchParams,
  key: string,
  value: string,
  mode?: 'exclusive',
) => {
  const newParams = new URLSearchParams(qs);
  newParams[mode === 'exclusive' ? 'set' : 'append'](key, value);
  return '?' + newParams.toString();
};

function roundEstimatedTotal(estimatedTotal: number) {
  return estimatedTotal < 50
    ? estimatedTotal
    : estimatedTotal - (estimatedTotal % 50);
}

function removeParam(qs: URLSearchParams, key: string, value?: string) {
  const searchParams = new URLSearchParams(qs);
  searchParams.delete(key, value);
  return '?' + searchParams.toString();
}

function formatIssueCountEstimate(count: number) {
  if (count < 1000) {
    return count;
  }
  return `~${Math.floor(count / 1000).toLocaleString()}k`;
}

function toStartRow(row: Row['issue']): StartRow {
  return {
    id: row.id,
    created: row.created,
    modified: row.modified,
  };
}

type Issues = Row<ReturnType<typeof queries.issueListV2>>[];

function useIssues(
  listContext: ListContextParams,
  userID: string,
  pageSize: number,
  start: StartRow | null,
  kind: 'forward' | 'backward' | 'permalink',
  anchorIndex: number,
  options: UseQueryOptions,
) {
  // Conditionally use useSlowQuery or useQuery based on USE_SLOW_QUERY flag
  const queryFn = USE_SLOW_QUERY ? useSlowQuery : useQuery;

  if (kind === 'permalink') {
    assert(start !== null);
    assert(pageSize % 2 === 0);

    // TODO(arv): Before can be exclusive, after inclusive

    const halfPageSize = pageSize / 2;
    const qBefore = queries.issueListV2({
      listContext,
      userID,
      limit: halfPageSize + 1,
      start,
      dir: 'backward',
      inclusive: true,
    });
    const qAfter = queries.issueListV2({
      listContext,
      userID,
      limit: halfPageSize + 1,
      start,
      dir: 'forward',
      inclusive: true,
    });
    const [issuesBefore, resultBefore]: [Issues, {type: string}] = queryFn(
      qBefore,
      options,
    ) as unknown as [Issues, {type: string}];
    const [issuesAfter, resultAfter]: [Issues, {type: string}] = queryFn(
      qAfter,
      options,
    ) as unknown as [Issues, {type: string}];
    const completeBefore = resultBefore.type === 'complete';
    const completeAfter = resultAfter.type === 'complete';

    return {
      issues: joinIssues(issuesBefore, issuesAfter, halfPageSize),
      complete: completeBefore && completeAfter,
      atStart: completeBefore && issuesBefore.length <= halfPageSize,
      atEnd: completeAfter && issuesAfter.length <= halfPageSize,
    };
  }

  kind satisfies 'forward' | 'backward';

  const q = queries.issueListV2({
    listContext,
    userID,
    limit: pageSize + 1,
    start,
    dir: kind,
    inclusive: start === null,
  });
  const [issues, result]: [Issues, {type: string}] = queryFn(
    q,
    options,
  ) as unknown as [Issues, {type: string}];
  // not used but needed to follow rules of hooks
  void queryFn(q, options);

  const complete = result.type === 'complete';
  const hasMoreIssues = issues.length > pageSize;
  const slicedIssues = hasMoreIssues ? issues.slice(0, pageSize) : issues;

  if (kind === 'forward') {
    return {
      issues: slicedIssues,
      complete,
      atStart: start === null || anchorIndex === 0,
      atEnd: complete && !hasMoreIssues,
    };
  }

  kind satisfies 'backward';
  assert(start !== null);

  return {
    issues: slicedIssues,
    complete,
    atStart: complete && !hasMoreIssues,
    atEnd: false,
  };
}

function joinIssues(
  issuesBefore: Issues,
  issuesAfter: Issues,
  pageSize: number,
): Issues {
  // issuesBefore is in reverse order (most recent first from backward query)
  // issuesAfter is in forward order (least recent first from forward query)
  // Both queries are inclusive, so they share the middle/anchor issue

  // Early returns for empty arrays to avoid unnecessary allocations
  if (issuesBefore.length === 0) {
    return issuesAfter;
  }

  // Slice issuesBefore to pageSize, reverse to get forward order
  const beforeSliced = issuesBefore.slice(0, pageSize).reverse();

  if (issuesAfter.length === 0) {
    return beforeSliced;
  }

  // issuesAfter already in forward order, but skip first item only if it's a duplicate anchor
  const lastBeforeId = beforeSliced[beforeSliced.length - 1].id;
  const firstAfterId = issuesAfter[0].id;
  const afterSliced =
    lastBeforeId === firstAfterId ? issuesAfter.slice(1) : issuesAfter;

  return [...beforeSliced, ...afterSliced];
}

function makeEven(n: number) {
  return n % 2 === 0 ? n : n + 1;
}
