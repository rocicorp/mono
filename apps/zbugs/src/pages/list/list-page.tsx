// oxlint-disable no-console
import type {Row} from '@rocicorp/zero';
import {useQuery, useZero, type UseQueryOptions} from '@rocicorp/zero/react';
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

let firstRowRendered = false;
const ITEM_SIZE = 56;
// Make sure this is even since we half it for permalink loading
const MIN_PAGE_SIZE = 100;
// const NUM_ROWS_FOR_LOADING_SKELETON = 1;

type StartRow = IssueRowSort;

type Anchor =
  | {
      startRow: StartRow | undefined;
      type: 'forward' | 'backward';
      index: number;
    }
  | {
      startRow: StartRow;
      type: 'permalink';
      index: number;
    };

type QueryAnchor = {
  anchor: Anchor;
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
  listContextParams: ListContextParams;
};

const MIN_ESTIMATED_ROWS_BEFORE = 1; //2 * MIN_PAGE_SIZE;
const MIN_ESTIMATED_ROWS_AFTER = MIN_ESTIMATED_ROWS_BEFORE;

const TOP_ANCHOR = Object.freeze({
  index: 0,
  type: 'forward',
  startRow: undefined,
});

const PERMALINK_INDEX = MIN_ESTIMATED_ROWS_BEFORE;

const START_ANCHOR: Anchor = !TOP_ANCHOR || {
  index: PERMALINK_INDEX,
  type: 'permalink',

  // First issue
  // index: 0
  // subject: Leaking listeners on AbortSignal
  startRow: {
    id: 'HdpMkgbHpK3_OcOIiQOuW',
    modified: 1765697824305,
    created: 1726473756000,
  },

  // // index: 45
  // // subject:RFE: enumerateCaches
  // startRow: {
  //   id: 'X-TwNXBDwTeQB0Mav31bU',
  //   modified: 1709537728000,
  //   created: 1669918206000,
  // },

  // // Middle issue
  // // index: 260
  // // title: Evaluate if we should return a ClientStateNotFoundResponse ...
  // startRow: {
  //   id: '0zTrvA-6aVO8eNdHBoW7G',
  //   modified: 1678220708000,
  //   created: 1671231873000,
  // },

  // // Close to bottom
  // // index: 500
  // // Subject: Add DevTools
  // startRow: {
  //   id: 'mrh64by3B9b6MRHbzkLQP',
  //   modified: 1677090672000,
  //   created: 1666168138000,
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
      index: PERMALINK_INDEX,
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

  // const [estimatedTotal, setEstimatedTotal] = useState(4 * MIN_PAGE_SIZE);

  // const [total, setTotal] = useState<number | undefined>(undefined);
  const total = undefined as number | undefined;

  const [estimatedRowsBefore, setEstimatedRowsBefore] = useState(
    START_ANCHOR.index,
  );
  const [estimatedRowsAfter, setEstimatedRowsAfter] = useState(
    MIN_ESTIMATED_ROWS_AFTER,
  );
  const [hasScrolledToPermalink, setHasScrolledToPermalink] = useState(false);
  const [pendingScrollShift, setPendingScrollShift] = useState<{
    oldIssuesLength: number;
    newAnchor: Anchor;
  } | null>(null);

  // We don't want to cache every single keystroke. We already debounce
  // keystrokes for the URL, so we just reuse that.
  const {
    issues,
    complete,
    // estimatedRowsBefore: newEstimatedRowsBefore,
    // estimatedRowsAfter: newEstimatedRowsAfter,
    // startRow,
    // endRow,
    atStart,
    atEnd,
    // ...rest
  } = useIssues(
    listContextParams,
    z.userID,
    pageSize,
    anchor.startRow ?? null,
    anchor.type,
    estimatedRowsBefore,
    textFilterQuery === textFilter ? CACHE_NAV : CACHE_NONE,
  );

  console.log(
    'Render',
    'estimatedRowsBefore',
    estimatedRowsBefore,
    'estimatedRowsAfter',
    estimatedRowsAfter,
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
    // When we reach the start with a backward anchor, anchor must be adjusted so issues don't overflow
    // the virtual list. For backward anchors, anchor.index should equal issues.length
    // so that issues[i] renders at virtualIndex = anchor.index - i
    // if (
    //   atStart &&
    //   anchor.type === 'backward' &&
    //   anchor.index !== issues.length
    // ) {
    //   console.log('adjusting anchor for atStart', {
    //     oldIndex: anchor.index,
    //     newIndex: issues.length,
    //     issuesLength: issues.length,
    //   });
    //   setQueryAnchor({
    //     anchor: {
    //       ...anchor,
    //       index: issues.length,
    //     },
    //     listContextParams,
    //   });
    //   setEstimatedRowsBefore(0);
    //   notImplemented();
    //   // setEstimatedRowsAfter(newEstimatedRowsAfter);
    //   return;
    // }
    // // When we reach the end with a forward/permalink anchor, anchor must be adjusted so issues don't overflow
    // // the virtual list. For forward anchors, anchor.index should equal estimatedRowsBefore
    // // so that issues[i] renders at virtualIndex = estimatedRowsBefore + i
    // if (
    //   atEnd &&
    //   anchor.type !== 'backward' &&
    //   anchor.index !== newEstimatedRowsBefore
    // ) {
    //   console.log('adjusting anchor for atEnd', {
    //     oldIndex: anchor.index,
    //     newIndex: newEstimatedRowsBefore,
    //     issuesLength: issues.length,
    //   });
    //   setQueryAnchor({
    //     anchor: {
    //       ...anchor,
    //       index: newEstimatedRowsBefore,
    //     },
    //     listContextParams,
    //   });
    //   setEstimatedRowsBefore(newEstimatedRowsBefore);
    //   setEstimatedRowsAfter(0);
    //   return;
    // }
    // // Update estimates from query results
    // if (newEstimatedRowsBefore !== estimatedRowsBefore) {
    //   const newAnchor: Anchor = {
    //     ...anchor,
    //     index: anchor.index + (newEstimatedRowsBefore - estimatedRowsBefore),
    //   };
    //   console.log('newAnchor', newAnchor.index);
    //   setQueryAnchor({
    //     anchor: newAnchor,
    //     listContextParams,
    //   });
    //   setEstimatedRowsBefore(newEstimatedRowsBefore);
    //   console.log('permalink scroll adjusted before', hasScrolledToPermalink);
    // }
    // if (newEstimatedRowsAfter !== estimatedRowsAfter) {
    //   setEstimatedRowsAfter(newEstimatedRowsAfter);
    // }
  }, [
    // newEstimatedRowsBefore,
    // newEstimatedRowsAfter,
    issues.length,
    atEnd,
    atStart,
    anchor.type,
    anchor.index,
  ]);

  useEffect(() => {
    if (issues.length > 0 || complete) {
      onReady();
    }
  }, [issues.length, complete, onReady]);

  // useEffect(() => {
  //   if (queryAnchor.listContextParams !== listContextParams) {
  //     if (listRef.current) {
  //       listRef.current.scrollTop = 0;
  //     }
  //     setEstimatedTotal(0);
  //     setTotal(undefined);
  //     setQueryAnchor({
  //       anchor: START_ANCHOR,
  //       listContextParams,
  //     });
  //   }
  // }, [listContextParams, queryAnchor]);

  // useEffect(() => {
  //   if (
  //     queryAnchor.listContextParams !== listContextParams ||
  //     anchor.direction === 'backward'
  //   ) {
  //     return;
  //   }
  //   const eTotal = anchor.index + issues.length;
  //   if (eTotal > estimatedTotal) {
  //     setEstimatedTotal(eTotal);
  //   }
  //   if (issuesResult.type === 'complete' && issues.length < pageSize) {
  //     setTotal(eTotal);
  //   }
  // }, [
  //   listContextParams,
  //   queryAnchor,
  //   issuesResult.type,
  //   issues,
  //   estimatedTotal,
  //   pageSize,
  // ]);

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

  const estimatedTotal =
    estimatedRowsBefore + issues.length + estimatedRowsAfter;

  const virtualizer = useVirtualizer({
    // count: total ?? estimatedTotal + NUM_ROWS_FOR_LOADING_SKELETON,
    count: estimatedTotal,
    estimateSize: () => ITEM_SIZE,
    overscan: 5,
    getScrollElement: () => listRef.current,
    initialOffset: () => {
      if (anchor.type === 'permalink') {
        return (anchor.index + pageSize / 2) * ITEM_SIZE;
      }
      // TODO(arv): is this correct?
      return 0; //ITEM_SIZE * anchor.index;
    },
  });

  // useEffect(() => {
  //   if (atStart && anchor.index !== 0) {
  //     console.log('hmmmm atStart adjusting scroll offset');
  //     setEstimatedRowsBefore(0);
  //     setQueryAnchor({
  //       anchor: TOP_ANCHOR,
  //       listContextParams,
  //     });
  //     return;
  //   }
  // }, [atStart, anchor.index]);

  // If the anchor is a permalink we need to adjust the scroll position once after we got the complete list.
  useEffect(() => {
    if (hasScrolledToPermalink) {
      return;
    }

    if (anchor.type === 'permalink' && complete) {
      // Find the actual index of the permalink item in the list. It might not be in the middle because there might not be full
      // pages before or after it.
      const issueArrayIndex = issues.findIndex(
        issue => issue.id === anchor.startRow.id,
      );
      console.log(
        'scrolling to issueArrayIndex',
        estimatedRowsBefore,
        issueArrayIndex,
        atStart,
      );
      // TODO(arv): Verify now that we do not have newEstimatedRowsBefore here
      virtualizer.scrollToIndex(
        (atStart ? 0 : estimatedRowsBefore) + issueArrayIndex,
        {
          align: 'start',
        },
      );
      setHasScrolledToPermalink(true);
    }
  }, [
    hasScrolledToPermalink,
    anchor.type,
    anchor.startRow?.id,
    complete,
    estimatedRowsBefore,
  ]);

  const virtualItems = virtualizer.getVirtualItems();

  // Apply pending scroll shift after new issues have loaded
  useEffect(() => {
    if (!pendingScrollShift || !complete) {
      return;
    }

    const {oldIssuesLength, newAnchor} = pendingScrollShift;
    const shift =
      newAnchor.type !== 'backward'
        ? newAnchor.index + oldIssuesLength - newAnchor.index
        : anchor.index - newAnchor.index;

    console.log('applying scroll shift', {
      shift,
      oldIssuesLength,
      newIssuesLength: issues.length,
      estimatedRowsBefore,
      newAnchorIndex: newAnchor.index,
    });

    if (shift > estimatedRowsBefore) {
      // We need to make room for the new rows we are loading before
      virtualizer.scrollToOffset(
        (virtualizer.scrollOffset ?? 0) + shift * ITEM_SIZE,
      );

      setQueryAnchor({
        anchor: {
          ...newAnchor,
          index: newAnchor.index + shift,
        },
        listContextParams,
      });
    }

    setEstimatedRowsAfter(estimatedRowsAfter + shift);
    setEstimatedRowsBefore(
      atStart ? 0 : Math.max(1, estimatedRowsBefore - shift),
    );

    setPendingScrollShift(null);
  }, [
    pendingScrollShift,
    complete,
    issues.length,
    anchor,
    estimatedRowsBefore,
    estimatedRowsAfter,
    atStart,
    listContextParams,
  ]);

  useEffect(() => {
    if (queryAnchor.listContextParams !== listContextParams) {
      return;
    }

    if (virtualItems.length === 0) {
      return;
    }

    const firstItem = virtualItems[0];
    const lastItem = virtualItems[virtualItems.length - 1];

    const nearPageEdgeThreshold = getNearPageEdgeThreshold(pageSize);

    // // Skip "anchoring to top" logic if we started from a permalink and virtualizer hasn't initialized yet
    // const shouldCheckAnchorToTop =
    //   queryAnchor.anchor.index === 0 || // Started from top, always check
    //   Math.abs(firstItem.index - anchor.index) < pageSize; // Or virtualizer has scrolled to anchor position

    // if (
    //   shouldCheckAnchorToTop &&
    //   anchor.index !== 0 &&
    //   firstItem.index <= nearPageEdgeThreshold
    // ) {
    //   // oxlint-disable-next-line no-console -- Debug logging in demo app
    //   console.log('anchoring to top', anchor.index, firstItem.index);
    //   setQueryAnchor({
    //     anchor: TOP_ANCHOR,
    //     listContextParams,
    //   });
    //   return;
    // }

    if (!complete) {
      return;
    }

    // Should we load more backward (page up)?
    const distanceFromStart =
      anchor.type === 'backward'
        ? firstItem.index - (anchor.index - issues.length)
        : firstItem.index - anchor.index;

    console.log(
      'distanceFromStart',
      distanceFromStart,
      'anchor.index',
      anchor.index,
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
      if (anchor.index !== 0 && anchor.type !== 'backward') {
        setEstimatedRowsBefore(0);
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
      virtualizer.scrollDirection === 'backward' &&
      !atStart &&
      distanceFromStart <= nearPageEdgeThreshold
    ) {
      // if (anchor.index !== 0 && distanceFromStart <= nearPageEdgeThreshold) {
      debugger;
      const issueArrayIndex = toBoundIssueArrayIndex(
        lastItem.index + nearPageEdgeThreshold * 2,
        anchor,
        issues.length,
      );
      const index = Math.max(
        // issues.length + pageSize, // leave headroom above current window
        toIndex(issueArrayIndex, anchor) - 1,
      );
      const newAnchor: Anchor = {
        index,
        type: 'backward',
        startRow: toStartRow(issues[issueArrayIndex]),
      };

      console.log('page up - setting new anchor', {
        index: newAnchor.index,
        type: newAnchor.type,
        startRow: newAnchor.startRow,
        oldIssuesLength: issues.length,
      });

      // Save the old issues.length so we can calculate shift after new issues load
      setPendingScrollShift({
        oldIssuesLength: issues.length,
        newAnchor,
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

    if (atEnd) {
      setEstimatedRowsAfter(0);
      return;
    }

    if (
      virtualizer.scrollDirection === 'forward' &&
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
        direction: 'forward',
        startRow: toStartRow(issues[issueArrayIndex]),
      });

      setQueryAnchor({
        anchor: {
          index,
          type: 'forward',
          startRow: toStartRow(issues[issueArrayIndex]),
        },
        listContextParams,
      });

      // When we page forward, we need to update estimatedRowsBefore to match the new anchor index
      setEstimatedRowsBefore(index);
    }
  }, [
    listContextParams,
    queryAnchor,
    issues,
    complete,
    pageSize,
    virtualItems,
  ]);

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

/////////////////////////////////////////////////////////
// New strategy

type CombinedPages = {
  issues: Issues;
  complete: boolean;
  // startRowXXX: StartRow | undefined;
  // endRowXXX: StartRow | undefined;
  atStart: boolean;
  atEnd: boolean;
  // estimatedRowsBefore: number;
  // estimatedRowsAfter: number;
};

function useIssues(
  listContext: ListContextParams,
  userID: string,
  pageSize: number,
  start: StartRow | null,
  kind: 'forward' | 'backward' | 'permalink',
  estimatedRowsBefore: number,
  options: UseQueryOptions,
): CombinedPages {
  if (kind === 'permalink') {
    assert(start !== null);
    assert(pageSize % 2 === 0);
    return usePermalinkPage(start, pageSize / 2, listContext, userID, options);
  }

  if (kind === 'forward') {
    if (start === null) {
      return useTopPage(pageSize, listContext, userID, options);
    }

    return useForwardPage(
      pageSize,
      listContext,
      userID,
      start,
      estimatedRowsBefore,
      options,
    );
  }

  kind satisfies 'backward';

  assert(start !== null);
  return useBackwardPage(pageSize, listContext, userID, start, options);
}

export function useTopPage(
  pageSize: number,
  listContext: ListContextParams,
  userID: string,
  useQueryOptions: UseQueryOptions,
): CombinedPages {
  // At start of list we know we are at the index 0 and at the start. We only have one page.

  const q = queries.issueListV2({
    listContext,
    userID,
    limit: pageSize + 1,
    start: null,
    dir: 'forward',
    inclusive: true, // not needed.
  });
  const [issues, result] = useQuery(q, useQueryOptions);

  // not used but needed to follow rules of hooks
  void useQuery(q, useQueryOptions);

  const atStart = true;
  const complete = result.type === 'complete';
  const atEnd = complete && issues.length <= pageSize;
  const slicedIssues = !atEnd ? issues.slice(0, pageSize) : issues;
  return {
    issues: slicedIssues,
    complete,
    atStart,
    atEnd,
  };
}

export function usePermalinkPage(
  middleRow: StartRow,
  pageSize: number,
  listContext: ListContextParams,
  userID: string,
  useQueryOptions: UseQueryOptions,
): CombinedPages {
  // This is the hook for when we have an anchor in the middle of the list and do not know the index.
  // We need to load two pages, one forward and one backward from the anchor.
  const qBefore = queries.issueListV2({
    listContext,
    userID,
    limit: pageSize + 1,
    start: middleRow,
    dir: 'backward',
    inclusive: true,
  });
  const qAfter = queries.issueListV2({
    listContext,
    userID,
    limit: pageSize + 1,
    start: middleRow,
    dir: 'forward',
    inclusive: true,
  });
  const [issuesBefore, resultBefore] = useQuery(qBefore, useQueryOptions);
  const [issuesAfter, resultAfter] = useQuery(qAfter, useQueryOptions);
  const completeBefore = resultBefore.type === 'complete';
  const completeAfter = resultAfter.type === 'complete';

  const atStart = completeBefore && issuesBefore.length <= pageSize;
  const atEnd = completeAfter && issuesAfter.length <= pageSize;

  const issues = joinIssues(issuesBefore, issuesAfter, pageSize);

  return {
    issues,
    complete: completeBefore && completeAfter,
    atStart,
    atEnd,
  };
}

export function useForwardPage(
  pageSize: number,
  listContext: ListContextParams,
  userID: string,
  start: StartRow,
  estimatedRowsBefore: number,
  useQueryOptions: UseQueryOptions,
): CombinedPages {
  const q = queries.issueListV2({
    listContext,
    userID,
    // overfetch by 1 to determine if there are more pages.
    limit: pageSize + 1,
    start,
    dir: 'forward',
    inclusive: false,
  });
  const [issues, result] = useQuery(q, useQueryOptions);
  // not used but needed to follow rules of hooks
  void useQuery(q, useQueryOptions);

  const complete = result.type === 'complete';
  const atStart = estimatedRowsBefore === 0;
  const atEnd = complete && issues.length <= pageSize;
  const slicedIssues = !atEnd ? issues.slice(0, pageSize) : issues;
  return {
    issues: slicedIssues,
    complete,
    atStart,
    atEnd,
  };
}

export function useBackwardPage(
  pageSize: number,
  listContext: ListContextParams,
  userID: string,
  start: StartRow,
  useQueryOptions: UseQueryOptions,
): CombinedPages {
  const q = queries.issueListV2({
    listContext,
    userID,
    // overfetch by 1 to determine if there are more pages.
    limit: pageSize + 1,
    start,
    dir: 'backward',
    inclusive: false,
  });
  const [issues, result] = useQuery(q, useQueryOptions);
  // not used but needed to follow rules of hooks
  void useQuery(q, useQueryOptions);

  // Backward query returns issues in descending position order (which is what we need)
  // so we should NOT reverse them
  const slicedIssues = issues.slice(0, pageSize);
  const complete = result.type === 'complete';
  const atStart = complete && issues.length <= pageSize;

  // Never atEnd when going backward because exclusive query
  const atEnd = false;

  return {
    issues: slicedIssues,
    complete,
    atStart,
    atEnd,
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
