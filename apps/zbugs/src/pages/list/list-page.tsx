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
import {toast} from 'react-toastify';
import {assert} from 'shared/src/asserts.ts';
import {useDebouncedCallback} from 'use-debounce';
import {useLocation, useParams, useSearch} from 'wouter';
import {useHistoryState} from 'wouter/use-browser-location';
import * as zod from 'zod/mini';
import {must} from '../../../../../packages/shared/src/must.ts';
import {
  issueRowSortSchema,
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
import {
  appendParam,
  navigate,
  removeParam,
  replaceHistoryState,
  setParam,
} from '../../navigate.ts';
import {recordPageLoad} from '../../page-load-stats.ts';
import {mark} from '../../perf-log.ts';
import {CACHE_NAV, CACHE_NONE} from '../../query-cache-policy.ts';
import {isGigabugs, useListContext} from '../../routes.tsx';
import {preload} from '../../zero-preload.ts';
import {getIDFromString} from '../issue/get-id.tsx';
import {ToastContainer, ToastContent} from '../issue/toast-content.tsx';

// Set to true to enable slow query simulation (half data → full data after 1s)
const DEBUG_QUERY = import.meta.env.DEV && false;
if (DEBUG_QUERY) {
  setupSlowQuery({delayMs: 1_000, unknownDataPercentage: 50});
}

let firstRowRendered = false;
const ITEM_SIZE = 56;
// Make sure this is even since we half it for permalink loading
const MIN_PAGE_SIZE = 100;
const NUM_ROWS_FOR_LOADING_SKELETON = 1;

export type Anchor = zod.infer<typeof anchorSchema>;

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
  kind: 'forward',
  startRow: undefined,
});

// First issue
// index: 0
//
// id: 'HdpMkgbHpK3_OcOIiQOuW',

// Very early issue
// index: 5
// subject: Replicator dies...
// id: 'HC7kWsm0qUYvf2BqjfiD_',

// Early issue
// index: 45
// subject:RFE: enumerateCaches
// id: 'X-TwNXBDwTeQB0Mav31bU',

// Second page
// index: 120
// subject: app-publish function needs more Memory
// id: 'Us_A9kc4ldfHuChlbKeU6',

// Middle issue
// index: 260
// title: Evaluate if we should return a ClientStateNotFoundResponse ...
// id: '0zTrvA-6aVO8eNdHBoW7G',

// Close to bottom
// index: 500
// Subject: Add DevTools
// id: 'mrh64by3B9b6MRHbzkLQP',

// Even closer to bottom
// title: Can we support...
// index: 512
// id: '8tyDj9FUJWQ5qd2JEP3KS',

// Last issue
// index: 515
// subject: docs: Add something about offline ...
// id: '4wBDlh9b774qfGD3pWe6d',

const anchorSchema = zod.discriminatedUnion('kind', [
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
      // startRow: issueRowSortSchema,
      id: zod.string(),
    }),
  ),
]);

const permalinkHistoryStateSchema = zod.readonly(
  zod.object({
    anchor: anchorSchema,
    scrollTop: zod.number(),
    estimatedTotal: zod.number(),
    hasReachedStart: zod.boolean(),
    hasReachedEnd: zod.boolean(),
  }),
);

type PermalinkHistoryState = zod.infer<typeof permalinkHistoryStateSchema>;

const getNearPageEdgeThreshold = (pageSize: number) => Math.ceil(pageSize / 10);

function RowDebugInfo({
  index,
  issueArrayIndex,
  issue,
  anchor,
  scrollTop,
}: {
  index: number;
  issueArrayIndex: number;
  issue?: Row['issue'] | undefined;
  anchor: Anchor;
  scrollTop: number;
}) {
  if (!DEBUG_QUERY) {
    return null;
  }

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
      title={`Click to copy permalink anchor
anchor: ${JSON.stringify(anchor, null, 2)}
scrollTop: ${scrollTop}`}
    >
      <span>{index}</span>,
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

  const permalinkID = qs.get('id');

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
        permalinkID,
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
      permalinkID,
    ],
  );

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

    document.title =
      `Zero Bugs → ${listContext.title}` +
      (permalinkID ? ` → Issue ${permalinkID}` : '');
  }, [listContext]);

  const listRef = useRef<HTMLDivElement>(null);
  const tableWrapperRef = useRef<HTMLDivElement>(null);
  const size = useElementSize(tableWrapperRef);

  // TODO:(arv): DRY
  const maybeHistoryState = useHistoryState<PermalinkHistoryState | null>();
  const res = permalinkHistoryStateSchema.safeParse(maybeHistoryState);
  const historyState = res.success ? res.data : null;

  // Initialize queryAnchor from history.state directly to avoid Strict Mode double-mount issues
  const [queryAnchor, setQueryAnchor] = useState<QueryAnchor>(() => {
    const {state} = history;
    const parseResult = permalinkHistoryStateSchema.safeParse(state);
    const anchor =
      parseResult.success && parseResult.data.anchor
        ? parseResult.data.anchor
        : permalinkID
          ? ({
              index: NUM_ROWS_FOR_LOADING_SKELETON,
              kind: 'permalink',
              id: permalinkID,
            } satisfies Anchor)
          : TOP_ANCHOR;
    return {
      anchor,
      listContextParams,
    };
  });

  // oxlint-disable-next-line no-explicit-any
  (globalThis as any).permalinkNavigate = (id: string | number) => {
    navigate(setParam(qs, 'id', String(id)));
  };

  const [pendingScrollAdjustment, setPendingScrollAdjustment] =
    useState<number>(0);

  const updateAnchor = (anchor: Anchor) => {
    setQueryAnchor({
      anchor,
      listContextParams,
    });
    if (!anchorEquals(historyState?.anchor, anchor)) {
      replaceHistoryState<PermalinkHistoryState>({
        anchor,
        scrollTop: virtualizer.scrollOffset ?? 0,
        estimatedTotal,
        hasReachedStart,
        hasReachedEnd,
      });
    }
  };

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

  const isListContextCurrent = useMemo(
    () => queryAnchor.listContextParams === listContextParams,
    [queryAnchor.listContextParams, listContextParams],
  );

  const anchor = useMemo(() => {
    if (isListContextCurrent) {
      return queryAnchor.anchor;
    }

    // TODO(arv): DRY
    if (permalinkID) {
      return {
        index: NUM_ROWS_FOR_LOADING_SKELETON,
        kind: 'permalink',
        id: permalinkID,
      } satisfies Anchor;
    }
    return TOP_ANCHOR;
  }, [isListContextCurrent, queryAnchor.anchor]);

  const [estimatedTotal, setEstimatedTotal] = useState(
    NUM_ROWS_FOR_LOADING_SKELETON,
  );

  const [skipPagingLogic, setSkipPagingLogic] = useState(false);
  const [hasReachedEnd, setHasReachedEnd] = useState(false);
  const [hasReachedStart, setHasReachedStart] = useState(false);
  const [isScrolling, setIsScrolling] = useState(false);

  // We don't want to cache every single keystroke. We already debounce
  // keystrokes for the URL, so we just reuse that.
  const {
    issueAt,
    issuesLength,
    complete,
    issuesEmpty,
    atStart,
    atEnd,
    firstIssueIndex,
    permalinkNotFound,
  } = useIssues(
    listContextParams,
    z.userID,
    pageSize,
    anchor,
    !isScrolling && textFilterQuery === textFilter ? CACHE_NAV : CACHE_NONE,
  );

  useEffect(() => {
    if (permalinkNotFound) {
      const toastID = 'permalink-issue-not-found';
      toast(
        <ToastContent toastID={toastID}>
          Permalink issue not found
        </ToastContent>,
        {
          toastId: toastID,
          containerId: 'bottom',
        },
      );
      navigate(removeParam(qs, 'id'), {replace: true});
    }
  }, [permalinkNotFound, permalinkID]);

  useEffect(() => {
    if (atStart) {
      setHasReachedStart(true);
    }
  }, [atStart]);

  useEffect(() => {
    if (atEnd) {
      setHasReachedEnd(true);
    }
  }, [atEnd]);

  useEffect(() => {
    if (issuesEmpty || !isListContextCurrent) {
      return;
    }

    if (skipPagingLogic && pendingScrollAdjustment === 0) {
      setSkipPagingLogic(false);
      return;
    }

    // There is a pending scroll adjustment from last anchor change.
    if (pendingScrollAdjustment !== 0) {
      virtualizer.scrollToOffset(
        (virtualizer.scrollOffset ?? 0) + pendingScrollAdjustment * ITEM_SIZE,
      );

      setEstimatedTotal(estimatedTotal + pendingScrollAdjustment);

      setPendingScrollAdjustment(0);
      setSkipPagingLogic(true);

      return;
    }

    // First issue is before start of list - need to shift down
    if (firstIssueIndex < 0) {
      const placeholderRows = !atStart ? NUM_ROWS_FOR_LOADING_SKELETON : 0;
      const offset = -firstIssueIndex + placeholderRows;

      setSkipPagingLogic(true);
      // skipPagingLogicRef.current = true;
      setPendingScrollAdjustment(offset);
      const newAnchor = {
        ...anchor,
        index: anchor.index + offset,
      };
      updateAnchor(newAnchor);
      return;
    }

    if (atStart && firstIssueIndex > 0) {
      setPendingScrollAdjustment(-firstIssueIndex);
      updateAnchor(TOP_ANCHOR);
      return;
    }
  }, [
    firstIssueIndex,
    anchor,
    atStart,
    pendingScrollAdjustment,
    skipPagingLogic,
    issuesEmpty,
    listContextParams,
    isListContextCurrent,
    estimatedTotal,
    // virtualizer, Do not depend on virtualizer. TDZ.
  ]);

  const newEstimatedTotal = firstIssueIndex + issuesLength;

  useEffect(() => {
    if (complete && newEstimatedTotal > estimatedTotal) {
      setEstimatedTotal(newEstimatedTotal);
    }
  }, [estimatedTotal, complete, newEstimatedTotal]);

  const total = hasReachedStart && hasReachedEnd ? estimatedTotal : undefined;

  useEffect(() => {
    if (!issuesEmpty || complete) {
      onReady();
    }
  }, [issuesEmpty, complete, onReady]);

  useEffect(() => {
    if (!isListContextCurrent) {
      if (historyState) {
        if (listRef.current) {
          listRef.current.scrollTop = historyState.scrollTop;
        }
        setEstimatedTotal(historyState.estimatedTotal);
        setHasReachedStart(historyState.hasReachedStart);
        setHasReachedEnd(historyState.hasReachedEnd);
        updateAnchor(historyState.anchor);
      } else if (permalinkID) {
        if (listRef.current) {
          listRef.current.scrollTop = NUM_ROWS_FOR_LOADING_SKELETON * ITEM_SIZE;
        }
        setEstimatedTotal(NUM_ROWS_FOR_LOADING_SKELETON);
        setHasReachedStart(false);
        setHasReachedEnd(false);
        updateAnchor({
          id: permalinkID,
          index: NUM_ROWS_FOR_LOADING_SKELETON,
          kind: 'permalink',
        });
      } else {
        if (listRef.current) {
          listRef.current.scrollTop = 0;
        }
        // virtualizer.scrollToOffset(0);
        setEstimatedTotal(0);
        setHasReachedStart(true);
        setHasReachedEnd(false);
        updateAnchor(TOP_ANCHOR);
      }
      setSkipPagingLogic(true);
    }
  }, [isListContextCurrent]);

  useEffect(() => {
    if (complete) {
      recordPageLoad('list-page');
      preload(z, projectName);
    }
  }, [login.loginState?.decoded, complete, z]);

  const onDeleteFilter = (e: React.MouseEvent) => {
    const target = e.currentTarget;
    const key = target.getAttribute('data-key');
    const value = target.getAttribute('data-value');
    if (key && value) {
      navigate(removeParam(removeParam(qs, key, value), 'id'));
    }
  };

  const onFilter = useCallback(
    (selection: Selection) => {
      const qsWithoutID = removeParam(qs, 'id');
      if ('creator' in selection) {
        navigate(setParam(qsWithoutID, 'creator', selection.creator));
      } else if ('assignee' in selection) {
        navigate(setParam(qsWithoutID, 'assignee', selection.assignee));
      } else {
        navigate(appendParam(qsWithoutID, 'label', selection.label));
      }
    },
    [qs],
  );

  const toggleSortField = useCallback(() => {
    navigate(
      setParam(
        removeParam(qs, 'id'),
        'sort',
        sortField === 'created' ? 'modified' : 'created',
      ),
    );
  }, [qs, sortField]);

  const toggleSortDirection = useCallback(() => {
    navigate(
      setParam(
        removeParam(qs, 'id'),
        'sortDir',
        sortDirection === 'asc' ? 'desc' : 'asc',
      ),
    );
  }, [qs, sortDirection]);

  const updateTextFilterQueryString = useDebouncedCallback((text: string) => {
    navigate(setParam(removeParam(qs, 'id'), 'q', text));
  }, 500);

  const onTextFilterChange = (text: string) => {
    setTextFilter(text);
    updateTextFilterQueryString(text);
  };

  const clearAndHideSearch = () => {
    if (searchMode) {
      setTextFilter(null);
      setForceSearchMode(false);
      navigate(removeParam(removeParam(qs, 'id'), 'q'));
    }
  };

  const Row = ({index, style}: {index: number; style: CSSProperties}) => {
    const toIssueArrayIndex = (
      index: number,
      anchor: Anchor,
      firstIssueIndex: number,
    ) =>
      anchor.kind !== 'backward'
        ? index - firstIssueIndex
        : anchor.index - index;

    const issue = issueAt(index); //issues[issueArrayIndex];
    const issueArrayIndex = toIssueArrayIndex(index, anchor, firstIssueIndex);
    if (issue === undefined) {
      return (
        <div
          className={classNames('row', 'skeleton-shimmer')}
          style={{
            ...style,
          }}
        >
          <RowDebugInfo
            index={index}
            issueArrayIndex={issueArrayIndex}
            anchor={anchor}
            scrollTop={virtualizer.scrollOffset ?? 0}
          />
        </div>
      );
    }

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
          {
            // TODO(arv): Extract into something cleaner
            permalink:
              issue.id === permalinkID || String(issue.shortID) === permalinkID,
          },
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
            anchor={anchor}
            scrollTop={virtualizer.scrollOffset ?? 0}
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

  const virtualizer = useVirtualizer({
    count:
      Math.max(estimatedTotal, newEstimatedTotal) +
      (!atEnd ? NUM_ROWS_FOR_LOADING_SKELETON : 0),
    estimateSize: () => ITEM_SIZE,
    overscan: 5,
    getScrollElement: () => listRef.current,
    initialOffset: () => {
      if (historyState?.scrollTop !== undefined) {
        return historyState.scrollTop;
      }
      if (anchor.kind === 'permalink') {
        return anchor.index * ITEM_SIZE;
      }
      return 0;
    },
  });

  useEffect(() => {
    setIsScrolling(virtualizer.isScrolling);
  }, [virtualizer.isScrolling]);

  const virtualItems = virtualizer.getVirtualItems();

  const updateHistoryState = useDebouncedCallback(() => {
    replaceHistoryState<PermalinkHistoryState>({
      anchor,
      scrollTop: virtualizer.scrollOffset ?? 0,
      estimatedTotal,
      hasReachedStart,
      hasReachedEnd,
    });
  }, 100);

  useEffect(() => {
    updateHistoryState();
  }, [
    anchor,
    virtualizer.scrollOffset,
    estimatedTotal,
    hasReachedStart,
    hasReachedEnd,
    updateHistoryState,
  ]);

  useEffect(() => {
    if (
      !isListContextCurrent ||
      virtualItems.length === 0 ||
      !complete ||
      skipPagingLogic ||
      pendingScrollAdjustment !== 0
    ) {
      return;
    }

    if (atStart) {
      if (firstIssueIndex !== 0) {
        updateAnchor(TOP_ANCHOR);
        return;
      }
    }

    const updateAnchorForEdge = (
      targetIndex: number,
      type: 'forward' | 'backward',
      indexOffset: number,
    ) => {
      const index = toBoundIndex(targetIndex, firstIssueIndex, issuesLength);
      const startRow = issueAt(index);
      assert(startRow !== undefined || type === 'forward');
      updateAnchor({
        index: index + indexOffset,
        kind: type,
        startRow,
      } as Anchor);
    };

    const firstItem = virtualItems[0];
    const lastItem = virtualItems[virtualItems.length - 1];
    const nearPageEdgeThreshold = getNearPageEdgeThreshold(pageSize);

    const distanceFromStart = firstItem.index - firstIssueIndex;
    const distanceFromEnd = firstIssueIndex + issuesLength - lastItem.index;

    if (!atStart && distanceFromStart <= nearPageEdgeThreshold) {
      updateAnchorForEdge(
        lastItem.index + 2 * nearPageEdgeThreshold,
        'backward',
        0,
      );
      return;
    }

    if (!atEnd && distanceFromEnd <= nearPageEdgeThreshold) {
      updateAnchorForEdge(
        firstItem.index - 2 * nearPageEdgeThreshold,
        'forward',
        1,
      );
      return;
    }
  }, [
    listContextParams,
    isListContextCurrent,
    virtualItems,
    skipPagingLogic,
    pendingScrollAdjustment,
    complete,
    pageSize,
    firstIssueIndex,
    issuesLength,
    atStart,
    atEnd,
    issueAt,
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
        <ToastContainer position="bottom" />
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
        {size && !issuesEmpty ? (
          <div
            style={{
              width: size.width,
              height: size.height,
              overflow: 'auto',
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

function roundEstimatedTotal(estimatedTotal: number) {
  return estimatedTotal < 50
    ? estimatedTotal
    : estimatedTotal - (estimatedTotal % 50);
}

function formatIssueCountEstimate(count: number) {
  if (count < 1000) {
    return count;
  }
  return `~${Math.floor(count / 1000).toLocaleString()}k`;
}

type Issue = Row<ReturnType<typeof queries.issueListV2>>;
type Issues = Issue[];

function useIssues(
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
  // Conditionally use useSlowQuery or useQuery based on USE_SLOW_QUERY flag
  const queryFn = DEBUG_QUERY ? useSlowQuery : useQuery;

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

    const [issue, resultIssue] = queryFn(qItem, options);
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

    const [issuesBefore, resultBefore] = queryFn(qBefore, options);
    const [issuesAfter, resultAfter] = queryFn(qAfter, options);
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
  const [issues, result]: [Issues, {type: string}] = queryFn(
    q,
    options,
  ) as unknown as [Issues, {type: string}];
  // not used but needed to follow rules of hooks
  void queryFn(null, options);
  void queryFn(null, options);

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

/**
 * Clamps an index to be within the valid range of issues.
 * @param targetIndex - The desired index to clamp
 * @param firstIssueIndex - The first valid issue index
 * @param issuesLength - The number of issues available
 * @returns The clamped index within [firstIssueIndex, firstIssueIndex + issuesLength - 1]
 */
function toBoundIndex(
  targetIndex: number,
  firstIssueIndex: number,
  issuesLength: number,
): number {
  return Math.max(
    firstIssueIndex,
    Math.min(firstIssueIndex + issuesLength - 1, targetIndex),
  );
}

function makeEven(n: number) {
  return n % 2 === 0 ? n : n + 1;
}

function anchorEquals(anchor: Anchor | undefined, other: Anchor): boolean {
  if (
    anchor === undefined ||
    anchor.index !== other.index ||
    anchor.kind !== other.kind
  ) {
    return false;
  }

  if (anchor.kind === 'permalink') {
    return anchor.id === (other as Extract<Anchor, {id: string}>).id;
  }

  const o = other as Extract<Anchor, {startRow?: IssueRowSort}>;
  const {startRow} = anchor;
  if (startRow === undefined) {
    return startRow === o.startRow;
  }

  return (
    startRow?.id === o.startRow?.id &&
    startRow?.modified === o.startRow?.modified &&
    startRow?.created === o.startRow?.created
  );
}
