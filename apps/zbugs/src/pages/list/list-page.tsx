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
  setupSlowQuery({delayMs: 1_000, unknownDataPercentage: 50});
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

    // // Very early issue
    // // index: 5
    // // subject: Replicator dies...
    // startRow: {
    //   id: 'HC7kWsm0qUYvf2BqjfiD_',
    //   modified: 1726188938000,
    //   created: 1726184763000,
    // },

    // // Early issue
    // // index: 45
    // // subject:RFE: enumerateCaches
    // startRow: {
    //   id: 'X-TwNXBDwTeQB0Mav31bU',
    //   modified: 1709537728000,
    //   created: 1669918206000,
    // },

    // // Second page
    // // index: 120
    // // subject: app-publish function needs more Memory
    startRow: {
      id: 'Us_A9kc4ldfHuChlbKeU6',
      modified: 1701202102000,
      created: 1698518825000,
    },

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
  const [pendingScrollAdjustment, setPendingScrollAdjustment] =
    useState<number>(0);

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

  const anchor = useMemo(
    () =>
      queryAnchor.listContextParams === listContextParams
        ? queryAnchor.anchor
        : START_ANCHOR,
    [queryAnchor.listContextParams, queryAnchor.anchor, listContextParams],
  );

  const [estimatedTotal, setEstimatedTotal] = useState(
    anchor.index + NUM_ROWS_FOR_LOADING_SKELETON,
  );

  const [hasScrolledToPermalink, setHasScrolledToPermalink] = useState(false);
  const [skipPagingLogic, setSkipPagingLogic] = useState(false);
  const skipPagingLogicRef = useRef<boolean>(false);

  const [hasReachedEnd, setHasReachedEnd] = useState(false);
  const [hasReachedStart, setHasReachedStart] = useState(false);

  // const total = hasReachedStart && hasReachedEnd ? estimatedTotal : undefined;

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
  } = useIssues(
    listContextParams,
    z.userID,
    pageSize,
    anchor.startRow ?? null,
    anchor.type,
    anchor.index,
    textFilterQuery === textFilter ? CACHE_NAV : CACHE_NONE,
  );

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
    if (issuesEmpty) {
      return;
    }

    if (queryAnchor.listContextParams !== listContextParams) {
      return;
    }

    if (
      skipPagingLogicRef.current &&
      skipPagingLogic &&
      pendingScrollAdjustment === 0
    ) {
      // TODO(arv): See if we can remove the state and only use the ref
      setSkipPagingLogic(false);
      skipPagingLogicRef.current = false;
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
      skipPagingLogicRef.current = true;

      return;
    }

    // First issue is before start of list - need to shift down
    if (firstIssueIndex < 0) {
      const placeholderRows = !atStart ? NUM_ROWS_FOR_LOADING_SKELETON : 0;
      const offset = -firstIssueIndex + placeholderRows;

      setSkipPagingLogic(true);
      skipPagingLogicRef.current = true;
      setPendingScrollAdjustment(offset);
      setQueryAnchor({
        anchor: {
          ...anchor,
          index: anchor.index + offset,
        },
        listContextParams,
      });
      return;
    }

    if (atStart && firstIssueIndex > 0) {
      setPendingScrollAdjustment(-firstIssueIndex);
      setQueryAnchor({
        anchor: TOP_ANCHOR,
        listContextParams,
      });
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
    queryAnchor.listContextParams,
    // virtualizer,
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
    if (queryAnchor.listContextParams !== listContextParams) {
      // if (listRef.current) {
      //   listRef.current.scrollTop = 0;
      // }

      setEstimatedTotal(0);
      setHasReachedStart(true);
      setHasReachedEnd(false);

      // setQueryAnchor({
      //   anchor: TOP_ANCHOR,
      //   listContextParams,
      // });
      setQueryAnchor({
        anchor,
        listContextParams,
      });
    }
  }, [listContextParams, queryAnchor]);

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
    const toIssueArrayIndex = (
      index: number,
      anchor: Anchor,
      firstIssueIndex: number,
    ) =>
      anchor.type !== 'backward'
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
          {' '}
          <RowDebugInfo index={index} issueArrayIndex={issueArrayIndex} />
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

  const virtualizer = useVirtualizer({
    count:
      Math.max(estimatedTotal, newEstimatedTotal) +
      (!atEnd ? NUM_ROWS_FOR_LOADING_SKELETON : 0),
    estimateSize: () => ITEM_SIZE,
    overscan: 5,
    getScrollElement: () => listRef.current,
    initialOffset: () => {
      if (anchor.type === 'permalink') {
        return anchor.index * ITEM_SIZE;
      }
      return 0;
    },
  });

  const virtualItems = virtualizer.getVirtualItems();

  useEffect(() => {
    if (queryAnchor.listContextParams !== listContextParams) {
      return;
    }

    if (virtualItems.length === 0) {
      return;
    }

    if (!complete) {
      return;
    }

    if (skipPagingLogic || pendingScrollAdjustment !== 0) {
      return;
    }

    if (atStart) {
      if (firstIssueIndex !== 0) {
        setQueryAnchor({
          anchor: TOP_ANCHOR,
          listContextParams,
        });
        return;
      }
    }

    const firstItem = virtualItems[0];
    const lastItem = virtualItems[virtualItems.length - 1];
    const nearPageEdgeThreshold = getNearPageEdgeThreshold(pageSize);

    const distanceFromStart = firstItem.index - firstIssueIndex;
    const distanceFromEnd = firstIssueIndex + issuesLength - lastItem.index;

    if (!atStart && distanceFromStart <= nearPageEdgeThreshold) {
      // When we scroll really fast up and the loading is slow we can end up with a page of only placeholders.
      // When that happens we use the first issue index as the anchor index which allows us to keep going.
      const index = clampIndex(
        lastItem.index + 2 * nearPageEdgeThreshold,
        firstIssueIndex,
        issuesLength,
      );

      setQueryAnchor({
        anchor: {
          index,
          type: 'backward',
          startRow: issueAt(index),
        },
        listContextParams,
      });

      return;
    }

    if (!atEnd && distanceFromEnd <= nearPageEdgeThreshold) {
      // When we scroll really fast down and the loading is slow we can end up with a page of only placeholders.
      // When that happens we use the last possible issue index as the anchor index which allows us to keep going.
      const index = clampIndex(
        firstItem.index - 2 * nearPageEdgeThreshold,
        firstIssueIndex,
        issuesLength,
      );

      setQueryAnchor({
        anchor: {
          index: index + 1,
          type: 'forward',
          startRow: issueAt(index),
        },
        listContextParams,
      });
      return;
    }
  }, [
    listContextParams,
    queryAnchor.listContextParams,
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

  console.log('XXX anchor', {
    // 'listRef.current?.scrollTop': listRef.current?.scrollTop,
    'index': anchor.index,
    'type': anchor.type,
    'startRow.?id': anchor.startRow?.id,
    // 'estimatedTotal': estimatedTotal,
  });

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
        {size && !issuesEmpty ? (
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

type Issue = Row<ReturnType<typeof queries.issueListV2>>;
type Issues = Issue[];

function useIssues(
  listContext: ListContextParams,
  userID: string,
  pageSize: number,
  start: StartRow | null,
  kind: 'forward' | 'backward' | 'permalink',
  anchorIndex: number,
  options: UseQueryOptions,
): {
  issueAt: (index: number) => Issue | undefined;
  issuesLength: number;
  complete: boolean;
  issuesEmpty: boolean;
  atStart: boolean;
  atEnd: boolean;
  firstIssueIndex: number;
} {
  // Conditionally use useSlowQuery or useQuery based on USE_SLOW_QUERY flag
  const queryFn = USE_SLOW_QUERY ? useSlowQuery : useQuery;

  if (kind === 'permalink') {
    assert(start !== null);
    assert(pageSize % 2 === 0);

    const halfPageSize = pageSize / 2;
    const qBefore = queries.issueListV2({
      listContext,
      userID,
      limit: halfPageSize + 1,
      start,
      dir: 'backward',
      inclusive: false,
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

    const issuesBeforeSize = Math.min(issuesBefore.length, halfPageSize);
    const issuesAfterSize = Math.min(issuesAfter.length, halfPageSize);

    const firstIssueIndex = anchorIndex - issuesBeforeSize;

    return {
      // issues,
      issueAt: (index: number) => {
        if (index >= anchorIndex) {
          if (index - anchorIndex >= issuesAfterSize) {
            return undefined;
          }
          return issuesAfter[index - anchorIndex];
        }
        assert(index < anchorIndex);
        if (anchorIndex - index > issuesBeforeSize) {
          return undefined;
        }
        return issuesBefore[anchorIndex - index - 1];
      },
      issuesLength: issuesBeforeSize + issuesAfterSize,
      complete: completeBefore && completeAfter,
      issuesEmpty: issuesBeforeSize === 0 && issuesAfterSize === 0,
      atStart: completeBefore && issuesBefore.length <= halfPageSize,
      atEnd: completeAfter && issuesAfter.length <= halfPageSize,
      firstIssueIndex,
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
  const issuesLength = hasMoreIssues ? pageSize : issues.length;
  const issuesEmpty = issues.length === 0;

  if (kind === 'forward') {
    return {
      // issues: slicedIssues,
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
    };
  }

  kind satisfies 'backward';
  assert(start !== null);

  return {
    // issues: slicedIssues,
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
  };
}

/**
 * Clamps an index to be within the valid range of issues.
 * @param targetIndex - The desired index to clamp
 * @param firstIssueIndex - The first valid issue index
 * @param issuesLength - The number of issues available
 * @returns The clamped index within [firstIssueIndex, firstIssueIndex + issuesLength - 1]
 */
function clampIndex(
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
