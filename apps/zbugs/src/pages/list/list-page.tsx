import type {Row} from '@rocicorp/zero';
import {useQuery, useZero} from '../../../shared/zero-hooks.ts';
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
import {useDebouncedCallback} from 'use-debounce';
import {useLocation, useParams, useSearch} from 'wouter';
import {navigate} from 'wouter/use-browser-location';
import {must} from '../../../../../packages/shared/src/must.ts';
import {
  queries,
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
const MIN_PAGE_SIZE = 100;
const NUM_ROWS_FOR_LOADING_SKELETON = 1;

type Anchor = {
  startRow: Row['issue'] | undefined;
  direction: 'forward' | 'backward';
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

const TOP_ANCHOR = Object.freeze({
  startRow: undefined,
  direction: 'forward',
  index: 0,
});

const getNearPageEdgeThreshold = (pageSize: number) => Math.ceil(pageSize / 10);

const toIssueArrayIndex = (index: number, anchor: Anchor) =>
  anchor.direction === 'forward' ? index - anchor.index : anchor.index - index;

const toBoundIssueArrayIndex = (
  index: number,
  anchor: Anchor,
  length: number,
) => Math.min(length - 1, Math.max(0, toIssueArrayIndex(index, anchor)));

const toIndex = (issueArrayIndex: number, anchor: Anchor) =>
  anchor.direction === 'forward'
    ? issueArrayIndex + anchor.index
    : anchor.index - issueArrayIndex;

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
    anchor: TOP_ANCHOR,
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
      ? Math.max(MIN_PAGE_SIZE, Math.ceil(size?.height / ITEM_SIZE) * 3)
      : MIN_PAGE_SIZE;
    if (newPageSize > pageSize) {
      setPageSize(newPageSize);
    }
  }, [pageSize, size]);

  const anchor =
    queryAnchor.listContextParams === listContextParams
      ? queryAnchor.anchor
      : TOP_ANCHOR;

  const q = queries.issueListV2({
    listContext: listContextParams,
    userID: z.userID,
    limit: pageSize,
    start: anchor.startRow
      ? {
          id: anchor.startRow.id,
          modified: anchor.startRow.modified,
          created: anchor.startRow.created,
        }
      : null,
    dir: anchor.direction,
  });

  const [estimatedTotal, setEstimatedTotal] = useState(0);
  const [total, setTotal] = useState<number | undefined>(undefined);

  // We don't want to cache every single keystroke. We already debounce
  // keystrokes for the URL, so we just reuse that.
  const [issues, issuesResult] = useQuery(
    q,
    textFilterQuery === textFilter ? CACHE_NAV : CACHE_NONE,
  );

  useEffect(() => {
    if (issues.length > 0 || issuesResult.type === 'complete') {
      onReady();
    }
  }, [issues.length, issuesResult.type, onReady]);

  useEffect(() => {
    if (queryAnchor.listContextParams !== listContextParams) {
      if (listRef.current) {
        listRef.current.scrollTop = 0;
      }
      setEstimatedTotal(0);
      setTotal(undefined);
      setQueryAnchor({
        anchor: TOP_ANCHOR,
        listContextParams,
      });
    }
  }, [listContextParams, queryAnchor]);

  useEffect(() => {
    if (
      queryAnchor.listContextParams !== listContextParams ||
      anchor.direction !== 'forward'
    ) {
      return;
    }
    const eTotal = anchor.index + issues.length;
    if (eTotal > estimatedTotal) {
      setEstimatedTotal(eTotal);
    }
    if (issuesResult.type === 'complete' && issues.length < pageSize) {
      setTotal(eTotal);
    }
  }, [
    listContextParams,
    queryAnchor,
    issuesResult.type,
    issues,
    estimatedTotal,
    pageSize,
  ]);

  useEffect(() => {
    if (issuesResult.type === 'complete') {
      recordPageLoad('list-page');
      preload(z, projectName);
    }
  }, [login.loginState?.decoded, issuesResult.type, z]);

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
        ></div>
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
    count: total ?? estimatedTotal + NUM_ROWS_FOR_LOADING_SKELETON,
    estimateSize: () => ITEM_SIZE,
    overscan: 5,
    getScrollElement: () => listRef.current,
  });

  const virtualItems = virtualizer.getVirtualItems();
  useEffect(() => {
    if (queryAnchor.listContextParams !== listContextParams) {
      return;
    }
    const [firstItem] = virtualItems;
    const lastItem = virtualItems[virtualItems.length - 1];
    if (!lastItem) {
      return;
    }

    if (
      anchor.index !== 0 &&
      firstItem.index <= getNearPageEdgeThreshold(pageSize)
    ) {
      // oxlint-disable-next-line no-console -- Debug logging in demo app
      console.log('anchoring to top');
      setQueryAnchor({
        anchor: TOP_ANCHOR,
        listContextParams,
      });
      return;
    }

    if (issuesResult.type !== 'complete') {
      return;
    }

    const hasPrev = anchor.index !== 0;
    const distanceFromStart =
      anchor.direction === 'backward'
        ? firstItem.index - (anchor.index - issues.length)
        : firstItem.index - anchor.index;

    const nearPageEdgeThreshold = getNearPageEdgeThreshold(pageSize);

    if (hasPrev && distanceFromStart <= nearPageEdgeThreshold) {
      const issueArrayIndex = toBoundIssueArrayIndex(
        lastItem.index + nearPageEdgeThreshold * 2,
        anchor,
        issues.length,
      );
      const index = toIndex(issueArrayIndex, anchor) - 1;
      const a = {
        index,
        direction: 'backward',
        startRow: issues[issueArrayIndex],
      } as const;
      // oxlint-disable-next-line no-console -- Debug logging in demo app
      console.log('page up', a);
      setQueryAnchor({
        anchor: a,
        listContextParams,
      });
      return;
    }

    const hasNext =
      anchor.direction === 'backward' || issues.length === pageSize;
    const distanceFromEnd =
      anchor.direction === 'backward'
        ? anchor.index - lastItem.index
        : anchor.index + issues.length - lastItem.index;
    if (hasNext && distanceFromEnd <= nearPageEdgeThreshold) {
      const issueArrayIndex = toBoundIssueArrayIndex(
        firstItem.index - nearPageEdgeThreshold * 2,
        anchor,
        issues.length,
      );
      const index = toIndex(issueArrayIndex, anchor) + 1;
      const a = {
        index,
        direction: 'forward',
        startRow: issues[issueArrayIndex],
      } as const;
      // oxlint-disable-next-line no-console -- Debug logging in demo app
      console.log('page down', a);
      setQueryAnchor({
        anchor: a,
        listContextParams,
      });
    }
  }, [
    listContextParams,
    queryAnchor,
    issues,
    issuesResult,
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
          {issuesResult.type === 'complete' || total || estimatedTotal ? (
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
            style={{width: size.width, height: size.height, overflow: 'auto'}}
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
