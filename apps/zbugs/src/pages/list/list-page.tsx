import {useQuery} from '@rocicorp/zero/react';
import {useVirtualizer} from '@tanstack/react-virtual';
import classNames from 'classnames';
import React, {
  type CSSProperties,
  type KeyboardEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import {useDebouncedCallback} from 'use-debounce';
import {useSearch} from 'wouter';
import {navigate} from 'wouter/use-browser-location';
import {Button} from '../../components/button.tsx';
import {Filter, type Selection} from '../../components/filter.tsx';
import {IssueLink} from '../../components/issue-link.tsx';
import {Link} from '../../components/link.tsx';
import {RelativeTime} from '../../components/relative-time.tsx';
import {useClickOutside} from '../../hooks/use-click-outside.ts';
import {useElementSize} from '../../hooks/use-element-size.ts';
import {useKeypress} from '../../hooks/use-keypress.ts';
import {useLogin} from '../../hooks/use-login.tsx';
import {useZero} from '../../hooks/use-zero.ts';
import {recordPageLoad} from '../../page-load-stats.ts';
import {mark} from '../../perf-log.ts';
import {preload} from '../../zero-preload.ts';
import {CACHE_NAV, CACHE_NONE} from '../../query-cache-policy.ts';
import {queries, type ListContext} from '../../../shared/queries.ts';
import type {IssueRow} from '../../../shared/schema.ts';

let firstRowRendered = false;
const ITEM_SIZE = 56;
const MIN_PAGE_SIZE = 100;

type Anchor = {
  startRow: IssueRow | undefined;
  direction: 'forward' | 'backward';
  index: number;
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

  const status = qs.get('status')?.toLowerCase() ?? 'open';
  const creator = qs.get('creator') ?? null;
  const assignee = qs.get('assignee') ?? null;
  const labels = qs.getAll('label');

  // Cannot drive entirely by URL params because we need to debounce the changes
  // while typing ito input box.
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

  const [anchor, setAnchor] = useState<Anchor>(TOP_ANCHOR);

  const listContextParams = {
    sortDirection,
    sortField,
    assignee,
    creator,
    labels,
    open,
    textFilter,
  } as const;

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
      setPageSize(pageSize);
    }
  }, [pageSize, size]);

  const q = queries.issueListV2(
    login.loginState?.decoded,
    listContextParams,
    z.userID,
    pageSize,
    anchor.startRow
      ? {
          id: anchor.startRow.id,
          modified: anchor.startRow.modified,
          created: anchor.startRow.created,
        }
      : null,
    anchor.direction,
  );

  // For detecting if the base query, i.e. ignoring pagination parameters, has
  // changed.
  const baseQ = queries.issueListV2(
    login.loginState?.decoded,
    listContextParams,
    z.userID,
    null, // no limit
    null, // no start
    'forward', // fixed direction
  );

  const [estimatedTotal, setEstimatedTotal] = useState(0);
  const [total, setTotal] = useState<number | undefined>(undefined);

  useEffect(() => {
    setEstimatedTotal(0);
    setTotal(undefined);
    setAnchor(TOP_ANCHOR);
    virtualizer.scrollToIndex(0);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [baseQ.hash()]);

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
    if (anchor.direction !== 'forward') {
      return;
    }
    const eTotal = anchor.index + issues.length;
    if (eTotal > estimatedTotal) {
      setEstimatedTotal(eTotal);
    }
    if (issuesResult.type === 'complete' && issues.length < pageSize) {
      setTotal(eTotal);
    }
  }, [anchor, issuesResult.type, issues, estimatedTotal, pageSize]);

  useEffect(() => {
    if (issuesResult.type === 'complete') {
      recordPageLoad('list-page');
      preload(login.loginState?.decoded, z);
    }
  }, [login.loginState?.decoded, issuesResult.type, z]);

  let title;
  if (creator || assignee || labels.length > 0 || textFilter) {
    title = 'Filtered Issues';
  } else {
    title = status.slice(0, 1).toUpperCase() + status.slice(1) + ' Issues';
  }

  const listContext: ListContext = {
    href: window.location.href,
    title,
    params: {
      open,
      assignee,
      creator,
      labels,
      textFilter: textFilter ?? null,
      sortField,
      sortDirection,
    },
  };

  const onDeleteFilter = (e: React.MouseEvent) => {
    const target = e.currentTarget;
    const key = target.getAttribute('data-key');
    const value = target.getAttribute('data-value');
    const entries = [...new URLSearchParams(qs).entries()];
    const index = entries.findIndex(([k, v]) => k === key && v === value);
    if (index !== -1) {
      entries.splice(index, 1);
    }
    navigate('?' + new URLSearchParams(entries).toString());
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
    setTextFilter('');
    updateTextFilterQueryString('');
    setForceSearchMode(false);
  };

  const Row = ({index, style}: {index: number; style: CSSProperties}) => {
    const issueArrayIndex = toIssueArrayIndex(index, anchor);
    if (issueArrayIndex < 0 || issueArrayIndex >= issues.length) {
      return (
        <div
          className={classNames('row')}
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
            login.loginState != undefined
            ? 'unread'
            : null,
        )}
        style={{
          ...style,
        }}
      >
        <IssueLink
          className={classNames('issue-title', {'issue-closed': !issue.open})}
          issue={issue}
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
              href={`/?label=${label.name}`}
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
    count: total ?? estimatedTotal,
    estimateSize: () => ITEM_SIZE,
    overscan: 5,
    getScrollElement: () => listRef.current,
  });

  const virtualItems = virtualizer.getVirtualItems();
  useEffect(() => {
    const [firstItem] = virtualItems;
    const lastItem = virtualItems[virtualItems.length - 1];
    if (!lastItem) {
      return;
    }

    if (
      anchor.index !== 0 &&
      firstItem.index <= getNearPageEdgeThreshold(pageSize)
    ) {
      console.log('anchoring to top');
      setAnchor(TOP_ANCHOR);
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
      console.log('page up', a);
      setAnchor(a);
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
      console.log('page down', a);
      setAnchor(a);
    }
  }, [anchor, issues, issuesResult, pageSize, virtualItems]);

  const [forceSearchMode, setForceSearchMode] = useState(false);
  const searchMode = forceSearchMode || Boolean(textFilter);
  const searchBox = useRef<HTMLHeadingElement>(null);
  const startSearchButton = useRef<HTMLButtonElement>(null);
  useKeypress('/', () => setForceSearchMode(true));
  useClickOutside([searchBox, startSearchButton], () =>
    setForceSearchMode(false),
  );
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
            <span className="list-view-title">{title}</span>
          )}
          {issuesResult.type === 'complete' || issues.length > 0 ? (
            <span className="issue-count">
              {total ?? `${estimatedTotal - (estimatedTotal % 50)}+`}
            </span>
          ) : null}
        </h1>
        <Button
          ref={startSearchButton}
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
        <Filter onSelect={onFilter} />
        <div className="sort-control-container">
          <Button
            className="sort-control"
            eventName="Toggle sort type"
            onAction={toggleSortField}
          >
            {sortField === 'modified' ? 'Modified' : 'Created'}
          </Button>
          <Button
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
    </>
  );
}

const addParam = (
  qs: URLSearchParams,
  key: string,
  value: string,
  mode?: 'exclusive' | undefined,
) => {
  const newParams = new URLSearchParams(qs);
  newParams[mode === 'exclusive' ? 'set' : 'append'](key, value);
  return '?' + newParams.toString();
};
