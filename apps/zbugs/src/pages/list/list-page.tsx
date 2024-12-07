import {escapeLike} from '@rocicorp/zero';
import {useQuery} from '@rocicorp/zero/react';
import {useVirtualizer} from '@tanstack/react-virtual';
import classNames from 'classnames';
import React, {
  type CSSProperties,
  type KeyboardEvent,
  useEffect,
  useRef,
  useState,
} from 'react';
import {useDebouncedCallback} from 'use-debounce';
import {useSearch} from 'wouter';
import {navigate} from 'wouter/use-browser-location';
import {Button} from '../../components/button.js';
import Filter, {type Selection} from '../../components/filter.js';
import IssueLink from '../../components/issue-link.js';
import {Link} from '../../components/link.js';
import RelativeTime from '../../components/relative-time.js';
import {useClickOutside} from '../../hooks/use-click-outside.js';
import {useElementSize} from '../../hooks/use-element-size.js';
import {useKeypress} from '../../hooks/use-keypress.js';
import {useLogin} from '../../hooks/use-login.js';
import {useZero} from '../../hooks/use-zero.js';
import {mark} from '../../perf-log.js';
import type {ListContext} from '../../routes.js';
import {preload} from '../../zero-setup.js';

let firstRowRendered = false;
const itemSize = 56;

export default function ListPage() {
  const z = useZero();
  const login = useLogin();
  const qs = new URLSearchParams(useSearch());

  const status = qs.get('status')?.toLowerCase() ?? 'open';
  const creator = qs.get('creator') ?? undefined;
  const assignee = qs.get('assignee') ?? undefined;
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

  let q = z.query.issue
    .orderBy(sortField, sortDirection)
    .orderBy('id', sortDirection)
    .related('labels')
    .related('viewState', q => q.where('userID', z.userID).one());

  const open =
    status === 'open' ? true : status === 'closed' ? false : undefined;

  if (open !== undefined) {
    q = q.where('open', open);
  }

  if (creator) {
    q = q.whereExists('creator', q => q.where('login', creator));
  }

  if (assignee) {
    q = q.whereExists('assignee', q => q.where('login', assignee));
  }

  if (textFilter) {
    q = q.where(({or, cmp, exists}) =>
      or(
        cmp('title', 'ILIKE', `%${escapeLike(textFilter)}%`),
        cmp('description', 'ILIKE', `%${escapeLike(textFilter)}%`),
        exists('comments', q =>
          q.where('body', 'ILIKE', `%${escapeLike(textFilter)}%`),
        ),
      ),
    );
  }

  for (const label of labels) {
    q = q.whereExists('labels', q => q.where('name', label));
  }

  const [issues, issuesResult] = useQuery(q);

  useEffect(() => {
    if (issuesResult.type === 'complete') {
      preload(z);
    }
  }, [issuesResult.type, z]);

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
      textFilter: textFilter ?? undefined,
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

  const onFilter = (selection: Selection) => {
    if ('creator' in selection) {
      navigate(addParam(qs, 'creator', selection.creator, 'exclusive'));
    } else if ('assignee' in selection) {
      navigate(addParam(qs, 'assignee', selection.assignee, 'exclusive'));
    } else {
      navigate(addParam(qs, 'label', selection.label));
    }
  };

  const toggleSortField = () => {
    navigate(
      addParam(
        qs,
        'sort',
        sortField === 'created' ? 'modified' : 'created',
        'exclusive',
      ),
    );
  };

  const toggleSortDirection = () => {
    navigate(
      addParam(
        qs,
        'sortDir',
        sortDirection === 'asc' ? 'desc' : 'asc',
        'exclusive',
      ),
    );
  };

  const updateTextFilterQueryString = useDebouncedCallback((text: string) => {
    navigate(addParam(qs, 'q', text, 'exclusive'));
  }, 500);

  const onTextFilterChange = (text: string) => {
    setTextFilter(text);
    updateTextFilterQueryString(text);
  };

  const Row = ({index, style}: {index: number; style: CSSProperties}) => {
    const issue = issues[index];
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

  const listRef = useRef<HTMLDivElement>(null);
  const tableWrapperRef = useRef<HTMLDivElement>(null);
  const size = useElementSize(tableWrapperRef.current);

  const virtualizer = useVirtualizer({
    count: issues.length,
    estimateSize: () => itemSize,
    overscan: 5,
    getItemKey: index => issues[index].id,
    getScrollElement: () => listRef.current,
  });

  const [forceSearchMode, setForceSearchMode] = useState(false);
  const searchMode = forceSearchMode || Boolean(textFilter);
  const searchBox = useRef<HTMLHeadingElement>(null);
  useKeypress('/', () => startSearch());
  useClickOutside(searchBox, () => setForceSearchMode(false));
  const startSearch = () => {
    setForceSearchMode(true);
    setTimeout(() => searchBox.current?.querySelector('input')?.focus(), 0);
  };
  const handleSearchKeyUp = (e: KeyboardEvent) => {
    if (e.key === 'Escape') {
      searchBox.current?.querySelector('input')?.blur();
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
            <input
              type="text"
              value={textFilter ?? ''}
              onChange={e => onTextFilterChange(e.target.value)}
              onFocus={() => setForceSearchMode(true)}
              onBlur={() => setForceSearchMode(false)}
              onKeyUp={handleSearchKeyUp}
              placeholder="Search…"
            />
          ) : (
            <span
              onMouseDown={e => {
                startSearch();
                e.stopPropagation();
              }}
            >
              {title}
            </span>
          )}
          <span className="issue-count">{issues.length}</span>
        </h1>
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
              {virtualizer.getVirtualItems().map(virtualRow => (
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
