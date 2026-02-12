import {useQuery, useZero} from '@rocicorp/zero/react';
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
import {useDebouncedCallback} from 'use-debounce';
import {useParams, useSearch} from 'wouter';
import {useHistoryState} from 'wouter/use-browser-location';
import {must} from '../../../../../packages/shared/src/must.ts';
import {
  queries,
  type IssueRowSort,
  type ListContext,
} from '../../../shared/queries.ts';
import InfoIcon from '../../assets/images/icon-info.svg?react';
import {Button} from '../../components/button.tsx';
import {Filter, type Selection} from '../../components/filter.tsx';
import {IssueLink} from '../../components/issue-link.tsx';
import {Link} from '../../components/link.tsx';
import {OnboardingModal} from '../../components/onboarding-modal.tsx';
import {RelativeTime} from '../../components/relative-time.tsx';
import {
  useArrayVirtualizer,
  type ScrollRestorationState,
} from '../../hooks/use-array-virtualizer.ts';
import {useClickOutside} from '../../hooks/use-click-outside.ts';
import {useElementSize} from '../../hooks/use-element-size.ts';
import {useHash} from '../../hooks/use-hash.ts';
import {useKeypress} from '../../hooks/use-keypress.ts';
import {useLogin} from '../../hooks/use-login.tsx';
import {appendParam, navigate, removeParam, setParam} from '../../navigate.ts';
import {recordPageLoad} from '../../page-load-stats.ts';
import {mark} from '../../perf-log.ts';
import {CACHE_NAV, CACHE_NONE} from '../../query-cache-policy.ts';
import {isGigabugs, links, useListContext} from '../../routes.tsx';
import {preload} from '../../zero-preload.ts';
import {getIDFromString} from '../issue/get-id.tsx';
import {ToastContainer, ToastContent} from '../issue/toast-content.tsx';

let firstRowRendered = false;
export const ITEM_SIZE = 56;

export function ListPage({onReady}: {onReady: () => void}) {
  const login = useLogin();
  const search = useSearch();
  const qs = useMemo(() => new URLSearchParams(search), [search]);
  const z = useZero();

  const params = useParams();
  const projectName = must(params.projectName);

  const [showOnboarding, setShowOnboarding] = useState(false);
  const isDemoMode = qs.has('demo');
  const isDemoVideo = qs.has('demovideo');

  useEffect(() => {
    if (isGigabugs(projectName) && !Cookies.get('onboardingDismissed')) {
      if (isDemoMode || isDemoVideo) {
        Cookies.set('onboardingDismissed', 'true', {expires: 365});
      } else {
        setShowOnboarding(true);
      }
    }
  }, [projectName, isDemoMode, isDemoVideo]);

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

  const hash = useHash();
  const permalinkID = useMemo(
    () => (hash.startsWith('issue-') ? hash.slice(6) : null),
    [hash],
  );

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

  const listContext: ListContext = useMemo(
    () => ({
      href: `${links.list({projectName})}?${search}`,
      title,
      params: listContextParams,
    }),
    [projectName, search, title, listContextParams],
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

  // oxlint-disable-next-line no-explicit-any
  (globalThis as any).permalinkNavigate = (id: string | number) => {
    navigate(`#issue-${id}`);
  };

  const scrollStateFromHistory =
    useHistoryState<ScrollRestorationState | null>();

  const scrollState = useMemo(
    () => scrollStateFromHistory,
    [
      scrollStateFromHistory?.scrollAnchorID,
      scrollStateFromHistory?.index,
      scrollStateFromHistory?.scrollOffset,
    ],
  );

  const handleScrollStateChange = useCallback(
    (state: ScrollRestorationState) => {
      window.history.replaceState(state, '', window.location.href);
    },
    [],
  );

  const estimateRowSize = useCallback(() => ITEM_SIZE, []);

  const getScrollElement = useCallback(() => listRef.current, []);

  const getPageQuery = useCallback(
    (limit: number, start: IssueRowSort | null, dir: 'forward' | 'backward') =>
      queries.issueListV2({
        listContext: listContextParams,
        userID: z.userID,
        limit,
        start,
        dir,
        inclusive: start === null,
      }),
    [listContextParams, z.userID],
  );

  const getSingleQuery = useCallback(
    (id: string) => {
      // Allow short ID too.
      const {idField, idValue} = getIDFromString(id);
      return queries.listIssueByID({
        idField,
        idValue,
        listContext: listContextParams,
      });
    },
    [listContextParams],
  );

  const toStartRow = useCallback(
    (row: {id: string; modified: number; created: number}) => ({
      id: row.id,
      modified: row.modified,
      created: row.created,
    }),
    [],
  );

  const {
    virtualizer,
    rowAt,
    complete,
    rowsEmpty,
    permalinkNotFound,
    estimatedTotal,
    total,
  } = useArrayVirtualizer({
    estimateRowSize,
    getScrollElement,
    permalinkID: permalinkID ?? undefined,
    listContextParams,
    getPageQuery,
    getSingleQuery,
    toStartRow,
    scrollState: scrollState ?? undefined,
    onScrollStateChange: handleScrollStateChange,
    options: textFilterQuery === textFilter ? CACHE_NAV : CACHE_NONE,
  });

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
      navigate(`?${qs}`, {replace: true});
    }
  }, [permalinkNotFound, permalinkID, qs]);

  useEffect(() => {
    if (!rowsEmpty || complete) {
      onReady();
    }
  }, [rowsEmpty, complete, onReady]);

  useEffect(() => {
    if (complete) {
      recordPageLoad('list-page');
      preload(z, projectName);
    }
  }, [complete, z, projectName]);

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
        navigate(setParam(qs, 'creator', selection.creator));
      } else if ('assignee' in selection) {
        navigate(setParam(qs, 'assignee', selection.assignee));
      } else {
        navigate(appendParam(qs, 'label', selection.label));
      }
    },
    [qs],
  );

  const toggleSortField = useCallback(() => {
    navigate(
      setParam(qs, 'sort', sortField === 'created' ? 'modified' : 'created'),
    );
  }, [qs, sortField]);

  const toggleSortDirection = useCallback(() => {
    navigate(setParam(qs, 'sortDir', sortDirection === 'asc' ? 'desc' : 'asc'));
  }, [qs, sortDirection]);

  const updateTextFilterQueryString = useDebouncedCallback((text: string) => {
    navigate(setParam(qs, 'q', text));
  }, 500);

  const onTextFilterChange = (text: string) => {
    setTextFilter(text);
    updateTextFilterQueryString(text);
  };

  const clearAndHideSearch = () => {
    if (searchMode) {
      setTextFilter(null);
      setForceSearchMode(false);
      navigate(removeParam(qs, 'q'));
    }
  };

  const Row = ({index, style}: {index: number; style: CSSProperties}) => {
    const issue = rowAt(index);
    if (issue === undefined) {
      return (
        <div
          className={classNames('row', 'skeleton-shimmer')}
          style={{
            ...style,
          }}
        ></div>
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
        {size && !rowsEmpty ? (
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
