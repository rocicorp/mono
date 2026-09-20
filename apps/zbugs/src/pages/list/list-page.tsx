import {
  rowAttributes,
  useZeroVirtualizer,
  type RowKey,
  type VirtualRow,
} from '@rocicorp/zero-virtual/react';
import {useQuery, useZero} from '@rocicorp/zero/react';
import classNames from 'classnames';
import Cookies from 'js-cookie';
import React, {
  memo,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent,
} from 'react';
import {toast} from 'react-toastify';
import {useDebouncedCallback} from 'use-debounce';
import {useParams, useSearch} from 'wouter';
import {must} from '../../../../../packages/shared/src/must.ts';
import {mutators} from '../../../shared/mutators.ts';
import {
  queries,
  type Issue,
  type IssueRowSort,
  type ListContext,
} from '../../../shared/queries.ts';
import InfoIcon from '../../assets/images/icon-info.svg?react';
import {Button} from '../../components/button.tsx';
import {Confirm} from '../../components/confirm.tsx';
import {Filter, type Selection} from '../../components/filter.tsx';
import {IssueLink} from '../../components/issue-link.tsx';
import {Link} from '../../components/link.tsx';
import {OnboardingModal} from '../../components/onboarding-modal.tsx';
import {RelativeTime} from '../../components/relative-time.tsx';
import {useClickOutside} from '../../hooks/use-click-outside.ts';
import {useElementSize} from '../../hooks/use-element-size.ts';
import {useHash} from '../../hooks/use-hash.ts';
import {useKeypress} from '../../hooks/use-keypress.ts';
import {useLogin} from '../../hooks/use-login.tsx';
import {useWouterScrollState} from '../../hooks/use-wouter-scroll-state.ts';
import {isPrimaryMouseButton} from '../../is-primary-mouse-button.ts';
import {appendParam, navigate, removeParam, setParam} from '../../navigate.ts';
import {recordPageLoad} from '../../page-load-stats.ts';
import {mark} from '../../perf-log.ts';
import {CACHE_NAV, CACHE_NONE} from '../../query-cache-policy.ts';
import {isGigabugs, links, useListContext} from '../../routes.tsx';
import {preload} from '../../zero-preload.ts';
import {getIDFromString} from '../issue/get-id.tsx';
import {ToastContainer, ToastContent} from '../issue/toast-content.tsx';

let firstRowRendered = false;
function markFirstRowRendered() {
  if (firstRowRendered === false) {
    mark('first issue row rendered');
    firstRowRendered = true;
  }
}

export const ITEM_SIZE = 56;

type RowProps = {
  item: VirtualRow<Issue>;
  sortField: 'created' | 'modified';
  permalinkID: string | null;
  projectName: string;
  listContext: ListContext;
  isLoggedIn: boolean;
  /** When true the row shows a checkbox and shifts over to make room for it. */
  selectMode: boolean;
  /** The ID of the logged in user, used to decide which rows can be selected. */
  currentUserID: string | undefined;
  /** True when the current user may delete any issue (crew / sandbox). */
  canDeleteAny: boolean;
  selected: boolean;
  onToggleSelected: (id: string) => void;
};

const EMPTY_SELECTION: ReadonlySet<string> = new Set();

// Hoisted to module scope (not defined inside `ListPage`) so its component
// identity is stable across `ListPage` renders. A component defined during
// render has a new function identity each render, which makes React remount
// the entire row subtree — churning DOM that the virtualizer measures and
// anchors. `memo` additionally skips re-rendering rows whose props are
// unchanged.
const Row = memo(function Row({
  item,
  sortField,
  permalinkID,
  projectName,
  listContext,
  isLoggedIn,
  selectMode,
  currentUserID,
  canDeleteAny,
  selected,
  onToggleSelected,
}: RowProps) {
  const {index, key, row: issue} = item;
  if (issue === undefined) {
    return (
      <div
        className={classNames('row', 'skeleton-shimmer')}
        {...rowAttributes(index, key)}
      ></div>
    );
  }

  markFirstRowRendered();

  const timestamp = sortField === 'modified' ? issue.modified : issue.created;
  const canDelete = canDeleteAny || issue.creatorID === currentUserID;

  // In select mode the row itself is the focusable, toggleable thing: the
  // whole row, checkbox included, toggles on mousedown like everything else in
  // this UI, and space on the focused row does the same. Links navigate on a
  // window-level mousedown (see useSoftNav), so stopping propagation here is
  // what keeps the title and label pills from navigating away and dropping
  // the selection. Modifier or non-primary clicks are left alone so "open in
  // new tab" still works.
  const toggle = () => {
    if (canDelete) {
      onToggleSelected(issue.id);
    }
  };

  const onRowMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    if (!selectMode || !isPrimaryMouseButton(e.nativeEvent)) {
      return;
    }
    e.stopPropagation();
    // Also stops the browser moving focus to whatever was under the pointer.
    e.preventDefault();
    e.currentTarget.focus();
    toggle();
  };

  const onRowKeyDown = (e: React.KeyboardEvent<HTMLDivElement>) => {
    if (!selectMode || e.key !== ' ') {
      return;
    }
    // Stops useSoftNav treating this as a click on a link, and the checkbox
    // from toggling itself a second time.
    e.stopPropagation();
    e.preventDefault();
    toggle();
  };

  // The mousedown already toggled; suppress the checkbox's native click toggle.
  const onSelectAreaClick = (e: React.MouseEvent) => {
    e.preventDefault();
  };

  return (
    <div
      className={classNames(
        'row',
        issue.modified > (issue.viewState?.viewed ?? 0) && isLoggedIn
          ? 'unread'
          : null,
        {
          // TODO(arv): Extract into something cleaner
          'permalink':
            issue.id === permalinkID || String(issue.shortID) === permalinkID,
          'select-mode': selectMode,
          'not-selectable': selectMode && !canDelete,
          selected,
        },
      )}
      title={
        selectMode && !canDelete
          ? 'You can only delete issues you created'
          : undefined
      }
      // The row is the only focusable thing in select mode so tab, j/k and
      // space all act on the row rather than the link or checkbox inside it.
      tabIndex={selectMode ? 0 : undefined}
      role={selectMode ? 'option' : undefined}
      aria-selected={selectMode ? selected : undefined}
      onMouseDown={onRowMouseDown}
      onKeyDown={onRowKeyDown}
      {...rowAttributes(index, key)}
    >
      {selectMode ? (
        <label className="issue-select-area" onClick={onSelectAreaClick}>
          <input
            type="checkbox"
            className="issue-select"
            checked={selected}
            disabled={!canDelete}
            readOnly
            tabIndex={-1}
            aria-hidden="true"
          />
        </label>
      ) : null}
      <IssueLink
        className={classNames('issue-title', {'issue-closed': !issue.open})}
        issue={{projectName, id: issue.id, shortID: issue.shortID}}
        title={issue.title}
        listContext={listContext}
        tabIndex={selectMode ? -1 : undefined}
      >
        {issue.title}
      </IssueLink>
      <div className="issue-taglist">
        {issue.labels.map(label => (
          <Link
            key={label.id}
            className="pill label"
            href={`?label=${label.name}`}
            tabIndex={selectMode ? -1 : undefined}
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
});

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

  const currentUserID = login.loginState?.decoded.sub;
  const [crewUser] = useQuery(currentUserID && queries.crewUser(currentUserID));
  const canDeleteAny = Boolean(
    import.meta.env.VITE_PUBLIC_SANDBOX || crewUser !== undefined,
  );

  const [selectModeState, setSelectModeState] = useState(false);
  const selectMode = selectModeState && currentUserID !== undefined;
  const [selectedIDs, setSelectedIDs] =
    useState<ReadonlySet<string>>(EMPTY_SELECTION);
  const [deleteConfirmationShown, setDeleteConfirmationShown] = useState(false);

  const toggleSelectMode = useCallback(() => {
    setSelectModeState(prev => !prev);
    setSelectedIDs(EMPTY_SELECTION);
  }, []);

  const exitSelectMode = useCallback(() => {
    setSelectModeState(false);
    setSelectedIDs(EMPTY_SELECTION);
  }, []);

  const onToggleSelected = useCallback((id: string) => {
    setSelectedIDs(prev => {
      const next = new Set(prev);
      if (!next.delete(id)) {
        next.add(id);
      }
      return next;
    });
  }, []);

  const deleteSelected = async () => {
    const ids = [...selectedIDs];
    if (ids.length === 0) {
      return;
    }
    // TODO: Implement undo - https://github.com/rocicorp/undo
    const result = z.mutate(mutators.issue.deleteMany(ids));
    const clientResult = await result.client;
    if (clientResult.type === 'error') {
      const toastID = 'delete-issues-failed';
      toast(
        <ToastContent toastID={toastID}>
          Failed to delete issues: {clientResult.error.message}
        </ToastContent>,
        {toastId: toastID, containerId: 'bottom'},
      );
      return;
    }
    exitSelectMode();
  };

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

  const listContext: ListContext = useMemo(
    () => ({
      href: `${links.list({projectName})}?${search}`,
      title,
      params: listContextParams,
    }),
    [projectName, search, title, listContextParams],
  );

  // A different list (filters, sort, project, user) means the selected rows
  // may no longer be visible, so drop the selection rather than deleting
  // things the user can't see.
  useEffect(() => {
    setSelectedIDs(EMPTY_SELECTION);
  }, [listContextParams, currentUserID]);

  const {setListContext} = useListContext();
  useEffect(() => {
    setListContext(listContext);
    document.title =
      `Zero Bugs → ${listContext.title}` +
      (permalinkID ? ` → Issue ${permalinkID}` : '');
  }, [listContext, permalinkID, setListContext]);

  const listRef = useRef<HTMLDivElement>(null);
  const tableWrapperRef = useRef<HTMLDivElement>(null);
  const size = useElementSize(tableWrapperRef);

  // oxlint-disable-next-line no-explicit-any
  (globalThis as any).permalinkNavigate = (id: string | number) => {
    navigate(`#issue-${id}`);
  };

  const [scrollState, setScrollState] = useWouterScrollState<IssueRowSort>();

  const queryOptions = textFilterQuery === textFilter ? CACHE_NAV : CACHE_NONE;

  const {
    items,
    spaceBefore,
    spaceAfter,
    complete,
    rowsEmpty,
    permalinkNotFound,
    estimatedTotal,
    total,
    scrollToItem,
  } = useZeroVirtualizer<typeof listContextParams, Issue, IssueRowSort>({
    estimateSize: () => ITEM_SIZE,
    getScrollElement: () => listRef.current,
    getRowKey: row => row.id,

    listContextParams,
    permalinkID,

    getPageQuery: ({limit, start, dir}) => ({
      query: queries.issueListV2({
        listContext: listContextParams,
        limit,
        start,
        dir,
        inclusive: start === null,
      }),
      options: queryOptions,
    }),

    getSingleQuery: ({id}) => {
      // Allow short ID too.
      const {idField, idValue} = getIDFromString(id);
      return {
        query: queries.listIssueByID({
          idField,
          idValue,
          listContext: listContextParams,
        }),
        options: queryOptions,
      };
    },

    toStartRow: row => ({
      id: row.id,
      modified: row.modified,
      created: row.created,
    }),

    scrollState,
    onScrollStateChange: setScrollState,
  });

  // The keydown handler below reads the loaded window, which changes identity
  // on every commit. Through a ref, so the listener isn't torn down and
  // re-attached each time the list pages.
  const itemsRef = useRef(items);
  itemsRef.current = items;

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

  // Keyboard navigation in select mode: j/k move focus to the next/previous
  // row. The row itself handles space (see Row), so the focused row is the
  // cursor and there is no separate cursor state.
  useEffect(() => {
    if (!selectMode || deleteConfirmationShown) {
      return;
    }

    const focusRow = (scrollElement: HTMLElement, key: RowKey) => {
      scrollToItem(key);
      scrollElement
        .querySelector<HTMLElement>(
          `[data-vrow-key="${CSS.escape(String(key))}"]`,
        )
        // scrollToItem has already placed the row; don't let the browser
        // scroll it a second time with its own idea of where it belongs.
        ?.focus({preventScroll: true});
    };

    // j/k only ever move one row, so the target is adjacent to the focused one
    // and the virtualizer already has it loaded. Nothing here needs to know
    // how tall a row is or where it sits — only which row comes next.
    const itemAt = (index: number) =>
      itemsRef.current.find(i => i.index === index);

    const onKeyDown = (e: globalThis.KeyboardEvent) => {
      if (
        (e.key !== 'j' && e.key !== 'k') ||
        e.metaKey ||
        e.ctrlKey ||
        e.altKey
      ) {
        return;
      }
      const target = e.target as HTMLElement | null;
      if (
        target &&
        (target.isContentEditable ||
          target.tagName === 'TEXTAREA' ||
          target.tagName === 'SELECT' ||
          target.tagName === 'INPUT')
      ) {
        return;
      }
      const scrollElement = listRef.current;
      if (!scrollElement) {
        return;
      }
      const focusedRow = target?.closest<HTMLElement>('.row[data-vrow-index]');
      const next =
        focusedRow && scrollElement.contains(focusedRow)
          ? itemAt(
              Number(focusedRow.dataset.vrowIndex) + (e.key === 'j' ? 1 : -1),
            )
          : // Nothing focused in the list: start from the first visible row.
            itemAt(Math.floor(scrollElement.scrollTop / ITEM_SIZE));
      if (!next) {
        // Either end of the list, or the window edge with paging not caught
        // up yet — scrolling toward it is what advances the window, so the
        // next press lands.
        return;
      }
      e.preventDefault();
      focusRow(scrollElement, next.key);
    };

    window.addEventListener('keydown', onKeyDown);
    return () => {
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [selectMode, deleteConfirmationShown, scrollToItem]);

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
        {currentUserID !== undefined ? (
          <Button
            enabledOffline
            className={classNames('select-toggle', {active: selectMode})}
            eventName="Toggle issue selection"
            onAction={toggleSelectMode}
            title={selectMode ? 'Done selecting' : 'Select issues'}
            aria-label={selectMode ? 'Done selecting' : 'Select issues'}
            aria-pressed={selectMode}
          ></Button>
        ) : null}
        {selectMode ? (
          <>
            <span className="bulk-action-count">
              {selectedIDs.size.toLocaleString()} selected
            </span>
            <div className="edit-buttons">
              <Button
                className="delete-button"
                eventName="Delete selected issues"
                disabled={selectedIDs.size === 0}
                onAction={() => setDeleteConfirmationShown(true)}
              >
                Delete
              </Button>
              <Button
                className="cancel-button"
                enabledOffline
                eventName="Cancel issue selection"
                onAction={exitSelectMode}
              >
                Cancel
              </Button>
            </div>
          </>
        ) : (
          <>
            <span className="filter-icon" aria-hidden="true"></span>
            <span className="filter-label">Filtered by:</span>
            <div className="set-filter-container">
              {Array.from(qs.entries(), ([key, val]) => {
                if (
                  key === 'label' ||
                  key === 'creator' ||
                  key === 'assignee'
                ) {
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
          </>
        )}
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
            <div style={{paddingTop: spaceBefore, paddingBottom: spaceAfter}}>
              {items.map(item => (
                <Row
                  key={item.key}
                  item={item}
                  sortField={sortField}
                  permalinkID={permalinkID}
                  projectName={projectName}
                  listContext={listContext}
                  isLoggedIn={login.loginState !== undefined}
                  selectMode={selectMode}
                  currentUserID={currentUserID}
                  canDeleteAny={canDeleteAny}
                  selected={selectedIDs.has(item.row?.id ?? '')}
                  onToggleSelected={onToggleSelected}
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
      <Confirm
        isOpen={deleteConfirmationShown}
        title="Delete Issues"
        text={`Really delete ${selectedIDs.size.toLocaleString()} ${
          selectedIDs.size === 1 ? 'issue' : 'issues'
        }?`}
        okButtonLabel="Delete"
        onClose={ok => {
          if (ok) {
            void deleteSelected();
          }
          setDeleteConfirmationShown(false);
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
