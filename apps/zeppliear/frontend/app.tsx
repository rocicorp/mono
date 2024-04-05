import type {UndoManager} from '@rocicorp/undo';
import classnames from 'classnames';
import {generateKeyBetween} from 'fractional-indexing';
import {isEqual, minBy, partial, pickBy, sortBy, sortedIndexBy} from 'lodash';
import {useQueryState} from 'next-usequerystate';
import {memo, useCallback, useEffect, useReducer, useState} from 'react';
import {HotKeys} from 'react-hotkeys';
import type {
  ExperimentalDiff as Diff,
  ReadTransaction,
  ReadonlyJSONValue,
  Replicache,
} from 'replicache';
import {useSubscribe} from 'replicache-react';
import {getPartialSyncState} from './control';
import {
  Comment,
  Description,
  ISSUE_KEY_PREFIX,
  Issue,
  IssueUpdate,
  IssueUpdateWithID,
  Order,
  Priority,
  Status,
  issueFromKeyAndValue,
  orderEnumSchema,
  priorityEnumSchema,
  priorityOrderValues,
  reverseTimestampSortKey,
  statusEnumSchema,
  statusOrderValues,
} from './issue';
import IssueBoard from './issue-board';
import IssueDetail from './issue-detail';
import IssueList from './issue-list';
import LeftMenu from './left-menu';
import type {M} from './mutators';
import TopFilter from './top-filter';

class Filters {
  readonly #viewStatuses: Set<Status> | undefined;
  readonly #issuesStatuses: Set<Status> | undefined;
  readonly #issuesPriorities: Set<Priority> | undefined;
  readonly hasNonViewFilters: boolean;
  constructor(
    view: string | null,
    priorityFilter: string | null,
    statusFilter: string | null,
  ) {
    this.#viewStatuses = undefined;
    switch (view?.toLowerCase()) {
      case 'active':
        this.#viewStatuses = new Set([Status.InProgress, Status.Todo]);
        break;
      case 'backlog':
        this.#viewStatuses = new Set([Status.Backlog]);
        break;
      default:
        this.#viewStatuses = undefined;
    }

    this.#issuesStatuses = undefined;
    this.#issuesPriorities = undefined;
    this.hasNonViewFilters = false;
    if (statusFilter) {
      this.#issuesStatuses = new Set<Status>();
      console.error('!!!!!!!', statusFilter);
      console.error('AAAAAAA', statusFilter.split(','));
      for (const s of statusFilter.split(',')) {
        const parseResult = statusEnumSchema.safeParse(s);
        if (
          parseResult.success &&
          (!this.#viewStatuses || this.#viewStatuses.has(parseResult.data))
        ) {
          this.hasNonViewFilters = true;
          this.#issuesStatuses.add(parseResult.data);
        }
      }
    }
    if (!this.hasNonViewFilters) {
      this.#issuesStatuses = this.#viewStatuses;
    }

    if (priorityFilter) {
      this.#issuesPriorities = new Set<Priority>();
      for (const p of priorityFilter.split(',')) {
        const parseResult = priorityEnumSchema.safeParse(p);
        if (parseResult.success) {
          this.hasNonViewFilters = true;
          this.#issuesPriorities.add(parseResult.data);
        }
      }
      if (this.#issuesPriorities.size === 0) {
        this.#issuesPriorities = undefined;
      }
    }
  }

  viewFilter(issue: Issue): boolean {
    return this.#viewStatuses ? this.#viewStatuses.has(issue.status) : true;
  }

  issuesFilter(issue: Issue): boolean {
    if (this.#issuesStatuses) {
      if (!this.#issuesStatuses.has(issue.status)) {
        return false;
      }
    }
    if (this.#issuesPriorities) {
      if (!this.#issuesPriorities.has(issue.priority)) {
        return false;
      }
    }
    return true;
  }

  equals(other: Filters): boolean {
    return (
      this === other ||
      (isEqual(this.#viewStatuses, other.#viewStatuses) &&
        isEqual(this.#issuesStatuses, other.#issuesStatuses) &&
        isEqual(this.#issuesPriorities, other.#issuesPriorities) &&
        isEqual(this.hasNonViewFilters, other.hasNonViewFilters))
    );
  }
}

function getFilters(
  view: string | null,
  priorityFilter: string | null,
  statusFilter: string | null,
): Filters {
  return new Filters(view, priorityFilter, statusFilter);
}

function getIssueOrder(view: string | null, orderBy: string | null): Order {
  if (view === 'board') {
    return Order.Kanban;
  }
  const parseResult = orderEnumSchema.safeParse(orderBy);
  return parseResult.success ? parseResult.data : Order.Modified;
}

function getTitle(view: string | null) {
  switch (view?.toLowerCase()) {
    case 'active':
      return 'Active issues';
    case 'backlog':
      return 'Backlog issues';
    case 'board':
      return 'Board';
    default:
      return 'All issues';
  }
}

type State = {
  allIssuesMap: Map<string, Issue>;
  viewIssueCount: number;
  filteredIssues: Issue[];
  filters: Filters;
  issueOrder: Order;
};
function timedReducer(
  state: State,
  action:
    | {
        type: 'diff';
        diff: Diff;
      }
    | {
        type: 'setFilters';
        filters: Filters;
      }
    | {
        type: 'setIssueOrder';
        issueOrder: Order;
      },
): State {
  const start = Date.now();
  const result = reducer(state, action);
  console.log(`Reducer took ${Date.now() - start}ms`, action);
  return result;
}

function getOrderValue(issueOrder: Order, issue: Issue): string {
  let orderValue: string;
  switch (issueOrder) {
    case Order.Created:
      orderValue = reverseTimestampSortKey(issue.created, issue.id);
      break;
    case Order.Modified:
      orderValue = reverseTimestampSortKey(issue.modified, issue.id);
      break;
    case Order.Status:
      orderValue =
        statusOrderValues[issue.status] +
        '-' +
        reverseTimestampSortKey(issue.modified, issue.id);
      break;
    case Order.Priority:
      orderValue =
        priorityOrderValues[issue.priority] +
        '-' +
        reverseTimestampSortKey(issue.modified, issue.id);
      break;
    case Order.Kanban:
      orderValue = issue.kanbanOrder + '-' + issue.id;
      break;
  }
  return orderValue;
}

function reducer(
  state: State,
  action:
    | {
        type: 'diff';
        diff: Diff;
      }
    | {
        type: 'setFilters';
        filters: Filters;
      }
    | {
        type: 'setIssueOrder';
        issueOrder: Order;
      },
): State {
  const filters = action.type === 'setFilters' ? action.filters : state.filters;
  const issueOrder =
    action.type === 'setIssueOrder' ? action.issueOrder : state.issueOrder;
  const orderIteratee = partial(getOrderValue, issueOrder);
  function filterAndSort(issues: Issue[]): Issue[] {
    return sortBy(
      issues.filter(issue => filters.issuesFilter(issue)),
      orderIteratee,
    );
  }
  function countViewIssues(issues: Issue[]): number {
    let count = 0;
    for (const issue of issues) {
      if (filters.viewFilter(issue)) {
        count++;
      }
    }
    return count;
  }

  switch (action.type) {
    case 'diff': {
      return diffReducer(state, action.diff);
    }
    case 'setFilters': {
      if (action.filters.equals(state.filters)) {
        return state;
      }
      const allIssues = [...state.allIssuesMap.values()];
      return {
        ...state,
        viewIssueCount: countViewIssues(allIssues),
        filters: action.filters,
        filteredIssues: filterAndSort(allIssues),
      };
    }
    case 'setIssueOrder': {
      if (action.issueOrder === state.issueOrder) {
        return state;
      }
      return {
        ...state,
        filteredIssues: sortBy(state.filteredIssues, orderIteratee),
        issueOrder: action.issueOrder,
      };
    }
  }

  return state;
}

function diffReducer(state: State, diff: Diff): State {
  if (diff.length === 0) {
    return state;
  }
  const newAllIssuesMap = new Map(state.allIssuesMap);
  let newViewIssueCount = state.viewIssueCount;
  const newFilteredIssues = [...state.filteredIssues];
  const orderIteratee = partial(getOrderValue, state.issueOrder);

  function add(key: string, newValue: ReadonlyJSONValue) {
    const newIssue = issueFromKeyAndValue(key, newValue);
    newAllIssuesMap.set(key, newIssue);
    if (state.filters.viewFilter(newIssue)) {
      newViewIssueCount++;
    }
    if (state.filters.issuesFilter(newIssue)) {
      newFilteredIssues.splice(
        sortedIndexBy(newFilteredIssues, newIssue, orderIteratee),
        0,
        newIssue,
      );
    }
  }
  function del(key: string, oldValue: ReadonlyJSONValue) {
    const oldIssue = issueFromKeyAndValue(key, oldValue);
    const index = sortedIndexBy(newFilteredIssues, oldIssue, orderIteratee);
    newAllIssuesMap.delete(key);
    if (state.filters.viewFilter(oldIssue)) {
      newViewIssueCount--;
    }
    if (newFilteredIssues[index]?.id === oldIssue.id) {
      newFilteredIssues.splice(index, 1);
    }
  }
  for (const diffOp of diff) {
    switch (diffOp.op) {
      case 'add': {
        add(diffOp.key as string, diffOp.newValue);
        break;
      }
      case 'del': {
        del(diffOp.key as string, diffOp.oldValue);
        break;
      }
      case 'change': {
        del(diffOp.key as string, diffOp.oldValue);
        add(diffOp.key as string, diffOp.newValue);
        break;
      }
    }
  }
  return {
    ...state,
    allIssuesMap: newAllIssuesMap,
    viewIssueCount: newViewIssueCount,
    filteredIssues: newFilteredIssues,
  };
}

type AppProps = {
  rep: Replicache<M>;
  undoManager: UndoManager;
};

// eslint-disable-next-line @typescript-eslint/naming-convention
const App = ({rep, undoManager}: AppProps) => {
  const [view] = useQueryState('view');
  const [priorityFilter] = useQueryState('priorityFilter');
  const [statusFilter] = useQueryState('statusFilter');
  console.log('useQueryState', statusFilter, typeof statusFilter);
  const [orderBy] = useQueryState('orderBy');
  const [detailIssueID, setDetailIssueID] = useQueryState('iss');
  const [menuVisible, setMenuVisible] = useState(false);

  const [state, dispatch] = useReducer(timedReducer, {
    allIssuesMap: new Map(),
    viewIssueCount: 0,
    filteredIssues: [],
    filters: getFilters(view, priorityFilter, statusFilter),
    issueOrder: getIssueOrder(view, orderBy),
  });

  const partialSync = useSubscribe(
    rep,
    async (tx: ReadTransaction) =>
      (await getPartialSyncState(tx)) || 'NOT_RECEIVED_FROM_SERVER',
    'NOT_RECEIVED_FROM_SERVER' as const,
  );

  const partialSyncComplete = partialSync === 'PARTIAL_SYNC_COMPLETE';
  useEffect(() => {
    console.log('partialSync', partialSync);
    if (!partialSyncComplete) {
      void rep.pull();
    }
  }, [rep, partialSync, partialSyncComplete]);

  useEffect(() => {
    rep.experimentalWatch(
      diff =>
        dispatch({
          type: 'diff',
          diff,
        }),
      {prefix: ISSUE_KEY_PREFIX, initialValuesInFirstDiff: true},
    );
  }, [rep]);

  useEffect(
    () =>
      dispatch({
        type: 'setFilters',
        filters: getFilters(view, priorityFilter, statusFilter),
      }),
    [view, priorityFilter, statusFilter],
  );

  useEffect(
    () =>
      dispatch({
        type: 'setIssueOrder',
        issueOrder: getIssueOrder(view, orderBy),
      }),
    [view, orderBy],
  );

  const handleCreateIssue = useCallback(
    async (issue: Omit<Issue, 'kanbanOrder'>, description: Description) => {
      const minKanbanOrderIssue = minBy(
        [...state.allIssuesMap.values()],
        issue => issue.kanbanOrder,
      );
      const minKanbanOrder = minKanbanOrderIssue
        ? minKanbanOrderIssue.kanbanOrder
        : null;

      await rep.mutate.putIssue({
        issue: {
          ...issue,
          kanbanOrder: generateKeyBetween(null, minKanbanOrder),
        },
        description,
      });
    },
    [rep.mutate, state.allIssuesMap],
  );
  const handleCreateComment = useCallback(
    async (comment: Comment) => {
      await undoManager.add({
        execute: () => rep.mutate.putIssueComment(comment),
        undo: () => rep.mutate.deleteIssueComment(comment),
      });
    },
    [rep.mutate, undoManager],
  );

  const handleUpdateIssues = useCallback(
    async (issueUpdates: Array<IssueUpdate>) => {
      const uChanges: Array<IssueUpdateWithID> =
        issueUpdates.map<IssueUpdateWithID>(issueUpdate => {
          const undoChanges = pickBy(
            issueUpdate.issue,
            (_, key) => key in issueUpdate.issueChanges,
          );
          return {
            id: issueUpdate.issue.id,
            issueChanges: undoChanges,
            descriptionChange: issueUpdate.descriptionUpdate?.description,
          };
        });
      await undoManager.add({
        execute: () =>
          rep.mutate.updateIssues(
            issueUpdates.map(({issue, issueChanges, descriptionUpdate}) => ({
              id: issue.id,
              issueChanges,
              descriptionChange: descriptionUpdate?.description,
            })),
          ),
        undo: () => rep.mutate.updateIssues(uChanges),
      });
    },
    [rep.mutate, undoManager],
  );

  const handleOpenDetail = useCallback(
    async (issue: Issue) => {
      await setDetailIssueID(issue.id, {scroll: false, shallow: true});
    },
    [setDetailIssueID],
  );
  const handleCloseMenu = useCallback(
    () => setMenuVisible(false),
    [setMenuVisible],
  );
  const handleToggleMenu = useCallback(
    () => setMenuVisible(!menuVisible),
    [setMenuVisible, menuVisible],
  );

  const handlers = {
    undo: () => undoManager.undo(),
    redo: () => undoManager.redo(),
  };

  return (
    <HotKeys
      {...{
        keyMap,
        handlers,
      }}
    >
      <Layout
        menuVisible={menuVisible}
        view={view}
        detailIssueID={detailIssueID}
        isLoading={!partialSyncComplete}
        state={state}
        rep={rep}
        onCloseMenu={handleCloseMenu}
        onToggleMenu={handleToggleMenu}
        onUpdateIssues={handleUpdateIssues}
        onCreateIssue={handleCreateIssue}
        onCreateComment={handleCreateComment}
        onOpenDetail={handleOpenDetail}
      ></Layout>
    </HotKeys>
  );
};

const keyMap = {
  undo: ['ctrl+z', 'command+z'],
  redo: ['ctrl+y', 'command+shift+z', 'ctrl+shift+z'],
};

interface LayoutProps {
  menuVisible: boolean;
  view: string | null;
  detailIssueID: string | null;
  isLoading: boolean;
  state: State;
  rep: Replicache<M>;
  onCloseMenu: () => void;
  onToggleMenu: () => void;
  onUpdateIssues: (issueUpdates: IssueUpdate[]) => void;
  onCreateIssue: (
    issue: Omit<Issue, 'kanbanOrder'>,
    description: Description,
  ) => void;
  onCreateComment: (comment: Comment) => void;
  onOpenDetail: (issue: Issue) => void;
}

function RawLayout({
  menuVisible,
  view,
  detailIssueID,
  isLoading,
  state,
  rep,
  onCloseMenu,
  onToggleMenu,
  onUpdateIssues,
  onCreateIssue,
  onCreateComment,
  onOpenDetail,
}: LayoutProps) {
  return (
    <div>
      <div className="flex w-full h-screen overflow-y-hidden">
        <LeftMenu
          menuVisible={menuVisible}
          onCloseMenu={onCloseMenu}
          onCreateIssue={onCreateIssue}
        />
        <div className="flex flex-col flex-grow min-w-0">
          <div
            className={classnames('flex flex-col', {
              hidden: detailIssueID,
            })}
          >
            <TopFilter
              onToggleMenu={onToggleMenu}
              title={getTitle(view)}
              filteredIssuesCount={
                state.filters.hasNonViewFilters
                  ? state.filteredIssues.length
                  : undefined
              }
              issuesCount={state.viewIssueCount}
              showSortOrderMenu={view !== 'board'}
            />
          </div>
          <div className="relative flex flex-1 min-h-0">
            {detailIssueID && (
              <IssueDetail
                issues={state.filteredIssues}
                rep={rep}
                onUpdateIssues={onUpdateIssues}
                onAddComment={onCreateComment}
                isLoading={isLoading}
              />
            )}
            <div
              className={classnames('absolute inset-0 flex flex-col', {
                'invisible': detailIssueID,
                // eslint-disable-next-line @typescript-eslint/naming-convention
                'pointer-events-none': detailIssueID,
              })}
            >
              {view === 'board' ? (
                <IssueBoard
                  issues={state.filteredIssues}
                  onUpdateIssues={onUpdateIssues}
                  onOpenDetail={onOpenDetail}
                />
              ) : (
                <IssueList
                  issues={state.filteredIssues}
                  onUpdateIssues={onUpdateIssues}
                  onOpenDetail={onOpenDetail}
                  view={view}
                />
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

// eslint-disable-next-line @typescript-eslint/naming-convention
const Layout = memo(RawLayout);

export default App;
