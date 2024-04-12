import type {UndoManager} from '@rocicorp/undo';
import type {EntityQuery} from '@rocicorp/zql/src';
import * as agg from '@rocicorp/zql/src/zql/query/agg.js';
import classnames from 'classnames';
import {generateKeyBetween} from 'fractional-indexing';
import {minBy, pickBy} from 'lodash';
import {useQueryState} from 'next-usequerystate';
import {memo, useCallback, useEffect, useReducer, useState} from 'react';
import {HotKeys} from 'react-hotkeys';
import type {Zero} from 'zero-client';
import {
  Comment,
  ISSUE_ENTITY_NAME,
  Issue,
  IssueUpdate,
  Order,
  Priority,
  Status,
  orderEnumSchema,
  priorityEnumSchema,
  statusEnumSchema,
} from './issue';
import IssueBoard from './issue-board';
import IssueDetail from './issue-detail';
import IssueList from './issue-list';
import LeftMenu from './left-menu';
import type {M} from './mutators';
import TopFilter from './top-filter';
import {getQuery, useQuery} from './zql.jsx';

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
  issueOrder: Order;
};
function timedReducer(
  state: State,
  action: {
    type: 'setIssueOrder';
    issueOrder: Order;
  },
): State {
  const start = Date.now();
  const result = reducer(state, action);
  console.log(`Reducer took ${Date.now() - start}ms`, action);
  return result;
}

function reducer(
  state: State,
  action: {
    type: 'setIssueOrder';
    issueOrder: Order;
  },
): State {
  switch (action.type) {
    case 'setIssueOrder': {
      if (action.issueOrder === state.issueOrder) {
        return state;
      }
      return {
        ...state,
        issueOrder: action.issueOrder,
      };
    }
  }
}

type AppProps = {
  zero: Zero<M>;
  undoManager: UndoManager;
};

// eslint-disable-next-line @typescript-eslint/naming-convention
const App = ({zero, undoManager}: AppProps) => {
  const [view] = useQueryState('view');
  const [priorityFilter] = useQueryState('priorityFilter');
  const [statusFilter] = useQueryState('statusFilter');
  console.log('useQueryState', statusFilter, typeof statusFilter);
  const [orderBy] = useQueryState('orderBy');
  const [detailIssueID, setDetailIssueID] = useQueryState('iss');
  const [menuVisible, setMenuVisible] = useState(false);

  const [state, dispatch] = useReducer(timedReducer, {
    issueOrder: getIssueOrder(view, orderBy),
  });

  const issueQuery = getQuery<{issue: Issue}>(zero, ISSUE_ENTITY_NAME);

  const allIssueColumns = [
    'id',
    'title',
    'priority',
    'status',
    'modified',
    'created',
    'creatorID',
    'kanbanOrder',
    'description',
  ] as const;

  const allIssues = useQuery(issueQuery.select(...allIssueColumns));

  const {q, hasNonViewFilters} = filterQuery(
    issueQuery,
    view,
    priorityFilter,
    statusFilter,
  );
  const filteredQuery = orderQuery(q, state.issueOrder);
  const filteredIssues = useQuery(filteredQuery, [
    view,
    priorityFilter,
    statusFilter,
    state.issueOrder,
  ]);

  const viewIssueCount =
    useQuery(filteredQuery.select(agg.count()))[0]?.count ?? 0;

  useEffect(
    () =>
      dispatch({
        type: 'setIssueOrder',
        issueOrder: getIssueOrder(view, orderBy),
      }),
    [view, orderBy],
  );

  const handleCreateIssue = useCallback(
    async (issue: Omit<Issue, 'kanbanOrder'>) => {
      // TODO(arv): Use zql min
      const minKanbanOrderIssue = minBy(allIssues, issue => issue.kanbanOrder);
      const minKanbanOrder = minKanbanOrderIssue
        ? minKanbanOrderIssue.kanbanOrder
        : null;

      await zero.mutate.putIssue({
        issue: {
          ...issue,
          kanbanOrder: generateKeyBetween(null, minKanbanOrder),
        },
      });
    },
    [zero.mutate, allIssues],
  );
  const handleCreateComment = useCallback(
    async (comment: Comment) => {
      await undoManager.add({
        execute: () => zero.mutate.putIssueComment({comment}),
        undo: () => zero.mutate.deleteIssueComment({comment}),
      });
    },
    [zero.mutate, undoManager],
  );

  const handleUpdateIssues = useCallback(
    async (issueUpdates: Array<{issue: Issue; update: IssueUpdate}>) => {
      const uChanges: Array<IssueUpdate> = issueUpdates.map<IssueUpdate>(
        ({issue, update}) => {
          const undoChanges = pickBy(issue, (_, key) => key in update);
          return {
            id: issue.id,
            issueChanges: undoChanges,
          };
        },
      );
      await undoManager.add({
        execute: () =>
          zero.mutate.updateIssues({
            issueUpdates: issueUpdates.map<IssueUpdate>(({update}) => update),
          }),
        undo: () => zero.mutate.updateIssues({issueUpdates: uChanges}),
      });
    },
    [zero.mutate, undoManager],
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
        // TODO: base on whether initial sync is done
        isLoading={false}
        viewIssueCount={viewIssueCount}
        filteredIssues={filteredIssues}
        hasNonViewFilters={hasNonViewFilters}
        zero={zero}
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
  viewIssueCount: number;
  filteredIssues: Issue[];
  hasNonViewFilters: boolean;
  zero: Zero<M>;
  onCloseMenu: () => void;
  onToggleMenu: () => void;
  onUpdateIssues: (issueUpdates: {issue: Issue; update: IssueUpdate}[]) => void;
  onCreateIssue: (issue: Omit<Issue, 'kanbanOrder'>) => void;
  onCreateComment: (comment: Comment) => void;
  onOpenDetail: (issue: Issue) => void;
}

function RawLayout({
  menuVisible,
  view,
  detailIssueID,
  isLoading,
  viewIssueCount,
  filteredIssues,
  hasNonViewFilters,
  zero,
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
                hasNonViewFilters ? filteredIssues.length : undefined
              }
              issuesCount={viewIssueCount}
              showSortOrderMenu={view !== 'board'}
            />
          </div>
          <div className="relative flex flex-1 min-h-0">
            {detailIssueID && (
              <IssueDetail
                issues={filteredIssues}
                zero={zero}
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
                  issues={filteredIssues}
                  onUpdateIssues={onUpdateIssues}
                  onOpenDetail={onOpenDetail}
                />
              ) : (
                <IssueList
                  issues={filteredIssues}
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

function filterQuery(
  q: EntityQuery<{issue: Issue}, []>,
  view: string | null,
  priorityFilter: string | null,
  statusFilter: string | null,
) {
  console.log(view, priorityFilter, statusFilter);
  let viewStatuses: Set<Status> | undefined;
  switch (view?.toLowerCase()) {
    case 'active':
      viewStatuses = new Set([Status.InProgress, Status.Todo]);
      break;
    case 'backlog':
      viewStatuses = new Set([Status.Backlog]);
      break;
  }

  let issuesStatuses: Set<Status> | undefined;
  let issuesPriorities: Set<Priority> | undefined;
  let hasNonViewFilters = false;
  if (statusFilter) {
    issuesStatuses = new Set<Status>();
    for (const s of statusFilter.split(',')) {
      const parseResult = statusEnumSchema.safeParse(s);
      if (
        parseResult.success &&
        (!viewStatuses || viewStatuses.has(parseResult.data))
      ) {
        hasNonViewFilters = true;
        issuesStatuses.add(parseResult.data);
      }
    }
  }
  if (!hasNonViewFilters) {
    issuesStatuses = viewStatuses;
  }

  if (priorityFilter) {
    issuesPriorities = new Set<Priority>();
    for (const p of priorityFilter.split(',')) {
      const parseResult = priorityEnumSchema.safeParse(p);
      if (parseResult.success) {
        hasNonViewFilters = true;
        issuesPriorities.add(parseResult.data);
      }
    }
    if (issuesPriorities.size === 0) {
      issuesPriorities = undefined;
    }
  }

  if (issuesStatuses) {
    // Consider allowing Set<T>for IN
    q = q.where('status', 'IN', [...issuesStatuses]);
  }
  if (issuesPriorities) {
    q = q.where('priority', 'IN', [...issuesPriorities]);
  }
  return {q, hasNonViewFilters};
}

function orderQuery(issueQuery: EntityQuery<{issue: Issue}, []>, order: Order) {
  switch (order) {
    case Order.Created:
      return issueQuery.desc('created');
    case Order.Modified:
      return issueQuery.desc('modified');
    // TODO(arv): Change Status and Priority to numeric enums
    case Order.Status:
      return issueQuery.desc('status', 'modified');
    case Order.Priority:
      return issueQuery.desc('priority', 'modified');
    case Order.Kanban:
      return issueQuery.asc('kanbanOrder');
  }
}

export default App;
