import type {UndoManager} from '@rocicorp/undo';
import * as agg from '@rocicorp/zql/src/zql/query/agg.js';
import classnames from 'classnames';
import {generateKeyBetween} from 'fractional-indexing';
import {minBy, pickBy} from 'lodash';
import {useQueryState} from 'next-usequerystate';
import {memo, useCallback, useState} from 'react';
import {HotKeys} from 'react-hotkeys';
import type {EntityQuery, Zero} from 'zero-client';
import {
  Comment,
  Issue,
  IssueUpdate,
  Order,
  Priority,
  Status,
  orderEnumSchema,
  priorityEnumSchema,
  statusStringSchema,
} from './issue';
import IssueBoard from './issue-board';
import IssueDetail from './issue-detail';
import IssueList from './issue-list';
import LeftMenu from './left-menu';
import type {M} from './mutators';
import TopFilter from './top-filter';
import {useQuery} from './hooks/useZql';
import {useZero} from './hooks/useZero';

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

export type Collections = {
  issue: Issue;
  comment: Comment;
};

type AppProps = {
  undoManager: UndoManager;
};

// eslint-disable-next-line @typescript-eslint/naming-convention
const App = ({undoManager}: AppProps) => {
  const [view] = useQueryState('view');
  const [priorityFilter] = useQueryState('priorityFilter');
  const [statusFilter] = useQueryState('statusFilter');
  const [orderBy] = useQueryState('orderBy');
  const [detailIssueID, setDetailIssueID] = useQueryState('iss');
  const [menuVisible, setMenuVisible] = useState(false);
  const zero = useZero<M, Collections>();

  const issueQuery = zero.query.issue;

  const allIssues = useQuery(issueQuery.select('*'));

  const {filteredQuery, hasNonViewFilters, viewCountQuery} = filterQuery(
    issueQuery,
    view,
    priorityFilter,
    statusFilter,
  );
  const issueOrder = getIssueOrder(view, orderBy);
  const filteredAndOrderedQuery = orderQuery(filteredQuery, issueOrder);
  const deps = [view, priorityFilter, statusFilter, issueOrder] as const;
  const filteredIssues = useQuery(filteredAndOrderedQuery, deps);
  const viewIssueCount = useQuery(viewCountQuery, deps)[0]?.count ?? 0;

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
  zero: Zero<M, Collections>;
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
      const parseResult = statusStringSchema.safeParse(s);
      if (parseResult.success) {
        const {data} = parseResult;
        if (!viewStatuses || viewStatuses.has(data)) {
          hasNonViewFilters = true;
          issuesStatuses.add(data);
        }
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

  const viewStatusesQuery = viewStatuses
    ? q.where('status', 'IN', [...viewStatuses])
    : q;
  const viewCountQuery = viewStatusesQuery.select(agg.count());

  if (issuesStatuses) {
    // Consider allowing Iterable<T> for IN
    q = q.where('status', 'IN', [...issuesStatuses]);
  }
  if (issuesPriorities) {
    q = q.where('priority', 'IN', [...issuesPriorities]);
  }

  return {filteredQuery: q, hasNonViewFilters, viewCountQuery};
}

function orderQuery(issueQuery: EntityQuery<{issue: Issue}, []>, order: Order) {
  switch (order) {
    case Order.Created:
      return issueQuery.desc('created');
    case Order.Modified:
      return issueQuery.desc('modified');
    case Order.Status:
      return issueQuery.desc('status', 'modified');
    case Order.Priority:
      return issueQuery.desc('priority', 'modified');
    case Order.Kanban:
      return issueQuery.asc('kanbanOrder');
  }
}

export default App;
