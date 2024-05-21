import type {UndoManager} from '@rocicorp/undo';
import * as agg from '@rocicorp/zql/src/zql/query/agg.js';
import classnames from 'classnames';
import {pickBy} from 'lodash';
import {memo, useCallback, useState} from 'react';
import {HotKeys} from 'react-hotkeys';
import type {EntityQuery, Zero} from 'zero-client';
import {getIssueOrder, getViewStatuses} from './filters';
import {
  FiltersState,
  useFilters,
  useIssueDetailState,
  useOrderByState,
  useViewState,
} from './hooks/query-state-hooks';
import {useZero} from './hooks/use-zero';
import {useQuery} from './hooks/use-zql';
import {
  Comment,
  Issue,
  IssueCreationPartial,
  IssueLabel,
  IssueUpdate,
  Label,
  Member,
  createIssue,
  deleteIssueComment,
  putIssueComment,
  updateIssues,
} from './issue';
import IssueBoard from './issue-board';
import IssueDetail from './issue-detail';
import IssueList from './issue-list';
import {useIssuesProps, type IssuesProps} from './issues-props.js';
import LeftMenu from './left-menu';
import TopFilter from './top-filter';

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
  member: Member;
  issue: Issue;
  comment: Comment;
  label: Label;
  issueLabel: IssueLabel;
};

type AppProps = {
  undoManager: UndoManager;
};

const crewNames = ['holden', 'naomi', 'alex', 'amos', 'bobbie'];
const activeUserName = crewNames[Math.floor(Math.random() * crewNames.length)];

// eslint-disable-next-line @typescript-eslint/naming-convention
const App = ({undoManager}: AppProps) => {
  const [view] = useViewState();
  const filters = useFilters();
  const [orderBy] = useOrderByState();
  const [detailIssueID, setDetailIssueID] = useIssueDetailState();
  const [menuVisible, setMenuVisible] = useState(false);
  const zero = useZero<Collections>();

  // Sync the user rows for the entire crew so that when we pick a random
  // crew member below to be the current active user, we already have it
  // synced.
  useQuery(
    zero.query.member.select('id', 'name').where('name', 'IN', crewNames),
  );

  const userID =
    useQuery(
      zero.query.member.select('id').where('name', '=', activeUserName),
    ).at(0)?.id ?? '';

  const issueQuery = zero.query.issue;

  // TODO: fix kanban
  //const allIssues = useQuery(issueQuery.select('kanbanOrder').limit(200));

  const issueListQuery = issueQuery
    .leftJoin(
      zero.query.issueLabel,
      'issueLabel',
      'issue.id',
      'issueLabel.issueID',
    )
    .leftJoin(zero.query.label, 'label', 'issueLabel.labelID', 'label.id');
  const {filteredQuery} = filterQuery(issueListQuery, view, filters);

  const issueOrder = getIssueOrder(view, orderBy);
  const issueQueryDeps = [view, filters, issueOrder] as const;
  const viewIssueCount = 0;

  const issuesProps = useIssuesProps(filteredQuery, issueQueryDeps, issueOrder);
  // ========= Ghetto pre-loads
  const fields = [
    'created',
    'creatorID',
    'description',
    'id',
    'kanbanOrder',
    'priority',
    'modified',
    'status',
    'title',
  ] as const;
  const preloadIssueLimit = 10_000;
  const createdPreloadQuery = issueQuery
    .select(...fields)
    .limit(preloadIssueLimit)
    .desc('created');
  const modifiedPreloadQuery = issueQuery
    .select(...fields)
    .limit(preloadIssueLimit)
    .desc('modified');
  const statusPreloadQuery = issueQuery
    .select(...fields)
    .limit(preloadIssueLimit)
    .desc('status', 'modified');
  const priorityPreloadQuery = issueQuery
    .select(...fields)
    .limit(preloadIssueLimit)
    .desc('priority', 'modified');

  const allLabelPreloadQuery = zero.query.label.select('id', 'name');
  const allIssueLabelPreloadQuery = zero.query.issueLabel.select(
    'id',
    'issueID',
    'labelID',
  );
  useQuery(createdPreloadQuery);
  useQuery(modifiedPreloadQuery);
  useQuery(statusPreloadQuery);
  useQuery(priorityPreloadQuery);
  useQuery(allIssueLabelPreloadQuery);
  useQuery(allLabelPreloadQuery);
  // ========= Ghetto pre-loads

  const handleCreateIssue = useCallback(
    async (issue: IssueCreationPartial) => {
      // TODO: UndoManager? - audit every other place we're doing mutations,
      // or remove undo for now.
      await createIssue(zero, issue, userID);
    },
    [zero, userID],
  );
  const handleCreateComment = useCallback(
    async (comment: Comment) => {
      await undoManager.add({
        execute: () => putIssueComment(zero, comment),
        undo: () => deleteIssueComment(zero, comment),
      });
    },
    [zero, undoManager],
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
          updateIssues(zero, {
            issueUpdates: issueUpdates.map<IssueUpdate>(({update}) => update),
          }),
        undo: () => updateIssues(zero, {issueUpdates: uChanges}),
      });
    },
    [zero, undoManager],
  );

  const handleOpenDetail = useCallback(
    (issue: Issue) => setDetailIssueID(issue.id),
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
        issuesProps={issuesProps}
        hasNonViewFilters={filters.hasNonViewFilters}
        zero={zero}
        userID={userID}
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
  issuesProps: IssuesProps;
  hasNonViewFilters: boolean;
  zero: Zero<Collections>;
  userID: string;
  onCloseMenu: () => void;
  onToggleMenu: () => void;
  onUpdateIssues: (issueUpdates: {issue: Issue; update: IssueUpdate}[]) => void;
  onCreateIssue: (issue: IssueCreationPartial) => void;
  onCreateComment: (comment: Comment) => void;
  onOpenDetail: (issue: Issue) => void;
}

function RawLayout({
  menuVisible,
  view,
  detailIssueID,
  isLoading,
  viewIssueCount,
  issuesProps,
  hasNonViewFilters,
  userID,
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
                // TODO(arv): This is wrong. We need to know the count of the filtered issues
                // hasNonViewFilters ? filteredIssues.length : undefined
                hasNonViewFilters ? 123456 : undefined
              }
              issuesCount={viewIssueCount}
              showSortOrderMenu={view !== 'board'}
            />
          </div>
          <div className="relative flex flex-1 min-h-0">
            {detailIssueID && (
              <IssueDetail
                issuesProps={issuesProps}
                onUpdateIssues={onUpdateIssues}
                onAddComment={onCreateComment}
                isLoading={isLoading}
                userID={userID}
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
                  issuesProps={issuesProps}
                  onUpdateIssues={onUpdateIssues}
                  onOpenDetail={onOpenDetail}
                />
              ) : (
                <IssueList
                  onUpdateIssues={onUpdateIssues}
                  onOpenDetail={onOpenDetail}
                  view={view}
                  issuesProps={issuesProps}
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
  // TODO: having to know the from set and return type of the query to take it in as an arg is...
  // confusing at best.
  // TODO: having to know the `FromSet` is dumb.
  q: EntityQuery<{issue: Issue; label: Label}, []>,
  view: string | null,
  filters: FiltersState,
) {
  const viewStatuses = getViewStatuses(view);
  const viewStatusesQuery = viewStatuses
    ? q.where('issue.status', 'IN', [...viewStatuses])
    : q;

  const viewCountQuery = viewStatusesQuery
    .distinct('issue.id')
    .select(agg.count());

  if (filters.statusFilter) {
    // Consider allowing Iterable<T> for IN
    q = q.where('issue.status', 'IN', [...filters.statusFilter]);
  }
  if (filters.priorityFilter) {
    q = q.where('issue.priority', 'IN', [...filters.priorityFilter]);
  }

  let filteredQuery = q
    .groupBy('issue.id')
    .select(
      'issue.created',
      'issue.creatorID',
      'issue.description',
      'issue.id',
      'issue.kanbanOrder',
      'issue.priority',
      'issue.modified',
      'issue.status',
      'issue.title',
      agg.array('label.name', 'labels'),
    );
  if (filters.labelFilter) {
    // TODO: if `having` has been applied then selection
    // set should not be updated to remove what `having` operates against.
    filteredQuery = filteredQuery.having('labels', 'INTERSECTS', [
      ...filters.labelFilter,
    ]);
  }

  return {filteredQuery, viewCountQuery};
}

export default App;
