import classnames from 'classnames';
import {memo, useCallback, useEffect, useState} from 'react';
import type {Zero} from 'zero-client';
import {getIssueOrder} from './filters.js';
import {
  FiltersState,
  useFilters,
  useIssueDetailState,
  useOrderByState,
} from './hooks/query-state-hooks.js';
import {useQuery} from './hooks/use-query.js';
import {useZero} from './hooks/use-zero.js';
import IssueDetail from './issue-detail.jsx';
import IssueList from './issue-list.jsx';
import {
  CommentCreationPartial,
  Issue,
  IssueCreationPartial,
  IssueUpdate,
  createIssue,
  createIssueComment,
  updateIssues,
} from './issue.js';
import {useIssuesProps, type IssuesProps} from './issues-props.js';
import LeftMenu from './left-menu.jsx';
import TopFilter from './top-filter.jsx';
import {escapeLike} from './util/escape-like.js';
import {Schema} from './schema.js';
import {getIssueListQuery, IssueListQuery} from './queries.js';

const crewUserNames = ['holden', 'naomi', 'alex', 'amos', 'bobbie'];
const activeUserName =
  crewUserNames[Math.floor(Math.random() * crewUserNames.length)];

// eslint-disable-next-line @typescript-eslint/naming-convention
const App = () => {
  const filters = useFilters();
  const [orderBy] = useOrderByState();
  const [detailIssueID, setDetailIssueID] = useIssueDetailState();
  const [menuVisible, setMenuVisible] = useState(false);

  // Sorry zod – times change.
  const z = useZero<Schema>();

  // TODO: zql needs .one() to make this return string|undefined.
  // TODO: Should be able to say .where('name', activeUserName) (implying '=')
  const userID =
    useQuery(z.query.member.select('id').where('name', '=', activeUserName)).at(
      0,
    )?.id ?? '';

  useEffect(() => {
    console.debug({activeUserName, userID});
  }, [userID]);

  const issueListQuery = getIssueListQuery(z);
  const filteredQuery = filterQuery(issueListQuery, filters);

  const issueOrder = getIssueOrder(orderBy);
  const issueQueryDeps = [filters, issueOrder] as const;

  const issuesProps = useIssuesProps(filteredQuery, issueQueryDeps, issueOrder);

  const handleCreateIssue = useCallback(
    async (issue: IssueCreationPartial) => {
      await createIssue(z, issue, userID);
    },
    [z, userID],
  );
  const handleCreateComment = useCallback(
    async (comment: CommentCreationPartial) => {
      createIssueComment(z, comment, userID);
    },
    [z, userID],
  );

  const handleUpdateIssues = useCallback(
    async (issueUpdates: Array<{issue: Issue; update: IssueUpdate}>) => {
      updateIssues(z, {
        issueUpdates: issueUpdates.map<IssueUpdate>(({update}) => update),
      });
    },
    [z],
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

  return (
    <Layout
      menuVisible={menuVisible}
      detailIssueID={detailIssueID}
      // TODO: base on whether initial sync is done
      isLoading={false}
      issuesProps={issuesProps}
      zero={z}
      userID={userID}
      onCloseMenu={handleCloseMenu}
      onToggleMenu={handleToggleMenu}
      onUpdateIssues={handleUpdateIssues}
      onCreateIssue={handleCreateIssue}
      onCreateComment={handleCreateComment}
      onOpenDetail={handleOpenDetail}
    ></Layout>
  );
};

interface LayoutProps {
  menuVisible: boolean;
  detailIssueID: string | null;
  isLoading: boolean;
  issuesProps: IssuesProps;
  zero: Zero<Schema>;
  userID: string;
  onCloseMenu: () => void;
  onToggleMenu: () => void;
  onUpdateIssues: (issueUpdates: {issue: Issue; update: IssueUpdate}[]) => void;
  onCreateIssue: (issue: IssueCreationPartial) => void;
  onCreateComment: (comment: CommentCreationPartial) => void;
  onOpenDetail: (issue: Issue) => void;
}

function RawLayout({
  menuVisible,
  detailIssueID,
  isLoading,
  issuesProps,
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
            <TopFilter onToggleMenu={onToggleMenu} />
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
              <IssueList
                onUpdateIssues={onUpdateIssues}
                onOpenDetail={onOpenDetail}
                issuesProps={issuesProps}
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

// eslint-disable-next-line @typescript-eslint/naming-convention
const Layout = memo(RawLayout);

function filterQuery(q: IssueListQuery, filters: FiltersState) {
  if (filters.statusFilter) {
    q = q.where('status', 'IN', [...filters.statusFilter]);
  }
  if (filters.priorityFilter) {
    q = q.where('priority', 'IN', [...filters.priorityFilter]);
  }

  let filteredQuery = q;

  if (filters.textFilter) {
    filteredQuery = filteredQuery.where(
      'title',
      'ILIKE',
      `%${escapeLike(filters.textFilter)}%`,
    );
  }

  return filteredQuery;
}

export default App;
