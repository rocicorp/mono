import {Delete as DeleteIcon, Edit as EditIcon} from '@mui/icons-material';
import {
  WhereCondition,
  and,
  exp,
  or,
} from '@rocicorp/zql/src/zql/query/entity-query.js';
import {nanoid} from 'nanoid';
import {useCallback, useState} from 'react';
import {Remark} from 'react-remark';
import {must} from 'shared/src/must.js';
import type {EntityQuery} from 'zero-client';
import type {Collections} from './app.jsx';
import ArrowIcon from './assets/icons/arrow.svg?react';
import DefaultAvatarIcon from './assets/icons/avatar.svg?react';
import CloseIcon from './assets/icons/close.svg?react';
import ConfirmationModal from './confirm-modal.jsx';
import {useIssueDetailState} from './hooks/query-state-hooks.js';
import {useKeyPressed} from './hooks/use-key-pressed.js';
import {useQuery} from './hooks/use-query.js';
import {useZero} from './hooks/use-zero.js';
import {
  Comment,
  CommentCreationPartial,
  Issue,
  IssueUpdate,
  Label,
  Order,
  Priority,
  Status,
  commentsForIssueQuery,
  orderQuery,
} from './issue.js';
import type {IssuesProps} from './issues-props.js';
import PriorityMenu from './priority-menu.jsx';
import StatusMenu from './status-menu.jsx';
import {timeAgo} from './util/date.js';

interface Props {
  onUpdateIssues: (issueUpdates: {issue: Issue; update: IssueUpdate}[]) => void;
  onAddComment: (comment: CommentCreationPartial) => void;
  issuesProps: IssuesProps;
  isLoading: boolean;
  userID: string;
}

function CommentsList(
  comments: {comment: Comment; member: {name: string}}[],
  isLoading: boolean,
) {
  const elements = comments.map(({comment, member}) => (
    <div
      key={comment.id}
      className="max-w-[85vw] mx-3 bg-gray-850 mt-0 mb-5 border-transparent rounded py-3 px-3 relative whitespace-pre-wrap overflow-auto"
    >
      <div className="h-6 mb-1 -mt-px relative">
        <DefaultAvatarIcon className="w-4.5 h-4.5 rounded-full overflow-hidden flex-shrink-0 float-left mr-2" />
        {member.name} {timeAgo(comment.created)}
      </div>
      <div className="block flex-1 whitespace-pre-wrap">
        <Remark>{comment.body}</Remark>
      </div>
    </div>
  ));
  if (isLoading) {
    elements.push(
      <div
        key="loading"
        className="max-w-[85vw] mx-3 bg-gray-400 mt-0 mb-5 border-transparent rounded py-3 px-3 relative whitespace-pre-wrap overflow-auto"
      >
        Loading...
      </div>,
    );
  }
  return elements;
}

export default function IssueDetail({
  onUpdateIssues,
  onAddComment,
  issuesProps,
  isLoading,
}: Props) {
  const [detailIssueID, setDetailIssueID] = useIssueDetailState();

  const [editMode, setEditMode] = useState(false);

  const [commentText, setCommentText] = useState('');
  const [titleText, setTitleText] = useState('');
  const [descriptionText, setDescriptionText] = useState('');

  const [isDeleteModalOpen, setDeleteModalOpen] = useState(false);
  const zero = useZero<Collections>();

  const issueSelectQuery = zero.query.issue.select(
    'issue.created',
    'issue.creatorID',
    'issue.description',
    'issue.id',
    'issue.kanbanOrder',
    'issue.priority',
    'issue.modified',
    'issue.status',
    'issue.title',
  );
  const issue: Issue | null =
    useQuery(issueSelectQuery.where('id', '=', detailIssueID ?? ''), [
      detailIssueID,
    ])[0] ?? null;

  const {query, queryDeps, order} = issuesProps;
  const nextIssues = useQuery(
    getNextIssueQuery(query, issue, order, 'fwd'),
    queryDeps.concat(issue),
  );
  const previousIssues = useQuery(
    getNextIssueQuery(query, issue, order, 'prev'),
    queryDeps.concat(issue),
  );

  const comments = useQuery(commentsForIssueQuery(zero, detailIssueID ?? ''), [
    detailIssueID,
  ]);

  const handleClose = useCallback(() => {
    setDetailIssueID(null);
  }, [setDetailIssueID]);

  const handleChangePriority = useCallback(
    (priority: Priority) => {
      issue && onUpdateIssues([{issue, update: {id: issue.id, priority}}]);
    },
    [onUpdateIssues, issue],
  );

  const handleChangeStatus = useCallback(
    (status: Status) => {
      issue && onUpdateIssues([{issue, update: {id: issue.id, status}}]);
    },
    [onUpdateIssues, issue],
  );

  const handleAddComment = useCallback(() => {
    if (commentText !== '' && issue) {
      onAddComment({
        id: nanoid(),
        issueID: issue.id,
        body: commentText,
      });
      setCommentText('');
    }
  }, [onAddComment, commentText, issue]);

  const handleFwdPrev = useCallback(
    (direction: 'prev' | 'fwd') => {
      let gotoIssue: {issue: Issue};
      if (direction === 'fwd') {
        if (nextIssues.length < 2) {
          return;
        }
        gotoIssue = nextIssues[1];
      } else {
        if (previousIssues.length < 2) {
          return;
        }
        gotoIssue = previousIssues[1];
      }
      setDetailIssueID(gotoIssue.issue.id);
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [nextIssues[0], previousIssues[0], setDetailIssueID],
  );

  const handleFwd = useCallback(() => {
    handleFwdPrev('fwd');
  }, [handleFwdPrev]);

  const handlePrev = useCallback(() => {
    handleFwdPrev('prev');
  }, [handleFwdPrev]);

  useKeyPressed('j', handleFwd);
  useKeyPressed('k', handlePrev);

  const handleEdit = () => {
    setTitleText(issue?.title || '');
    setDescriptionText(issue?.description || '');
    setEditMode(true);
  };

  const handleCancel = () => {
    setEditMode(false);
  };

  const handleSave = () => {
    if (issue) {
      let update: IssueUpdate = {id: issue.id};
      if (descriptionText !== issue.description) {
        update = {...update, description: descriptionText};
      }
      if (titleText !== issue.title) {
        update = {...update, title: titleText};
      }
      onUpdateIssues([
        {
          issue,
          update,
        },
      ]);
    }
    setEditMode(false);
  };

  const handleDelete = () => {
    setDeleteModalOpen(true);
  };

  const handleDeleteConfirm = async () => {
    await zero.mutate.issue.delete({id: must(detailIssueID)});
    handleDismiss();
    handleClose();
  };

  const handleDismiss = () => {
    setDeleteModalOpen(false);
  };

  return (
    <div className="flex flex-col flex-grow m-3 rounded-md shadow-mdw-7xl border-gray-850 border min-h-0 min-w-0">
      <ConfirmationModal
        isOpen={isDeleteModalOpen}
        onDismiss={handleDismiss}
        onConfirm={handleDeleteConfirm}
        title="Are you sure you want to delete this issue?"
        message=""
        action="Delete"
      />
      <div className="flex bg-gray-850 border border-gray-700 justify-around">
        <div className="flex-1 p-2">
          <div className="flex flex-row flex-initial ml-3">
            <div
              className="inline-flex items-center justify-center h-6 w-6 rounded hover:bg-gray-400  cursor-pointer"
              onMouseDown={handleClose}
            >
              <CloseIcon className="w-4" />
            </div>
            {issue && (
              <>
                <div className="flex flex-row flex-initial select-none cursor-pointer">
                  <button
                    className="h-6 px-2 rounded border-solid border inline-flex items-center justify-center flex-shrink-0 font-medium m-0 select-none whitespace-no-wrap ml-2  hover:bg-gray-400 disabled:opacity-25"
                    type="button"
                    onMouseDown={() => handleFwdPrev('prev')}
                    disabled={previousIssues.length < 2}
                  >
                    <ArrowIcon
                      style={{transform: 'rotate(180deg)'}}
                      className=""
                    />
                  </button>
                </div>
                <div
                  role="button"
                  className="flex flex-row flex-initial select-none cursor-pointer"
                >
                  <button
                    className="h-6 px-2 rounded border-solid border inline-flex items-center justify-center flex-shrink-0 font-medium m-0 select-none whitespace-no-wrap ml-2  hover:bg-gray-400 disabled:opacity-50"
                    type="button"
                    onMouseDown={() => handleFwdPrev('fwd')}
                    disabled={nextIssues.length < 2}
                  >
                    <ArrowIcon className="" />
                  </button>
                </div>
              </>
            )}
          </div>
        </div>
      </div>
      <div className="flex flex-1 p-2 overflow-hidden">
        <div className="flex flex-col flex-[3_0_0] items-center md:p-3 border-gray-700 md:border-r min-h-0 min-w-0 overflow-auto">
          <div className="flex flex-col w-full lg:max-w-4xl max-w-[90vw]">
            <div className="flex border-solid border-b lg:px-5 justify-between px-2">
              <div className="flex visible md:invisible">
                <StatusMenu
                  onSelect={handleChangeStatus}
                  status={issue?.status || Status.Backlog}
                  labelVisible={true}
                />
                <PriorityMenu
                  onSelect={handleChangePriority}
                  labelVisible={true}
                  priority={issue?.priority || Priority.None}
                />
              </div>
              {editMode ? (
                <div className="text-sm flex mb-1">
                  <button
                    className="px-2 ml-2 rounded hover:bg-indigo-700 focus:outline-none bg-gray-850 text-white"
                    onMouseDown={handleSave}
                  >
                    Save
                  </button>
                  <button
                    className="px-2 ml-2 rounded hover:bg-indigo-700 focus:outline-none bg-gray-700 text-white"
                    onMouseDown={handleCancel}
                  >
                    Cancel
                  </button>
                </div>
              ) : (
                <div className="flex mb-1">
                  <div className="text-sm mr-2">
                    <EditIcon
                      className="!w-4 cursor-pointer"
                      onMouseDown={handleEdit}
                    />
                  </div>
                  <div className="text-sm">
                    <DeleteIcon
                      className="!w-4 cursor-pointer"
                      onMouseDown={handleDelete}
                    />
                  </div>
                </div>
              )}
            </div>
            <div className="flex flex-col border-solid border-b px-5">
              <div className="text-md py-4">
                {editMode ? (
                  <input
                    className="block px-2 py-1 whitespace-pre-wrap text-size-sm w-full bg-gray-850 placeholder-gray-300 placeholder:text-sm"
                    onChange={e => setTitleText(e.target.value)}
                    value={titleText}
                  />
                ) : (
                  issue?.title
                )}
              </div>
              <div className="text-sm pb-4 text-gray-100 overflow-auto whitespace-pre-wrap">
                {editMode ? (
                  <textarea
                    className="block  px-2 py-1 whitespace-pre-wrap text-size-sm w-full bg-gray-850 h-[calc(100vh-340px)] placeholder-gray-300 placeholder:text-sm"
                    onChange={e => setDescriptionText(e.target.value)}
                    value={descriptionText}
                  />
                ) : isLoading && issue?.description === null ? (
                  'Loading...'
                ) : (
                  <Remark>{issue?.description || ''}</Remark>
                )}
              </div>
            </div>
            <div className="text-md py-4 px-5 text-white">Comments</div>
            {CommentsList(comments, isLoading && issue?.description === null)}
            <div className="mx-3 bg-gray-850 flex-1 mx- mt-0 mb-3 flex-1 border-transparent rounded full py-3 px-3 relative whitespace-pre-wrap ">
              <textarea
                className="block flex-1 whitespace-pre-wrap text-size-sm w-full bg-gray-850 min-h-[6rem] placeholder-gray-300 placeholder:text-sm"
                placeholder="Leave a comment ..."
                onChange={e => setCommentText(e.target.value)}
                value={commentText}
              />
              <div className="flex justify-end">
                <button
                  className="px-3 ml-2 mt-2 rounded h-8 focus:outline-none bg-gray text-white "
                  onMouseDown={handleAddComment}
                >
                  Comment
                </button>
              </div>
            </div>
          </div>
        </div>
        <div className="hidden md:block flex flex-[1_0_0] min-w-0 p-3">
          <div className="max-w-4xl">
            <div className="flex border-solid border-b px-5">
              {/* For consistent spacing with left col */}
              <div className="text-sm invisible">
                <EditIcon className="!w-4" />
              </div>
            </div>
            <div className="flex flex-col px-5 py-4 text-sm">
              <div className="flex flex-row items-center my-1">
                <div className="w-20">Status</div>
                <StatusMenu
                  onSelect={handleChangeStatus}
                  status={issue?.status || Status.Backlog}
                  labelVisible={true}
                />
              </div>
              <div className="flex flex-row items-center my-1">
                <div className="w-20">Priority</div>
                <PriorityMenu
                  onSelect={handleChangePriority}
                  labelVisible={true}
                  priority={issue?.priority || Priority.None}
                />
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

type FS = {
  issue: Issue;
  label: Label;
};

type Q = EntityQuery<FS, {issue: Issue; labels: string[]}[]>;

function getNextIssueQuery(
  issueSelectQuery: Q,
  issue: Issue | null,
  order: Order,
  direction: 'fwd' | 'prev',
): Q {
  if (issue === null) {
    return issueSelectQuery.where('issue.id', '<', '').limit(0);
  }

  const filteredAndOrderedQuery = orderQuery(
    issueSelectQuery,
    order,
    direction === 'prev',
  );

  let op: '<' | '>' = direction === 'fwd' ? '<' : '>';

  let primary: 'created' | 'modified' | 'status' | 'priority' | 'kanbanOrder';
  switch (order) {
    case Order.Created:
      primary = 'created';
      break;
    case Order.Modified:
      primary = 'modified';
      break;
    case Order.Status:
      primary = 'status';
      break;
    case Order.Priority:
      primary = 'priority';
      break;
    case Order.Kanban:
      primary = 'kanbanOrder';
      // also flip op for kanban order
      op = direction === 'fwd' ? '>' : '<';
      break;
  }

  let whereClause: WhereCondition<FS> = or(
    and(
      exp(`issue.${primary}`, '=', issue[primary]),
      exp('issue.id', op === '<' ? '<=' : '>=', issue.id),
    ),
    exp(`issue.${primary}`, op, issue[primary]),
  );

  if (order === Order.Status || order === Order.Priority) {
    // These sort issue[primary] desc, issue.modified desc, issue.id desc
    whereClause = or(
      and(
        exp(`issue.${primary}`, '=', issue[primary]),
        exp('issue.modified', '=', issue.modified),
        exp('issue.id', op === '<' ? '<=' : '>=', issue.id),
      ),
      and(
        exp(`issue.${primary}`, '=', issue[primary]),
        exp('issue.modified', op, issue.modified),
      ),
      exp(`issue.${primary}`, op, issue[primary]),
    );
  }

  const query = filteredAndOrderedQuery.where(whereClause).limit(2);

  return query;
}
