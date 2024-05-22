import {generateNKeysBetween} from 'fractional-indexing';
import {memo, useCallback, useMemo} from 'react';
import {DragDropContext, DropResult} from 'react-beautiful-dnd';
import IssueCol from './issue-col.jsx';
import {
  Issue,
  IssueUpdate,
  IssueWithLabels,
  Priority,
  Status,
} from './issue.js';
import type {IssuesProps} from './issues-props.js';
import type {ListData} from './list-data.js';
import {assert} from './util/asserts.js';

export type IssuesByStatusType = {
  [Status.Backlog]: IssueWithLabels[];
  [Status.Todo]: IssueWithLabels[];
  [Status.InProgress]: IssueWithLabels[];
  [Status.Done]: IssueWithLabels[];
  [Status.Canceled]: IssueWithLabels[];
};

function getKanbanOrderIssueUpdates(
  issueToMove: Issue,
  issueToInsertBefore: Issue,
  listData: ListData,
): {issue: Issue; update: IssueUpdate}[] {
  let beforeKey: string | null = null;

  for (const issue of listData.iterateIssuesBefore(issueToInsertBefore)) {
    beforeKey = issue.issue.kanbanOrder;
    break;
  }

  let afterKey: string | null = null;
  const issuesToReKey: Issue[] = [];
  // If the issues we are trying to move between
  // have identical kanbanOrder values, we need to fix up the
  // collision by re-keying the issues.
  for (const issue of listData.iterateIssuesAfter(issueToInsertBefore)) {
    if (issue.issue.kanbanOrder !== beforeKey) {
      afterKey = issue.issue.kanbanOrder;
      break;
    }
    issuesToReKey.push(issue.issue);
  }
  const newKanbanOrderKeys = generateNKeysBetween(
    beforeKey,
    afterKey,
    issuesToReKey.length + 1, // +1 for the dragged issue
  );

  const issueUpdates = [
    {
      issue: issueToMove,
      update: {id: issueToMove.id, kanbanOrder: newKanbanOrderKeys[0]},
    },
  ];
  for (let i = 0; i < issuesToReKey.length; i++) {
    issueUpdates.push({
      issue: issuesToReKey[i],
      update: {id: issuesToReKey[i].id, kanbanOrder: newKanbanOrderKeys[i + 1]},
    });
  }
  return issueUpdates;
}

interface Props {
  issuesProps: IssuesProps;
  onUpdateIssues: (issueUpdates: {issue: Issue; update: IssueUpdate}[]) => void;
  onOpenDetail: (issue: Issue) => void;
}

function IssueBoard({issuesProps, onUpdateIssues, onOpenDetail}: Props) {
  const listDataMap = useMemo(() => new Map<Status, ListData>(), []);
  const onListData = useCallback(
    (status: Status, listData: ListData) => {
      listDataMap.set(status, listData);
    },
    [listDataMap],
  );

  const handleDragEnd = useCallback(
    ({source, destination}: DropResult) => {
      if (!destination) {
        return;
      }
      const sourceStatus = parseInt(source.droppableId) as Status;

      const sourceListData = listDataMap.get(sourceStatus);
      assert(sourceListData);

      const draggedIssue = sourceListData.getIssue(source.index)?.issue;
      if (!draggedIssue) {
        return;
      }
      const destinationStatus = parseInt(destination.droppableId) as Status;
      const destinationIndex =
        sourceStatus === destinationStatus && source.index < destination.index
          ? destination.index + 1
          : destination.index;

      const destinationListData = listDataMap.get(destinationStatus);
      assert(destinationListData);
      const issueToInsertBefore =
        destinationListData.getIssue(destinationIndex)?.issue;
      if (draggedIssue === issueToInsertBefore) {
        return;
      }
      const issueUpdates = issueToInsertBefore
        ? getKanbanOrderIssueUpdates(
            draggedIssue,
            issueToInsertBefore,
            destinationListData,
          )
        : [{issue: draggedIssue, update: {id: draggedIssue.id}}];
      if (destinationStatus !== sourceStatus) {
        issueUpdates[0] = {
          ...issueUpdates[0],
          update: {
            ...issueUpdates[0].update,
            status: destinationStatus,
          },
        };
      }
      onUpdateIssues(issueUpdates);
    },
    [listDataMap, onUpdateIssues],
  );

  const handleChangePriority = useCallback(
    (issue: Issue, priority: Priority) => {
      onUpdateIssues([
        {
          issue,
          update: {id: issue.id, priority},
        },
      ]);
    },
    [onUpdateIssues],
  );

  return (
    <DragDropContext onDragEnd={handleDragEnd}>
      <div className="flex flex-1 pt-6 pl-8 overflow-scroll-x bg-gray border-color-gray-50 border-right-width-1">
        <IssueCol
          title={'Backlog'}
          status={Status.Backlog}
          issuesProps={issuesProps}
          onChangePriority={handleChangePriority}
          onOpenDetail={onOpenDetail}
          onListData={onListData}
        />
        <IssueCol
          title={'Todo'}
          status={Status.Todo}
          issuesProps={issuesProps}
          onChangePriority={handleChangePriority}
          onOpenDetail={onOpenDetail}
          onListData={onListData}
        />
        <IssueCol
          title={'In Progress'}
          status={Status.InProgress}
          issuesProps={issuesProps}
          onChangePriority={handleChangePriority}
          onOpenDetail={onOpenDetail}
          onListData={onListData}
        />
        <IssueCol
          title={'Done'}
          status={Status.Done}
          issuesProps={issuesProps}
          onChangePriority={handleChangePriority}
          onOpenDetail={onOpenDetail}
          onListData={onListData}
        />
        <IssueCol
          title={'Canceled'}
          status={Status.Canceled}
          issuesProps={issuesProps}
          onChangePriority={handleChangePriority}
          onOpenDetail={onOpenDetail}
          onListData={onListData}
        />
      </div>
    </DragDropContext>
  );
}

export default memo(IssueBoard);
