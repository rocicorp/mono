import { generateNKeysBetween } from "fractional-indexing";
import { groupBy, indexOf } from "lodash";
import React, { memo, useCallback } from "react";
import { DragDropContext, DropResult } from "react-beautiful-dnd";

import { Status, Issue, IssueUpdate, Priority } from "./issue";
import IssueCol from "./issue-col";

export type IssuesByStatusType = {
  [Status.BACKLOG]: Issue[];
  [Status.TODO]: Issue[];
  [Status.IN_PROGRESS]: Issue[];
  [Status.DONE]: Issue[];
  [Status.CANCELED]: Issue[];
};

export const getIssueByType = (allIssues: Issue[]): IssuesByStatusType => {
  const issuesBySType = groupBy(allIssues, "status");
  const defaultIssueByType = {
    [Status.BACKLOG]: [],
    [Status.TODO]: [],
    [Status.IN_PROGRESS]: [],
    [Status.DONE]: [],
    [Status.CANCELED]: [],
  };
  const result = { ...defaultIssueByType, ...issuesBySType };
  return result;
};

export function getKanbanOrderIssueUpdates(
  issueToMove: Issue,
  issueToInsertBefore: Issue,
  issues: Issue[]
): IssueUpdate[] {
  const indexInKanbanOrder = indexOf(issues, issueToInsertBefore);
  let beforeKey: string | null = null;
  if (indexInKanbanOrder > 0) {
    beforeKey = issues[indexInKanbanOrder - 1].kanbanOrder;
  }
  let afterKey: string | null = null;
  const issuesToReKey: Issue[] = [];
  // If the issues we are trying to move between
  // have identical kanbanOrder values, we need to fix up the
  // collision by re-keying the issues.
  for (let i = indexInKanbanOrder; i < issues.length; i++) {
    if (issues[i].kanbanOrder !== beforeKey) {
      afterKey = issues[i].kanbanOrder;
      break;
    }
    issuesToReKey.push(issues[i]);
  }
  const newKanbanOrderKeys = generateNKeysBetween(
    beforeKey,
    afterKey,
    issuesToReKey.length + 1 // +1 for the dragged issue
  );

  const issueUpdates = [
    {
      issue: issueToMove,
      issueChanges: { kanbanOrder: newKanbanOrderKeys[0] },
    },
  ];
  for (let i = 0; i < issuesToReKey.length; i++) {
    issueUpdates.push({
      issue: issuesToReKey[i],
      issueChanges: { kanbanOrder: newKanbanOrderKeys[i + 1] },
    });
  }
  return issueUpdates;
}

interface Props {
  issues: Issue[];
  onUpdateIssues: (issueUpdates: IssueUpdate[]) => void;
  onOpenDetail: (issue: Issue) => void;
}

function IssueBoard({ issues, onUpdateIssues, onOpenDetail }: Props) {
  const issuesByType = getIssueByType(issues);

  const handleDragEnd = useCallback(
    ({ source, destination }: DropResult) => {
      if (!destination) {
        return;
      }
      const sourceStatus = source?.droppableId as Status;
      const draggedIssue = issuesByType[sourceStatus][source.index];
      if (!draggedIssue) {
        return;
      }
      const newStatus = destination.droppableId as Status;
      const newIndex =
        sourceStatus === newStatus && source.index < destination.index
          ? destination.index + 1
          : destination.index;
      const issueToInsertBefore = issuesByType[newStatus][newIndex];
      if (draggedIssue === issueToInsertBefore) {
        return;
      }
      const issueUpdates = issueToInsertBefore
        ? getKanbanOrderIssueUpdates(draggedIssue, issueToInsertBefore, issues)
        : [{ issue: draggedIssue, issueChanges: {} }];
      if (newStatus !== sourceStatus) {
        issueUpdates[0] = {
          ...issueUpdates[0],
          issueChanges: {
            ...issueUpdates[0].issueChanges,
            status: newStatus,
          },
        };
      }
      onUpdateIssues(issueUpdates);
    },
    [issues, issuesByType, onUpdateIssues]
  );

  const handleChangePriority = useCallback(
    (issue: Issue, priority: Priority) => {
      onUpdateIssues([
        {
          issue,
          issueChanges: { priority },
        },
      ]);
    },
    [onUpdateIssues]
  );

  return (
    <DragDropContext onDragEnd={handleDragEnd}>
      <div className="flex flex-1 pt-6 pl-8 overflow-scroll-x bg-gray border-color-gray-50 border-right-width-1">
        <IssueCol
          title={"Backlog"}
          status={Status.BACKLOG}
          issues={issuesByType[Status.BACKLOG]}
          onChangePriority={handleChangePriority}
          onOpenDetail={onOpenDetail}
        />
        <IssueCol
          title={"Todo"}
          status={Status.TODO}
          issues={issuesByType[Status.TODO]}
          onChangePriority={handleChangePriority}
          onOpenDetail={onOpenDetail}
        />
        <IssueCol
          title={"In Progress"}
          status={Status.IN_PROGRESS}
          issues={issuesByType[Status.IN_PROGRESS]}
          onChangePriority={handleChangePriority}
          onOpenDetail={onOpenDetail}
        />
        <IssueCol
          title={"Done"}
          status={Status.DONE}
          issues={issuesByType[Status.DONE]}
          onChangePriority={handleChangePriority}
          onOpenDetail={onOpenDetail}
        />
        <IssueCol
          title={"Canceled"}
          status={Status.CANCELED}
          issues={issuesByType[Status.CANCELED]}
          onChangePriority={handleChangePriority}
          onOpenDetail={onOpenDetail}
        />
      </div>
    </DragDropContext>
  );
}

export default memo(IssueBoard);
