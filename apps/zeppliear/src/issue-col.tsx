import {CSSProperties, memo} from 'react';
import {
  Draggable,
  DraggableProvided,
  Droppable,
  DroppableProvided,
  DroppableStateSnapshot,
} from 'react-beautiful-dnd';
import AutoSizer from 'react-virtualized-auto-sizer';
import {FixedSizeList} from 'react-window';
import type {Issue, IssueWithLabels, Priority, Status} from './issue';
import IssueItem from './issue-item';
import {IssueItemByID} from './issue-item-by-id';
import {IssuesProps, useIssuesProps} from './issues-props.js';
import {useListData} from './list-data.js';
import StatusIcon from './status-icon';

interface Props {
  issuesProps: IssuesProps;
  status: Status;
  title: string;
  onChangePriority: (issue: Issue, priority: Priority) => void;
  onOpenDetail: (issue: Issue) => void;
}

type ListData = {
  getIssue(index: number): IssueWithLabels;
  readonly onChangePriority: (issue: Issue, priority: Priority) => void;
  readonly onChangeStatus: (issue: Issue, status: Status) => void;
  readonly onOpenDetail: (issue: Issue) => void;
};

interface RowProps {
  index: number;
  data: ListData;
  // data: {
  //   issues: Array<IssueWithLabels>;
  //   onChangePriority: (issue: Issue, priority: Priority) => void;
  //   onOpenDetail: (issue: Issue) => void;
  // };
  style: CSSProperties;
}

function RowPreMemo({data, index, style}: RowProps) {
  const issue = data.getIssue(index);
  // We are rendering an extra item for the placeholder.
  // To do this we increased our data set size to include one 'fake' item.
  if (!issue) {
    return null;
  }

  return (
    <Draggable draggableId={issue.issue.id} index={index} key={issue.issue.id}>
      {(provided: DraggableProvided) => (
        <div
          {...provided.draggableProps}
          {...provided.dragHandleProps}
          style={{
            ...provided.draggableProps.style,
            ...style,
          }}
          ref={provided.innerRef}
        >
          <IssueItem
            issue={issue.issue}
            key={index}
            onChangePriority={data.onChangePriority}
            onOpenDetail={data.onOpenDetail}
          />
        </div>
      )}
    </Draggable>
  );
}

// eslint-disable-next-line @typescript-eslint/naming-convention
const Row = memo(RowPreMemo);

function IssueCol({
  issuesProps: {query, queryDeps, order},
  title,
  status,
  onChangePriority,
  onOpenDetail,
}: Props) {
  const issuesProps = useIssuesProps(
    query.where('issue.status', '=', status),
    queryDeps.concat(status),
    order,
  );
  const itemData = useListData({
    issuesProps,
    onChangePriority,
    onChangeStatus: () => void 0,
    onOpenDetail,
  });

  const statusIcon = <StatusIcon className="flex-shrink-0" status={status} />;
  return (
    <div className="flex flex-col pr-3 flex-1 select-none min-w-[9rem]">
      {/* column title */}
      <div className="flex items-center pb-3 text-sm whitespace-nowrap overflow-hidden">
        {statusIcon}
        <div className="ml-3 mr-3 font-medium">{title}</div>
        <div className="mr-3 font-normal text-gray-400">{itemData.count}</div>
      </div>

      {/* list of issues */}
      <div className="flex flex-col flex-1">
        <Droppable
          droppableId={status.toString()}
          key={status}
          type="category"
          mode="virtual"
          renderClone={(provided, _snapshot, rubric) => (
            <div
              ref={provided.innerRef}
              {...provided.draggableProps}
              {...provided.dragHandleProps}
            >
              <IssueItemByID
                issueID={rubric.draggableId}
                issuesProps={issuesProps}
              />
            </div>
          )}
        >
          {(provided: DroppableProvided, snapshot: DroppableStateSnapshot) => {
            // Add an extra item to our list to make space for a dragging item
            // Usually the DroppableProvided.placeholder does this, but that won't
            // work in a virtual list
            const itemCount =
              itemData.count + (snapshot.isUsingPlaceholder ? 1 : 0);

            return (
              <AutoSizer>
                {({height, width}) => (
                  <FixedSizeList
                    height={height}
                    itemCount={itemCount}
                    itemSize={100}
                    width={width}
                    outerRef={provided.innerRef}
                    itemData={itemData}
                  >
                    {Row}
                  </FixedSizeList>
                )}
              </AutoSizer>
            );
          }}
        </Droppable>
      </div>
    </div>
  );
}

export default memo(IssueCol);
