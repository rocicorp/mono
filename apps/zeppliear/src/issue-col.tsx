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
import {IssueItemByID} from './issue-item-by-id.jsx';
import IssueItem from './issue-item.jsx';
import type {Issue, Priority, Status} from './issue.js';
import {IssuesProps, useIssuesProps} from './issues-props.js';
import {ListData, useListData} from './list-data.js';
import StatusIcon from './status-icon.jsx';

interface Props {
  issuesProps: IssuesProps;
  status: Status;
  title: string;
  onChangePriority: (issue: Issue, priority: Priority) => void;
  onOpenDetail: (issue: Issue) => void;
  // This is used to update the list data in the parent component.
  onListData: (status: Status, listData: ListData) => void;
}

interface RowProps {
  index: number;
  data: ListData;
  style: CSSProperties;
}

function RowPreMemo({data, index, style}: RowProps) {
  // We are rendering an extra item for the placeholder.
  // To do this we increased our data set size to include one 'fake' item.
  const issue = data.getIssue(index);
  if (!issue || data.isLoadingIndicator(index)) {
    return null;
  }

  return (
    <Draggable draggableId={issue.id} index={index} key={issue.id}>
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
            issue={issue}
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
  onListData,
}: Props) {
  const issuesProps = useIssuesProps(
    query.where('status', '=', status),
    queryDeps.concat(status),
    order,
  );
  const listData = useListData({
    issuesProps,
    onChangePriority,
    onChangeStatus: () => void 0,
    onOpenDetail,
  });
  onListData(status, listData);

  const statusIcon = <StatusIcon className="flex-shrink-0" status={status} />;
  return (
    <div className="flex flex-col pr-3 flex-1 select-none min-w-[9rem]">
      {/* column title */}
      <div className="flex items-center pb-3 text-sm whitespace-nowrap overflow-hidden">
        {statusIcon}
        <div className="ml-3 mr-3 font-medium">{title}</div>
        <div className="mr-3 font-normal text-gray-400">{listData.count}</div>
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
              listData.count + (snapshot.isUsingPlaceholder ? 1 : 0);

            return (
              <AutoSizer>
                {({height, width}) => (
                  <FixedSizeList
                    height={height}
                    itemCount={itemCount}
                    itemSize={100}
                    width={width}
                    outerRef={provided.innerRef}
                    itemData={listData}
                    onItemsRendered={listData.onItemsRendered}
                    overscanCount={10}
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
