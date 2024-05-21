import {CSSProperties, memo, useCallback, useEffect, useRef} from 'react';
import AutoSizer from 'react-virtualized-auto-sizer';
import {FixedSizeList} from 'react-window';
import type {Issue, IssueUpdate, Priority, Status} from './issue';
import IssueRow from './issue-row';
import type {IssuesProps} from './issues-props.js';
import {ListData, useListData} from './list-data.js';

interface Props {
  issuesProps: IssuesProps;
  onUpdateIssues: (issueUpdates: {issue: Issue; update: IssueUpdate}[]) => void;
  onOpenDetail: (issue: Issue) => void;
  view: string | null;
}

const itemKey = (index: number, data: ListData) =>
  data.getIssue(index).issue.id;

function RawRow({
  data,
  index,
  style,
}: {
  data: ListData;
  index: number;
  style: CSSProperties;
}) {
  return (
    <div style={style}>
      <IssueRow
        row={data.getIssue(index)}
        onChangePriority={data.onChangePriority}
        onChangeStatus={data.onChangeStatus}
        onOpenDetail={data.onOpenDetail}
      />
    </div>
  );
}

// eslint-disable-next-line @typescript-eslint/naming-convention
const Row = memo(RawRow);

function IssueList({issuesProps, onUpdateIssues, onOpenDetail, view}: Props) {
  const fixedSizeListRef = useRef<FixedSizeList>(null);
  useEffect(() => {
    fixedSizeListRef.current?.scrollTo(0);
  }, [view]);

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

  const handleChangeStatus = useCallback(
    (issue: Issue, status: Status) => {
      onUpdateIssues([
        {
          issue,
          update: {id: issue.id, status},
        },
      ]);
    },
    [onUpdateIssues],
  );

  const itemData = useListData({
    issuesProps,
    onChangePriority: handleChangePriority,
    onChangeStatus: handleChangeStatus,
    onOpenDetail,
  });

  return (
    <div className="flex flex-col flex-grow overflow-auto">
      <AutoSizer>
        {({height, width}: {width: number; height: number}) => (
          <FixedSizeList
            ref={fixedSizeListRef}
            height={height}
            itemCount={itemData.count}
            itemSize={43}
            itemData={itemData}
            itemKey={itemKey}
            overscanCount={10}
            width={width}
          >
            {Row}
          </FixedSizeList>
        )}
      </AutoSizer>
    </div>
  );
}

export default memo(IssueList);
