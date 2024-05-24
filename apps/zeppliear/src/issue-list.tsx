import {
  CSSProperties,
  memo,
  useCallback,
  useEffect,
  useRef,
  useState,
} from 'react';
import AutoSizer from 'react-virtualized-auto-sizer';
import {FixedSizeList} from 'react-window';
import IssueRow from './issue-row.jsx';
import type {Issue, IssueUpdate, Priority, Status} from './issue.js';
import type {IssuesProps} from './issues-props.js';
import {ListData, useListData} from './list-data.js';
import {useQuery} from './hooks/use-query.js';
import {useZero} from './hooks/use-zero.js';
import type {Collections} from './app.js';
import {useTimeout} from './hooks/use-timeout.js';

interface Props {
  issuesProps: IssuesProps;
  onUpdateIssues: (issueUpdates: {issue: Issue; update: IssueUpdate}[]) => void;
  onOpenDetail: (issue: Issue) => void;
  view: string | null;
}

const itemKey = (index: number, data: ListData) =>
  data.mustGetIssue(index).issue.id;

function RawRow({
  data,
  index,
  style,
}: {
  data: ListData;
  index: number;
  style: CSSProperties;
}) {
  const row = data.mustGetIssue(index);
  const issueID = row.issue.id;

  const zero = useZero<Collections>();

  const [timerFired, setTimerFired] = useState(false);
  useTimeout(() => {
    console.log('Preloading issue', issueID);
    setTimerFired(true);
  }, 500);

  // preload for detail view
  const comments = useQuery(
    zero.query.comment
      .where('issueID', '=', issueID ?? '')
      .join(zero.query.member, 'member', 'comment.creatorID', 'member.id')
      .select(
        'comment.id',
        'comment.issueID',
        'comment.created',
        'comment.creatorID',
        'comment.body',
        'member.name',
      )
      .asc('comment.created'),
    [issueID],
    timerFired,
  );

  if (comments.length > 0) {
    console.log('issue row preloaded', issueID);
  }

  return (
    <div style={style}>
      <IssueRow
        row={row}
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
            onItemsRendered={itemData.onItemsRendered}
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
