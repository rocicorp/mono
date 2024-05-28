import {CSSProperties, memo, useCallback, useEffect, useRef} from 'react';
import AutoSizer from 'react-virtualized-auto-sizer';
import {FixedSizeList} from 'react-window';
import IssueRow from './issue-row.jsx';
import type {Issue, IssueUpdate, Priority, Status} from './issue.js';
import type {IssuesProps} from './issues-props.js';
import {ListData, useListData} from './list-data.js';
import {useZero} from './hooks/use-zero.js';
import type {Collections} from './app.js';
import type {Zero} from 'zero-client';

const preloadQueue: string[] = [];
const lowPriorityPreloadQueue: string[] = [];

function preloadComments(zero: Zero<Collections>, issueID: string) {
  if (preloadQueue.includes(issueID)) {
    return;
  }
  const lowPriorityIndex = lowPriorityPreloadQueue.indexOf(issueID);
  if (lowPriorityIndex > -1) {
    lowPriorityPreloadQueue.splice(lowPriorityIndex, 1);
  }
  preloadQueue.push(issueID);
  void processPreloadQueues(zero);
}

function deprioritizcePreloadingComments(
  zero: Zero<Collections>,
  issueID: string,
) {
  const index = preloadQueue.indexOf(issueID);
  if (index > -1) {
    preloadQueue.splice(index, 1);
  }
  if (lowPriorityPreloadQueue.includes(issueID)) {
    return;
  }
  lowPriorityPreloadQueue.push(issueID);
  void processPreloadQueues(zero);
}

let preloadQueueProcessing = false;
async function processPreloadQueues(zero: Zero<Collections>) {
  if (preloadQueueProcessing) {
    return;
  }
  preloadQueueProcessing = true;
  try {
    while (preloadQueue.length > 0 || lowPriorityPreloadQueue.length > 0) {
      const highPriority = preloadQueue.length > 0;
      const issueID = highPriority
        ? preloadQueue.shift()
        : lowPriorityPreloadQueue.shift();
      console.debug(
        'preloading comments for',
        issueID,
        'highPriority?',
        highPriority,
      );
      await zero.query.comment
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
        .asc('comment.created')
        .prepare()
        .preload().preloaded;
      console.debug('preloaded comments for', issueID);
    }
  } finally {
    preloadQueueProcessing = false;
  }
}

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

  useEffect(() => {
    const timeout = setTimeout(() => {
      preloadComments(zero, issueID);
    }, 250);
    return () => {
      clearTimeout(timeout);
      deprioritizcePreloadingComments(zero, issueID);
    };
  }, [zero, issueID]);

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
