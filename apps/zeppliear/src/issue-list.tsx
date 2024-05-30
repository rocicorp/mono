import {CSSProperties, memo, useCallback, useEffect, useRef} from 'react';
import AutoSizer from 'react-virtualized-auto-sizer';
import {FixedSizeList} from 'react-window';
import type {Zero} from 'zero-client';
import type {Collections} from './app.js';
import {useZero} from './hooks/use-zero.js';
import IssueRowLoading from './issue-row-loading.js';
import IssueRow from './issue-row.jsx';
import {
  commentsForIssueQuery,
  type Issue,
  type IssueUpdate,
  type Priority,
  type Status,
} from './issue.js';
import type {IssuesProps} from './issues-props.js';
import {ListData, useListData} from './list-data.js';

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

function deprioritizePreloadingComments(
  zero: Zero<Collections>,
  issueID: string,
) {
  const index = preloadQueue.indexOf(issueID);
  if (index > -1) {
    preloadQueue.splice(index, 1);

    if (lowPriorityPreloadQueue.includes(issueID)) {
      return;
    }
    lowPriorityPreloadQueue.unshift(issueID);
    if (lowPriorityPreloadQueue.length > 250) {
      lowPriorityPreloadQueue.length = 250;
    }
    void processPreloadQueues(zero);
  }
}

let preloadQueueProcessing = false;
async function processPreloadQueues(zero: Zero<Collections>) {
  if (preloadQueueProcessing) {
    return;
  }
  preloadQueueProcessing = true;
  try {
    while (preloadQueue.length > 0 || lowPriorityPreloadQueue.length > 0) {
      console.debug(
        'comment preload queue sizes',
        preloadQueue.length,
        lowPriorityPreloadQueue.length,
      );
      const p: Promise<unknown>[] = [];
      for (let i = 0; i < 5; i++) {
        const highPriority = preloadQueue.length > 0;
        const issueID = highPriority
          ? preloadQueue.shift()
          : lowPriorityPreloadQueue.shift();
        if (!issueID) {
          break;
        }
        console.debug(
          'preloading comments for',
          issueID,
          'highPriority?',
          highPriority,
        );
        p.push(
          commentsForIssueQuery(zero, issueID)
            .prepare()
            .preload()
            .preloaded.then(() => {
              console.debug('preloaded comments for', issueID);
            }),
        );
      }
      await Promise.all(p);
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

const loadingIndicatorKey = 'loading-indicator-key';

const itemKey = (index: number, data: ListData) => {
  if (data.isLoadingIndicator(index)) {
    return loadingIndicatorKey;
  }
  return data.mustGetIssue(index).issue.id;
};

function RawRow({
  data,
  index,
  style,
}: {
  data: ListData;
  index: number;
  style: CSSProperties;
}) {
  const zero = useZero<Collections>();
  const isLoadingIndicator = data.isLoadingIndicator(index);
  const issueID = isLoadingIndicator ? null : data.mustGetIssue(index).issue.id;

  useEffect(() => {
    if (issueID === null) {
      return;
    }
    const timeout = setTimeout(() => {
      preloadComments(zero, issueID);
    }, 250);
    return () => {
      clearTimeout(timeout);
      deprioritizePreloadingComments(zero, issueID);
    };
  }, [zero, issueID]);

  return (
    <div style={style}>
      {isLoadingIndicator ? (
        <IssueRowLoading />
      ) : (
        <IssueRow
          row={data.mustGetIssue(index)}
          onChangePriority={data.onChangePriority}
          onChangeStatus={data.onChangeStatus}
          onOpenDetail={data.onOpenDetail}
        />
      )}
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
