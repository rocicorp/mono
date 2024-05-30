import {UndoManager} from '@rocicorp/undo';
import {createRoot} from 'react-dom/client';
import {must} from 'shared/src/must.js';
import {EntityQuery, FromSet, Zero} from 'zero-client';
import App, {Collections} from './app.jsx';
import {ZeroProvider} from './hooks/use-zero.jsx';
import './index.css';
import {
  commentsForIssuesQuery,
  type Comment,
  type Issue,
  type IssueLabel,
  type Label,
  type Member,
} from './issue.js';
import * as agg from '@rocicorp/zql/src/zql/query/agg.js';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type TODO = any;
async function preload(z: Zero<Collections>) {
  const allMembersPreload = z.query.member.select('id', 'name');
  allMembersPreload.prepare().preload();

  const allLabelsPreload = z.query.label.select('id', 'name');
  allLabelsPreload.prepare().preload();

  const preloadIssueLimit = 2000;
  const preloadIssueIncrement = 1000;
  const issueBaseQuery = z.query.issue
    .leftJoin(
      z.query.issueLabel,
      'issueLabel',
      'issue.id',
      'issueLabel.issueID',
    )
    .leftJoin(z.query.label, 'label', 'issueLabel.labelID', 'label.id')
    .select(
      'issue.created',
      'issue.creatorID',
      'issue.description',
      'issue.id',
      'issue.kanbanOrder',
      'issue.priority',
      'issue.modified',
      'issue.status',
      'issue.title',
      agg.array('label.name', 'labels'),
    )
    .groupBy('issue.id');

  const issueSorts: Parameters<typeof issueBaseQuery.desc>[] = [
    ['issue.modified'],
    ['issue.created'],
  ];

  for (const issueSort of issueSorts) {
    await incrementalPreload(
      `issues order by ${issueSort.join(', ')} desc`,
      issueBaseQuery.desc(...issueSort) as TODO,
      preloadIssueLimit,
      preloadIssueIncrement,
    );
  }

  if (new URL(location.href).searchParams.get('commentPreload') === 'true') {
    console.log('Aggressive comment preload beginning!!!');
    let preloadStarted = false;
    const aggresiveCommentPreloadLimit = 10000;
    const cleanup = z.query.issue
      .select('id')
      .desc('issue.modified')
      .limit(aggresiveCommentPreloadLimit)
      .prepare()
      .subscribe(async (results, resultType) => {
        if (resultType === 'complete') {
          cleanup();
          if (preloadStarted) {
            return;
          }
          preloadStarted = true;
          let preloaded = 0;
          const ids = [];
          for (const {id} of results) {
            ids.push(id);
            if (ids.length === 500) {
              await commentsForIssuesQuery(z, ids).prepare().preload()
                .preloaded;
              preloaded += ids.length;
              ids.length = 0;
              console.log(
                'Aggressive comment preload progress',
                preloaded,
                '/',
                aggresiveCommentPreloadLimit,
              );
            }
          }
          console.log('Aggressive comment preload done.');
        }
      });
  }

  console.debug('COMPLETED PRELOAD');
}

async function incrementalPreload<F extends FromSet, R>(
  description: string,
  baseQuery: EntityQuery<F, R[]>,
  targetLimit: number,
  increment: number,
  currentLimit = 0,
): Promise<() => void> {
  if (currentLimit === 0) {
    currentLimit = increment;
    console.debug('STARTING preload of', description);
  }
  currentLimit = Math.min(currentLimit, targetLimit);
  console.debug('incremental preload', description, {
    currentLimit,
    targetLimit,
  });
  let done = false;
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  let lastCleanup: () => void = () => {};
  for (let currentLimit = increment; !done; currentLimit += increment) {
    currentLimit = Math.min(targetLimit, currentLimit);
    const createdPreloadStatement = baseQuery.limit(currentLimit).prepare();
    console.debug('incremental preload', description, {
      currentLimit,
      targetLimit,
    });
    const {cleanup, preloaded} = createdPreloadStatement.preload();
    lastCleanup?.();
    lastCleanup = cleanup;
    await preloaded;
    if (currentLimit === targetLimit) {
      done = true;
    }
  }
  console.debug('COMPLETED preload of', description);
  return lastCleanup;
}

function init() {
  const z = new Zero({
    logLevel: 'info',
    server: import.meta.env.VITE_PUBLIC_SERVER,
    userID: 'anon',
    queries: {
      issue: v => v as Issue,
      comment: v => v as Comment,
      label: v => v as Label,
      issueLabel: v => v as IssueLabel,
      member: v => v as Member,
    },
  });
  const undoManager = new UndoManager();

  setTimeout(() => preload(z), 2_000);

  function Home({
    zero,
    undoManager,
  }: {
    zero: Zero<Collections>;
    undoManager: UndoManager;
  }) {
    return (
      <div className="repliear">
        <ZeroProvider zero={zero}>
          <App undoManager={undoManager}></App>
        </ZeroProvider>
      </div>
    );
  }

  const root = must(document.getElementById('root'));
  createRoot(root).render(<Home zero={z} undoManager={undoManager} />);
}

init();
