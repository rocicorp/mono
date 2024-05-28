import {resolver} from '@rocicorp/resolver';
import {UndoManager} from '@rocicorp/undo';
import ReactDOM from 'react-dom/client';
import {EntityQuery, FromSet, Zero} from 'zero-client';
import App, {Collections} from './app.jsx';
import {ZeroProvider} from './hooks/use-zero.jsx';
import './index.css';
import type {Comment, Issue, IssueLabel, Label, Member} from './issue.js';
import * as agg from '@rocicorp/zql/src/zql/query/agg.js';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type TODO = any;
async function preload(z: Zero<Collections>) {
  const allMembersPreload = z.query.member.select('id', 'name');
  allMembersPreload.prepare().preload();

  const preloadIssueLimit = 10_000;
  const preloadIssueIncrement = 500;
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
    ['issue.created'],
    ['issue.modified'],
    ['issue.status', 'issue.modified'],
    ['issue.priority', 'issue.modified'],
  ];
  for (const issueSort of issueSorts) {
    const [stmt, unsub] = await incrementalPreload(
      `issues order by ${issueSort.join(', ')} desc`,
      issueBaseQuery.desc(...issueSort) as TODO,
      preloadIssueLimit,
      preloadIssueIncrement,
    );
    // hacky conversion to a preload statement
    // so we no longer maintain the view.
    stmt.preload();
    unsub();
  }
}

function incrementalPreload<F extends FromSet, R>(
  description: string,
  baseQuery: EntityQuery<F, R[]>,
  targetLimit: number,
  increment: number,
  currentLimit = 0,
): Promise<[ReturnType<typeof baseQuery.prepare>, () => void]> {
  if (currentLimit === 0) {
    currentLimit = increment;
    console.debug('STARTING preload of', description);
  }
  currentLimit = Math.min(currentLimit, targetLimit);
  const createdPreloadStatement = baseQuery.limit(currentLimit).prepare();

  console.debug('incremental preload', description, {
    currentLimit,
    targetLimit,
  });
  const {resolve, promise} =
    resolver<[ReturnType<typeof baseQuery.prepare>, () => void]>();
  let done = false;
  const unsub = createdPreloadStatement.subscribe(result => {
    console.debug('incremental preload', description, 'got', {
      currentLimit,
      targetLimit,
      resultLength: result.length,
    });
    if (currentLimit >= targetLimit && !done) {
      done = true;
      console.debug('COMPLETED preload of', description);
      resolve([createdPreloadStatement, unsub]);
    }
    if (result.length >= currentLimit && currentLimit < targetLimit) {
      incrementalPreload(
        description,
        baseQuery,
        targetLimit,
        increment,
        currentLimit + increment,
      ).then(resolve);
      unsub();
      createdPreloadStatement.destroy();
    }
  });
  return promise;
}

async function init() {
  const z = new Zero({
    logLevel: 'debug',
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
          <App undoManager={undoManager} />
        </ZeroProvider>
      </div>
    );
  }

  ReactDOM.createRoot(document.getElementById('root')!).render(
    <Home zero={z} undoManager={undoManager} />,
  );
}

await init();
