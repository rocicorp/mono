import {UndoManager} from '@rocicorp/undo';
import ReactDOM from 'react-dom/client';
import {EntityQuery, Zero, FromSet} from 'zero-client';
import App, {Collections} from './app.jsx';
import {ZeroProvider} from './hooks/use-zero.jsx';
import './index.css';
import type {Comment, Issue, IssueLabel, Label, Member} from './issue.js';
import {resolver} from '@rocicorp/resolver';

async function preload(z: Zero<Collections>) {
  const allLabelPreloadQuery = z.query.label.select('id', 'name');
  allLabelPreloadQuery.prepare().preload();

  const allMembersPreload = z.query.member.select('id', 'name');
  allMembersPreload.prepare().preload();

  const preloadIssueLimit = 10_000;
  const preloadIssueIncrement = 500;
  const issueBaseQuery = z.query.issue.select(
    'created',
    'creatorID',
    'description',
    'id',
    'kanbanOrder',
    'priority',
    'modified',
    'status',
    'title',
  );

  const issueSorts: Parameters<typeof issueBaseQuery.desc>[] = [
    ['created'],
    ['modified'],
    ['status', 'modified'],
    ['priority', 'modified'],
  ];
  for (const issueSort of issueSorts) {
    await incrementalPreload(
      `issues order by ${issueSort.join(', ')} desc`,
      issueBaseQuery.desc(...issueSort),
      preloadIssueLimit,
      preloadIssueIncrement,
    );
  }

  await incrementalPreload(
    'issueLabels',
    z.query.issueLabel.select('id', 'issueID', 'labelID'),
    100_000,
    5_000,
  );
}

function incrementalPreload<F extends FromSet, R>(
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
  const createdPreloadStatement = baseQuery.limit(currentLimit).prepare();

  console.debug('incremental preload', description, {
    currentLimit,
    targetLimit,
  });
  const {resolve, promise} = resolver<() => void>();
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
      resolve(unsub);
    }
    if (result.length === currentLimit && currentLimit < targetLimit) {
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
    server: import.meta.env.VITE_PUBLIC_SERVER,
    userID: 'anon',
    kvStore: 'idb',
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
