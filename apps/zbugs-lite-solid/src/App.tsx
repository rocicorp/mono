import {useQuery, useZero} from '@rocicorp/zero/solid';
import {createEffect, createSignal, For, Show} from 'solid-js';
import {IssueRow} from './IssueRow.tsx';
import {useLogin} from './login-provider.tsx';
import {mutators, queries, ZERO_PROJECT_ID} from './zero.ts';

export function App() {
  const z = useZero();
  const login = useLogin();
  const [newTitle, setNewTitle] = createSignal('');

  console.log('[App] Zero instance:', z(), 'clientID:', z().clientID);

  // Use issueList query from zbugs (registered with the server)
  const [issues, result] = useQuery(() =>
    queries.issueList({
      listContext: {
        open: true,
        projectName: 'zero',
        assignee: null,
        creator: null,
        labels: null,
        textFilter: null,
        sortField: 'modified',
        sortDirection: 'desc',
      },
      userID: 'anon',
      limit: 20,
    }),
  );

  createEffect(() => {
    console.log(
      '[App] Query result type:',
      result().type,
      'issues count:',
      issues()?.length ?? 0,
    );
    const issueList = issues();
    if (issueList && issueList.length > 0) {
      console.log('[App] First issue:', issueList[0]?.title);
    }
  });

  const createIssue = () => {
    const title = newTitle().trim();
    if (!title) return;

    const now = Date.now();
    z().mutate(
      mutators.issue.create({
        id: crypto.randomUUID(),
        title,
        created: now,
        modified: now,
        projectID: ZERO_PROJECT_ID,
      }),
    );
    setNewTitle('');
  };

  const handleKeyDown = (e: KeyboardEvent) => {
    if (e.key === 'Enter') {
      createIssue();
    }
  };

  const loginHref = `/api/login/github?redirect=${encodeURIComponent(window.location.pathname + window.location.search)}`;
  const loginState = login.loginState();

  return (
    <div class="app">
      <header>
        <div
          style={{
            'display': 'flex',
            'justify-content': 'space-between',
            'align-items': 'center',
          }}
        >
          <div>
            <h1>zsolid</h1>
            <p class="subtitle">Zero + Solid reactivity test</p>
          </div>
          <div
            style={{'display': 'flex', 'gap': '10px', 'align-items': 'center'}}
          >
            <Show when={loginState} fallback={<a href={loginHref}>Login</a>}>
              <span>{loginState?.decoded.name}</span>
              <button onClick={login.logout}>Logout</button>
            </Show>
          </div>
        </div>
      </header>
      <main>
        <div class="create-issue">
          <input
            class="create-issue-input"
            type="text"
            placeholder="New issue title..."
            value={newTitle()}
            onInput={e => setNewTitle(e.currentTarget.value)}
            onKeyDown={handleKeyDown}
          />
          <button class="create-issue-button" onClick={createIssue}>
            Create
          </button>
        </div>
        <div class="issue-list">
          <div style={{'font-size': '12px', 'color': '#888'}}>
            Query state: {result().type} | Count: {issues()?.length ?? 'N/A'}
          </div>
          <Show when={issues()} fallback={<div>Loading...</div>}>
            {issueList => (
              <For each={issueList()}>
                {issue => <IssueRow issue={issue} />}
              </For>
            )}
          </Show>
        </div>
      </main>
    </div>
  );
}
