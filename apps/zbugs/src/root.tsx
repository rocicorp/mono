import {ZeroProvider} from '@rocicorp/zero/react';
import {useCallback, useState, useSyncExternalStore} from 'react';
import {Route, Switch} from 'wouter';
import {Nav} from './components/nav.js';
import {ErrorPage} from './pages/error/error-page.js';
import {IssuePage} from './pages/issue/issue-page.js';
import {ListPage} from './pages/list/list-page.js';
import {routes} from './routes.js';
import {zeroRef} from './zero-setup.js';

export function Root() {
  const z = useSyncExternalStore(
    zeroRef.onChange,
    useCallback(() => zeroRef.value, []),
  );

  const [contentReady, setContentReady] = useState(false);

  if (!z) {
    return null;
  }

  return (
    <ZeroProvider zero={z}>
      <div
        className="app-container flex p-8"
        style={{visibility: contentReady ? 'visible' : 'hidden'}}
      >
        <div className="primary-nav w-48 shrink-0 grow-0">
          <Nav />
        </div>
        <div className="primary-content">
          <Switch>
            <Route path={routes.home}>
              <ListPage onReady={() => setContentReady(true)} />
            </Route>
            <Route path={routes.issue}>
              {params => (
                <IssuePage
                  key={params.id}
                  onReady={() => setContentReady(true)}
                />
              )}
            </Route>
            <Route component={ErrorPage} />
          </Switch>
        </div>
      </div>
    </ZeroProvider>
  );
}
