import {ZeroProvider} from '@rocicorp/zero/react';
import {useCallback, useSyncExternalStore} from 'react';
import {Route, Switch} from 'wouter';
import {Nav} from './components/nav.js';
import ErrorPage from './pages/error/error-page.js';
import IssuePage from './pages/issue/issue-page.js';
import ListPage from './pages/list/list-page.js';
import {zeroRef} from './zero-setup.js';
import {routes} from './routes.js';

export default function Root() {
  const z = useSyncExternalStore(
    zeroRef.onChange,
    useCallback(() => zeroRef.value, []),
  );

  if (!z) {
    return null;
  }

  return (
    <ZeroProvider zero={z}>
      <div className="app-container flex p-8">
        <div className="primary-nav w-48 shrink-0 grow-0">
          <Nav />
        </div>
        <div className="primary-content">
          <Switch>
            <Route path={routes.home} component={ListPage} />
            <Route path={routes.issue} component={IssuePage} />
            <Route component={ErrorPage} />
          </Switch>
        </div>
      </div>
    </ZeroProvider>
  );
}
