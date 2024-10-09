import {Zero} from '@rocicorp/zero';
import {ZeroProvider} from '@rocicorp/zero/react';
import {useEffect, useState} from 'react';
import {Route, Switch} from 'wouter';
import {Nav} from './components/nav.js';
import {type Schema, schema} from './domain/schema.js';
import {useLogin} from './hooks/use-login.js';
import ErrorPage from './pages/error/error-page.js';
import IssuePage from './pages/issue/issue-page.js';
import ListPage from './pages/list/list-page.js';
import {mark} from './perf-log.js';

export default function Root() {
  const login = useLogin();

  const [z, setZ] = useState<Zero<Schema> | undefined>();

  useEffect(() => {
    mark('root effect start');
    const z = new Zero({
      logLevel: 'info',
      server: import.meta.env.VITE_PUBLIC_SERVER,
      userID: login.loginState?.userID ?? 'anon',
      auth: login.loginState?.token,
      schema,
    });
    setZ(z);

    // To enable accessing zero in the devtools easily.
    (window as {z?: Zero<Schema>}).z = z;

    const baseIssueQuery = z.query.issue
      .related('creator')
      .related('labels')
      .related('comments', c => c.limit(10).related('creator'))
      .orderBy('modified', 'desc');

    const {complete} = baseIssueQuery.preload();
    complete.then(() => mark('issue preload complete'));

    z.query.user.preload();
    z.query.label.preload();

    return () => {
      z.close();
    };
  }, [login]);

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
            <Route path="/" component={ListPage} />
            <Route path="/issue/:id" component={IssuePage} />
            <Route component={ErrorPage} />
          </Switch>
        </div>
      </div>
    </ZeroProvider>
  );
}
