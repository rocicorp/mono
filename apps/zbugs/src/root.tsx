import {useConnectionState} from '../shared/zero-hooks.ts';
import {useEffect, useState} from 'react';
import {Redirect, Route, Switch} from 'wouter';
import {ZERO_PROJECT_NAME} from '../shared/schema.ts';
import {LoadingSpinner} from './components/loading-spinner.tsx';
import {Nav} from './components/nav.tsx';
import {useLogin} from './hooks/use-login.tsx';
import {useSoftNav} from './hooks/use-softnav.ts';
import {ErrorPage} from './pages/error/error-page.tsx';
import {IssuePage, IssueRedirect} from './pages/issue/issue-page.tsx';
import {ListPage} from './pages/list/list-page.tsx';
import {
  isGigabugs,
  links,
  ListContextProvider,
  routes,
  useProjectName,
} from './routes.tsx';

function OGImageUpdater() {
  const projectName = useProjectName();

  useEffect(() => {
    const ogImage = isGigabugs(projectName)
      ? 'https://zero.rocicorp.dev/api/og?title=Gigabugs&subtitle=2.5%20Million%20Row%20Sync%20Demo&logo=zero'
      : `${window.location.origin}/og-image.png`;

    // Update OG image meta tags
    const ogImageTag = document.querySelector('meta[property="og:image"]');
    if (ogImageTag) {
      ogImageTag.setAttribute('content', ogImage);
    }

    const twitterImageTag = document.querySelector(
      'meta[name="twitter:image"]',
    );
    if (twitterImageTag) {
      twitterImageTag.setAttribute('content', ogImage);
    }

    // Update alt text
    const ogImageAlt = document.querySelector('meta[property="og:image:alt"]');
    if (ogImageAlt) {
      ogImageAlt.setAttribute(
        'content',
        isGigabugs(projectName)
          ? 'Gigabugs - 2.5 Million Row Sync Demo'
          : 'Zero Bugs logo',
      );
    }

    const twitterImageAlt = document.querySelector(
      'meta[name="twitter:image:alt"]',
    );
    if (twitterImageAlt) {
      twitterImageAlt.setAttribute(
        'content',
        isGigabugs(projectName)
          ? 'Gigabugs - 2.5 Million Row Sync Demo'
          : 'Zero Bugs logo',
      );
    }
  }, [projectName]);

  return null;
}

export function Root() {
  const [contentReady, setContentReady] = useState(false);

  useSoftNav();

  const login = useLogin();
  const connectionState = useConnectionState();

  // if we're in needs-auth state, log out the user
  useEffect(() => {
    if (connectionState.name === 'needs-auth') {
      login.logout();
    }
  }, [connectionState, login]);

  return (
    <ListContextProvider>
      {!contentReady && <LoadingSpinner />}
      <div
        className="app-container flex p-8"
        style={{visibility: contentReady ? 'visible' : 'hidden'}}
      >
        <Switch>
          <Route path={routes.home}>
            <Redirect
              to={`${links.list({projectName: ZERO_PROJECT_NAME.toLocaleLowerCase()})}${window.location.search}`}
              replace
            />
          </Route>
          <Route path={routes.deprecatedIssue}>
            <IssueRedirect onReady={() => setContentReady(true)} />
          </Route>
          <Route path="/p/:projectName" nest>
            <OGImageUpdater />
            <div className="primary-nav w-48 shrink-0 grow-0">
              <Nav />
            </div>
            <div className="primary-content">
              <Route path="/">
                <ListPage onReady={() => setContentReady(true)} />
              </Route>
              <Route path="/issue/:id">
                {params => (
                  <IssuePage
                    key={params.id}
                    onReady={() => setContentReady(true)}
                  />
                )}
              </Route>
            </div>
          </Route>
          <Route component={ErrorPage} />
        </Switch>
      </div>
    </ListContextProvider>
  );
}
