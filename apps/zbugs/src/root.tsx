import Cookies from 'js-cookie';
import {useState} from 'react';
import {Redirect, Route, Switch} from 'wouter';
import {Nav} from './components/nav.tsx';
import {OnboardingModal} from './components/onboarding-modal.tsx';
import {useSoftNav} from './hooks/use-softnav.ts';
import {ErrorPage} from './pages/error/error-page.tsx';
import {IssuePage, IssueRedirect} from './pages/issue/issue-page.tsx';
import {ListPage} from './pages/list/list-page.tsx';
import {routes} from './routes.ts';
import {ZERO_PROJECT_NAME} from '../shared/schema.ts';

export function Root() {
  const [contentReady, setContentReady] = useState(false);
  const [showOnboarding, setShowOnboarding] = useState(
    () => !Cookies.get('onboardingDismissed'),
  );

  useSoftNav();

  return (
    <>
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
              <Redirect
                to={`/p/${ZERO_PROJECT_NAME.toLocaleLowerCase()}${window.location.search}`}
                replace
              />
            </Route>
            <Route path={routes.list}>
              <ListPage onReady={() => setContentReady(true)} />
            </Route>
            <Route path={routes.deprecatedIssue}>
              <IssueRedirect></IssueRedirect>
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
      <OnboardingModal
        isOpen={showOnboarding}
        onDismiss={() => {
          Cookies.set('onboardingDismissed', 'true', {expires: 365});
          setShowOnboarding(false);
        }}
      />
    </>
  );
}
