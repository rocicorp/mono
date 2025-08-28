import Cookies from 'js-cookie';
import {createContext, useState} from 'react';
import {Route, Switch} from 'wouter';
import {Nav} from './components/nav.tsx';
import {OnboardingModal} from './components/onboarding-modal.tsx';
import {useSoftNav} from './hooks/use-softnav.ts';
import {ErrorPage} from './pages/error/error-page.tsx';
import {IssuePage} from './pages/issue/issue-page.tsx';
import {ListPage} from './pages/list/list-page.tsx';
import {routes} from './routes.ts';
import {MaybeSuspend} from './components/maybe-suspend.tsx';

type InitialLoadState = 'loading' | 'complete';
export const LoadContext = createContext<InitialLoadState>('loading');

export function Root() {
  const [showOnboarding, setShowOnboarding] = useState(
    () => !Cookies.get('onboardingDismissed'),
  );

  const [loadState, setLoadState] = useState<InitialLoadState>('loading');

  useSoftNav();

  return (
    <>
      <MaybeSuspend
        enabled={loadState === 'loading'}
        onReveal={() => setLoadState('complete')}
      >
        <LoadContext.Provider value={loadState}>
          <div className="app-container flex p-8">
            <div className="primary-nav w-48 shrink-0 grow-0">
              <Nav />
            </div>
            <div className="primary-content">
              <Switch>
                <Route path={routes.home}>
                  <ListPage />
                </Route>
                <Route path={routes.issue}>
                  {params => <IssuePage key={params.id} />}
                </Route>
                <Route component={ErrorPage} />
              </Switch>
            </div>
          </div>
        </LoadContext.Provider>
      </MaybeSuspend>
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
