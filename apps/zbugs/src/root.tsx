import {useConnectionState} from '@rocicorp/zero/react';
import {useEffect, useState} from 'react';
import {Redirect, Route, Switch} from 'wouter';
import {ZERO_PROJECT_NAME} from '../shared/schema.ts';
import {
  getDemoLoadTime,
  LoadingSpinner,
} from './components/loading-spinner.tsx';
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

function DemoLoadedPill({
  loadTimeMs,
  compact = false,
}: {
  loadTimeMs: number;
  compact?: boolean | undefined;
}) {
  const [visible, setVisible] = useState(true);
  const [fading, setFading] = useState(false);

  useEffect(() => {
    const fadeTimer = setTimeout(() => setFading(true), 8000);
    const hideTimer = setTimeout(() => setVisible(false), 8500);
    return () => {
      clearTimeout(fadeTimer);
      clearTimeout(hideTimer);
    };
  }, []);

  if (!visible) {
    return null;
  }

  const seconds = (loadTimeMs / 1000).toFixed(1);

  return (
    <div className={`demo-loaded-pill ${fading ? 'fading' : ''}`}>
      <div className="demo-loaded-pill-header">
        {compact ? `Loaded in ${seconds} seconds.` : `Tada! Loaded in ${seconds} seconds.`}
      </div>
      {!compact && (
        <p className="demo-loaded-pill-body">
          Zero syncs millions of rows to IndexedDB in seconds, enabling instant
          queries and offline support.
        </p>
      )}
    </div>
  );
}

export function Root() {
  const [contentReady, setContentReady] = useState(false);
  const [demoLoadTime, setDemoLoadTime] = useState<number | null>(null);

  useSoftNav();

  const login = useLogin();
  const connectionState = useConnectionState();

  // if we're in needs-auth state, log out the user
  useEffect(() => {
    if (connectionState.name === 'needs-auth') {
      login.logout();
    }
  }, [connectionState, login]);

  const qs = new URLSearchParams(window.location.search);
  const isDemoMode = qs.has('demo');
  const isDemoVideo = qs.has('demovideo');
  const spinnerStay = isDemoMode && qs.has('spinnerstay');

  // Capture demo load time when content becomes ready
  useEffect(() => {
    if (
      contentReady &&
      (isDemoMode || isDemoVideo) &&
      !spinnerStay &&
      demoLoadTime === null
    ) {
      const loadTime = getDemoLoadTime();
      if (loadTime !== null) {
        setDemoLoadTime(loadTime);
      }
    }
  }, [contentReady, isDemoMode, isDemoVideo, spinnerStay, demoLoadTime]);

  return (
    <ListContextProvider>
      {(!contentReady || spinnerStay) && (
        <LoadingSpinner forceShow={spinnerStay} />
      )}
      {demoLoadTime !== null && !spinnerStay && (
        <DemoLoadedPill loadTimeMs={demoLoadTime} compact={isDemoVideo} />
      )}
      <div
        className="app-container flex p-8"
        style={{
          visibility: contentReady && !spinnerStay ? 'visible' : 'hidden',
        }}
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
