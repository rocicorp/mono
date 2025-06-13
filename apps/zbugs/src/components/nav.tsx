import {FPSMeter} from '@schickling/fps-meter';
import classNames from 'classnames';
import {memo, useCallback, useEffect, useMemo, useState} from 'react';
import {useRoute, useSearch} from 'wouter';
import {navigate, useHistoryState} from 'wouter/use-browser-location';
import {useQuery} from 'zero-react/src/use-query.js';
import logoURL from '../assets/images/logo.svg';
import markURL from '../assets/images/mark.svg';
import {useLogin} from '../hooks/use-login.tsx';
import {IssueComposer} from '../pages/issue/issue-composer.tsx';
import {
  links,
  routes,
  type ListContext,
  type ZbugsHistoryState,
} from '../routes.ts';
import {AvatarImage} from './avatar-image.tsx';
import {ButtonWithLoginCheck} from './button-with-login-check.tsx';
import {Button} from './button.tsx';
import {Link} from './link.tsx';
import {queries} from '../../shared/schema.ts';

export const Nav = memo(() => {
  const search = useSearch();
  const qs = useMemo(() => new URLSearchParams(search), [search]);
  const [isHome] = useRoute(routes.home);
  const zbugsHistoryState = useHistoryState<ZbugsHistoryState | undefined>();
  const listContext = zbugsHistoryState?.zbugsListContext;
  const status = getStatus(isHome, qs, listContext);
  const login = useLogin();
  const [isMobile, setIsMobile] = useState(false);
  const [showUserPanel, setShowUserPanel] = useState(false); // State to control visibility of user-panel-mobile
  const [user] = useQuery(
    queries.user.where('id', login.loginState?.decoded.sub ?? '').one(),
  );

  const [showIssueModal, setShowIssueModal] = useState(false);

  const loginHref = links.login(
    window.location.pathname,
    window.location.search,
  );

  const newIssue = useCallback(() => {
    setShowIssueModal(true);
  }, []);

  useEffect(() => {
    const checkMobile = () => {
      setIsMobile(window.innerWidth <= 900);
    };

    checkMobile();
    window.addEventListener('resize', checkMobile);

    return () => {
      window.removeEventListener('resize', checkMobile);
    };
  }, []);

  const handleClick = useCallback(() => {
    setShowUserPanel(!showUserPanel); // Toggle the user panel visibility
  }, [showUserPanel]);

  return (
    <>
      <div className="nav-container flex flex-col">
        <Link className="logo-link-container" href="/">
          <img src={logoURL} className="zero-logo" />
          <img src={markURL} className="zero-mark" />
        </Link>
        {/* could not figure out how to add this color to tailwind.config.js */}
        <ButtonWithLoginCheck
          className="primary-cta"
          eventName="New issue modal"
          onAction={newIssue}
          loginMessage="You need to be logged in to create a new issue."
        >
          <span className="primary-cta-text">New Issue</span>
        </ButtonWithLoginCheck>

        <div className="section-tabs">
          <Link
            href={addStatusParam(qs, undefined)}
            eventName="Toggle open issues"
            className={classNames('nav-item', {
              'nav-active': status === 'open',
            })}
          >
            Open
          </Link>
          <Link
            href={addStatusParam(qs, 'closed')}
            eventName="Toggle closed issues"
            className={classNames('nav-item', {
              'nav-active': status === 'closed',
            })}
          >
            Closed
          </Link>
          <Link
            href={addStatusParam(qs, 'all')}
            eventName="Toggle all issues"
            className={classNames('nav-item', {
              'nav-active': status === 'all',
            })}
          >
            All
          </Link>
        </div>

        <div className="user-login">
          {import.meta.env.DEV && (
            <FPSMeter className="fps-meter" width={192} height={38} />
          )}
          {login.loginState === undefined ? (
            <a href={loginHref}>Login</a>
          ) : (
            user && (
              <div className="logged-in-user-container">
                <div className="logged-in-user">
                  {isMobile ? (
                    <div className="mobile-login-container">
                      <Button
                        eventName="Toggle user options (mobile)"
                        onAction={handleClick}
                      >
                        <AvatarImage
                          user={user}
                          className="issue-creator-avatar"
                          title={user.login}
                        />
                      </Button>
                      <div
                        className={classNames('user-panel-mobile', {
                          hidden: !showUserPanel, // Conditionally hide/show the panel
                        })}
                      >
                        <Button
                          className="logout-button-mobile"
                          eventName="Log out (mobile)"
                          onAction={login.logout}
                          title="Log out"
                        >
                          Log out
                        </Button>
                      </div>
                    </div>
                  ) : (
                    <AvatarImage
                      user={user}
                      className="issue-creator-avatar"
                      title={user.login}
                    />
                  )}
                  <span className="logged-in-user-name">
                    {login.loginState?.decoded.name}
                  </span>
                </div>
                <Button
                  className="logout-button"
                  eventName="Log out"
                  onAction={login.logout}
                  title="Log out"
                ></Button>
              </div>
            )
          )}
        </div>
      </div>
      <IssueComposer
        isOpen={showIssueModal}
        onDismiss={id => {
          setShowIssueModal(false);
          if (id) {
            navigate(links.issue({id}));
          }
        }}
      />
    </>
  );
});

const addStatusParam = (
  qs: URLSearchParams,
  status: 'closed' | 'all' | undefined,
) => {
  const newParams = new URLSearchParams(qs);
  if (status === undefined) {
    newParams.delete('status');
  } else {
    newParams.set('status', status);
  }
  return '/?' + newParams.toString();
};

function getStatus(
  isHome: boolean,
  qs: URLSearchParams,
  listContext: ListContext | undefined,
) {
  if (isHome) {
    const status = qs.get('status')?.toLowerCase();
    switch (status) {
      case 'closed':
        return 'closed';
      case 'all':
        return 'all';
      default:
        return 'open';
    }
  }
  if (listContext) {
    const open = listContext.params.open;
    switch (open) {
      case true:
        return 'open';
      case false:
        return 'closed';
      default:
        return 'all';
    }
  }
  return undefined;
}
