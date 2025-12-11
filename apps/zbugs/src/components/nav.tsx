import {useQuery, useConnectionState} from '@rocicorp/zero/react';
import {FPSMeter} from '@schickling/fps-meter';
import classNames from 'classnames';
import {memo, useCallback, useEffect, useMemo, useState} from 'react';
import {useSearch} from 'wouter';
import {navigate} from 'wouter/use-browser-location';
import {queries, type ListContext} from '../../shared/queries.ts';
import logoGigabugsURL from '../assets/images/logo-gigabugs.svg';
import logoURL from '../assets/images/logo.svg';
import markURL from '../assets/images/mark.svg';
import {useIsOffline} from '../hooks/use-is-offline.ts';
import {useLogin} from '../hooks/use-login.tsx';
import {IssueComposer} from '../pages/issue/issue-composer.tsx';
import {isGigabugs, links, useListContext, useProjectName} from '../routes.tsx';
import {AvatarImage} from './avatar-image.tsx';
import {ButtonWithLoginCheck} from './button-with-login-check.tsx';
import {Button} from './button.tsx';
import {Link} from './link.tsx';
import {ProjectPicker} from './project-picker.tsx';

export const Nav = memo(() => {
  const search = useSearch();
  const qs = useMemo(() => new URLSearchParams(search), [search]);
  const {listContext} = useListContext();
  const status = getStatus(listContext);
  const projectName = useProjectName();
  const login = useLogin();
  const isOffline = useIsOffline();
  const [isMobile, setIsMobile] = useState(false);
  const [showUserPanel, setShowUserPanel] = useState(false); // State to control visibility of user-panel-mobile
  const [user] = useQuery(queries.user(login.loginState?.decoded.sub ?? ''));

  const [projects] = useQuery(queries.allProjects());
  const project = projects.find(
    p => p.lowerCaseName === projectName.toLocaleLowerCase(),
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
        <Link className="logo-link-container" href={links.list({projectName})}>
          <img
            src={
              (project?.logoURL ?? isGigabugs(projectName))
                ? logoGigabugsURL
                : logoURL
            }
            className="zero-logo"
          />
          <img src={project?.markURL ?? markURL} className="zero-mark" />
        </Link>{' '}
        {/* could not figure out how to add this color to tailwind.config.js */}
        <ButtonWithLoginCheck
          disabled={isOffline}
          className="primary-cta"
          eventName="New issue modal"
          onAction={newIssue}
          loginMessage="You need to be logged in to create a new issue."
        >
          <span className="primary-cta-text">New Issue</span>
        </ButtonWithLoginCheck>
        <div className="section-tabs">
          <div>
            <ProjectPicker
              projects={projects}
              selectedProjectName={projectName}
              onChange={value =>
                navigate(links.list({projectName: value.name}))
              }
            ></ProjectPicker>
          </div>
          <Link
            href={addStatusParam(projectName, qs, undefined)}
            eventName="Toggle open issues"
            className={classNames('nav-item', {
              'nav-active': status === 'open',
            })}
          >
            Open
          </Link>
          <Link
            href={addStatusParam(projectName, qs, 'closed')}
            eventName="Toggle closed issues"
            className={classNames('nav-item', {
              'nav-active': status === 'closed',
            })}
          >
            Closed
          </Link>
          <Link
            href={addStatusParam(projectName, qs, 'all')}
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
          <ConnectionStatusPill />
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
      {project && (
        <IssueComposer
          projectID={project.id}
          isOpen={showIssueModal}
          onDismiss={createdID => {
            setShowIssueModal(false);
            if (createdID) {
              navigate(links.issue({projectName, id: createdID}));
            }
          }}
        />
      )}
    </>
  );
});

const ConnectionStatusPill = () => {
  const connectionState = useConnectionState();

  // we wait to show the connecting status until after a short delay to avoid flickering
  const [shouldShowConnecting, setShouldShowConnecting] = useState(false);
  useEffect(() => {
    const timeout = setTimeout(() => {
      setShouldShowConnecting(true);
    }, 1000);

    return () => {
      clearTimeout(timeout);
    };
  });

  // Show error state immediately without delay
  if (connectionState.name === 'error') {
    return (
      <div className="connection-status-container">
        <div className="connection-status-pill">
          <svg
            xmlns="http://www.w3.org/2000/svg"
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <circle cx="12" cy="12" r="10" />
            <line x1="12" y1="8" x2="12" y2="12" />
            <line x1="12" y1="16" x2="12.01" y2="16" />
          </svg>
          <span>Error</span>
        </div>
      </div>
    );
  }

  return shouldShowConnecting &&
    (connectionState.name === 'connecting' ||
      connectionState.name === 'disconnected') ? (
    <div className="connection-status-container">
      <div className="connection-status-pill">
        <svg
          xmlns="http://www.w3.org/2000/svg"
          width="16"
          height="16"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <path d="m19 5 3-3" />
          <path d="m2 22 3-3" />
          <path d="M6.3 20.3a2.4 2.4 0 0 0 3.4 0L12 18l-6-6-2.3 2.3a2.4 2.4 0 0 0 0 3.4Z" />
          <path d="M7.5 13.5 10 11" />
          <path d="M10.5 16.5 13 14" />
          <path d="m12 6 6 6 2.3-2.3a2.4 2.4 0 0 0 0-3.4l-2.6-2.6a2.4 2.4 0 0 0-3.4 0Z" />
        </svg>
        <span>
          {connectionState.name === 'disconnected' ? 'Offline' : 'Connecting'}
        </span>
      </div>
    </div>
  ) : null;
};

const addStatusParam = (
  projectName: string,
  qs: URLSearchParams,
  status: 'closed' | 'all' | undefined,
) => {
  const newParams = new URLSearchParams(qs);
  if (status === undefined) {
    newParams.delete('status');
  } else {
    newParams.set('status', status);
  }
  if (newParams.size === 0) {
    return links.list({projectName});
  }
  return links.list({projectName}) + '?' + newParams.toString();
};

function getStatus(listContext: ListContext | undefined) {
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
