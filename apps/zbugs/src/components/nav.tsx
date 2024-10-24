import {FPSMeter} from '@schickling/fps-meter';
import classNames from 'classnames';
import {useEffect, useState} from 'react';
import {useSearch} from 'wouter';
import {navigate} from 'wouter/use-browser-location';
import {useQuery} from 'zero-react/src/use-query.js';
import logoURL from '../assets/images/logo.svg';
import markURL from '../assets/images/mark.svg';
import {useLogin} from '../hooks/use-login.js';
import {useZero} from '../hooks/use-zero.js';
import IssueComposer from '../pages/issue/issue-composer.js';
import {links} from '../routes.js';
import {ButtonWithLoginCheck} from './button-with-login-check.js';
import {Button} from './button.js';
import {Link} from './link.js';

export function Nav() {
  const qs = new URLSearchParams(useSearch());
  const login = useLogin();
  const [isMobile, setIsMobile] = useState(false);
  const [showUserPanel, setShowUserPanel] = useState(false); // State to control visibility of user-panel-mobile
  const zero = useZero();
  const user = useQuery(
    zero.query.user.where('id', login.loginState?.decoded.sub ?? '').one(),
  );

  const [showIssueModal, setShowIssueModal] = useState(false);

  const addStatusParam = (status: 'closed' | 'all' | undefined) => {
    const newParams = new URLSearchParams(qs);
    if (status === undefined) {
      newParams.delete('status');
    } else {
      newParams.set('status', status);
    }
    return '/?' + newParams.toString();
  };

  const loginHref = links.login(
    window.location.pathname,
    window.location.search,
  );

  const newIssue = () => {
    setShowIssueModal(true);
  };

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

  const handleClick = () => {
    setShowUserPanel(!showUserPanel); // Toggle the user panel visibility
  };

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
          onAction={newIssue}
          loginMessage="You need to be logged in to create a new issue."
        >
          <span className="primary-cta-text">New Issue</span>
        </ButtonWithLoginCheck>

        <div className="section-tabs">
          <Link
            href={addStatusParam(undefined)}
            className={classNames('nav-item', {
              'nav-active': !qs.has('status'),
            })}
          >
            Open
          </Link>
          <Link
            href={addStatusParam('closed')}
            className={classNames('nav-item', {
              'nav-active': qs.get('status') === 'closed',
            })}
          >
            Closed
          </Link>
          <Link
            href={addStatusParam('all')}
            className={classNames('nav-item', {
              'nav-active': qs.get('status') === 'all',
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
            <div className="logged-in-user-container">
              <div className="logged-in-user">
                {isMobile ? (
                  <div className="mobile-login-container">
                    <Button onAction={handleClick}>
                      <img
                        src={user?.avatar}
                        className="issue-creator-avatar"
                        alt={user?.name}
                        title={user?.login}
                      />
                    </Button>
                    <div
                      className={classNames('user-panel-mobile', {
                        hidden: !showUserPanel, // Conditionally hide/show the panel
                      })}
                    >
                      <Button
                        className="logout-button-mobile"
                        onAction={login.logout}
                        title="Log out"
                      >
                        Log out
                      </Button>
                    </div>
                  </div>
                ) : (
                  <img
                    src={user?.avatar}
                    className="issue-creator-avatar"
                    alt={user?.name}
                    title={user?.login}
                  />
                )}
                <span className="logged-in-user-name">
                  {login.loginState?.decoded.name}
                </span>
              </div>
              <Button
                className="logout-button"
                onAction={login.logout}
                title="Log out"
              ></Button>
            </div>
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
}
