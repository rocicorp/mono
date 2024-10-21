import {FPSMeter} from '@schickling/fps-meter';
import classNames from 'classnames';
import {useState} from 'react';
import {useSearch} from 'wouter';
import {navigate} from 'wouter/use-browser-location';
import {useQuery} from 'zero-react/src/use-query.js';
import logoURL from '../assets/images/logo.svg';
import markURL from '../assets/images/mark.svg';
import {useLogin} from '../hooks/use-login.js';
import {useZero} from '../hooks/use-zero.js';
import IssueComposer from '../pages/issue/issue-composer.js';
import {links} from '../routes.js';
import {Link} from './link.js';
import {NotLoggedInModal} from './not-logged-in-modal.js';

export function Nav() {
  const qs = new URLSearchParams(useSearch());
  const login = useLogin();

  const zero = useZero();
  const user = useQuery(
    zero.query.user.where('id', login.loginState?.decoded.sub ?? '').one(),
  );

  const [showIssueModal, setShowIssueModal] = useState(false);
  const [showNotLoggedInModal, setShowNotLoggedInModal] = useState(false);

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
    if (login.loginState === undefined) {
      setShowNotLoggedInModal(true);
    } else {
      setShowIssueModal(true);
    }
  };

  return (
    <>
      <div className="nav-container flex flex-col">
        <Link href="/">
          <img src={logoURL} className="zero-logo" />
          <img src={markURL} className="zero-mark" />
        </Link>
        {/* could not figure out how to add this color to tailwind.config.js */}
        <button className="primary-cta" onMouseDown={newIssue}>
          <span className="primary-cta-text">New Issue</span>
        </button>

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
        <div className="spacer"></div>
        {import.meta.env.DEV && (
          <FPSMeter className="fps-meter" width={192} height={38} />
        )}
        <div className="user-login">
          {login.loginState === undefined ? (
            <a href={loginHref}>Login</a>
          ) : (
            <div className="logged-in-user-container">
              <div className="logged-in-user">
                <img
                  src={user?.avatar}
                  className="issue-creator-avatar"
                  alt={user?.name}
                  title={user?.login}
                />
                <span className="logged-in-user-name">
                  {login.loginState?.decoded.name}
                </span>
              </div>
              <button
                className="logout-button"
                onMouseDown={login.logout}
                title="Log out"
              ></button>
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
      <NotLoggedInModal
        isOpen={showNotLoggedInModal}
        onDismiss={() => setShowNotLoggedInModal(false)}
        href={loginHref}
      />
    </>
  );
}
