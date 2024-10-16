import logoURL from '../assets/images/logo.svg';
import markURL from '../assets/images/mark.svg';
import {Link} from './link.js';
import classNames from 'classnames';
import {FPSMeter} from '@schickling/fps-meter';
import {useSearch} from 'wouter';
import {useLogin} from '../hooks/use-login.js';
import IssueComposer from '../pages/issue/issue-composer.js';
import {useState} from 'react';
import {useZero} from '../hooks/use-zero.js';
import {useQuery} from 'zero-react/src/use-query.js';

export function Nav() {
  const qs = new URLSearchParams(useSearch());
  const login = useLogin();

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

  return (
    <>
      <div className="nav-container flex flex-col">
        <Link href="/">
          <img src={logoURL} className="zero-logo" />
          <img src={markURL} className="zero-mark" />
        </Link>
        {/* could not figure out how to add this color to tailwind.config.js */}
        <button
          className="primary-cta"
          onMouseDown={() => setShowIssueModal(true)}
        >
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

        <FPSMeter className="fps-meter" width={192} height={38} />
        <div className="user-login">
          {login.loginState === undefined ? (
            <a href="/api/login/github">Login</a>
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
        onDismiss={() => setShowIssueModal(false)}
      />
    </>
  );
}
