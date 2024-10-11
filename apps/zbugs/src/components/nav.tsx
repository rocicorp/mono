import logoURL from '../assets/images/logo.svg';
import {Link} from './link.js';
import classNames from 'classnames';
import {FPSMeter} from '@schickling/fps-meter';
import {useSearch} from 'wouter';
import {useLogin} from '../hooks/use-login.js';
import IssueComposer from '../pages/issue/issue-composer.js';
import {useState} from 'react';

export function Nav() {
  const qs = new URLSearchParams(useSearch());
  const login = useLogin();

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
        </Link>
        {/* could not figure out how to add this color to tailwind.config.js */}
        <button
          className="primary-cta"
          onMouseDown={() => setShowIssueModal(true)}
        >
          New Issue
        </button>

        <div className="section-issues">
          <div className="pt-2 flex flex-col gap-2">
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
        </div>

        <FPSMeter className="fps-meter" width={192} height={38} />
        <div className="user-login">
          {login.loginState === undefined ? (
            <a href="/api/login/github">Login</a>
          ) : (
            <div className="logged-in-user-container">
              <div className="logged-in-user">
                {/* Need access to user avatar */}
                <span>{login.loginState?.decoded.name}</span>
              </div>
              <button
                className="logout-button"
                onMouseDown={login.logout}
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
