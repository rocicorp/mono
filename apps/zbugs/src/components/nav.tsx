import {useState} from 'react';
import logoURL from '../assets/images/logo.svg';
import {clearJwt, getJwt} from '../jwt.js';
import {Link} from './link.js';
import classNames from 'classnames';
import {FPSMeter} from '@schickling/fps-meter';
import {useSearch} from 'wouter';

export function Nav() {
  const qs = new URLSearchParams(useSearch());
  const [jwt, setJwt] = useState(() => {
    return getJwt();
  });

  const addOpenParam = (open: boolean | undefined) => {
    const newParams = new URLSearchParams(qs);
    if (open === undefined) {
      newParams.delete('open');
    } else {
      newParams.set('open', open ? 'true' : 'false');
    }
    return '/?' + newParams.toString();
  };

  return (
    <div className="nav-container flex flex-col">
      <Link href="/">
        <img src={logoURL} className="zero-logo" />
      </Link>
      {/* could not figure out how to add this color to tailwind.config.js */}
      <button className="primary-cta">New Issue</button>

      <div className="section-issues">
        <div className="pt-2 flex flex-col gap-2">
          <Link
            href={addOpenParam(undefined)}
            className={classNames('nav-item', {
              'nav-active': !qs.has('open'),
            })}
          >
            All
          </Link>
          <Link
            href={addOpenParam(true)}
            className={classNames('nav-item', {
              'nav-active': qs.get('open') === 'true',
            })}
          >
            Open
          </Link>
          <Link
            href={addOpenParam(false)}
            className={classNames('nav-item', {
              'nav-active': qs.get('open') === 'false',
            })}
          >
            Closed
          </Link>
        </div>
      </div>
      <FPSMeter className="fps-meter" width={192} height={38} />
      <div className="user-login">
        {jwt === undefined ? (
          <a href="/api/login/github">Login</a>
        ) : (
          <span
            className="cursor-pointer"
            onClick={() => {
              clearJwt();
              setJwt(undefined);
            }}
          >
            Logout {(jwt as {name: string}).name}
          </span>
        )}
      </div>
    </div>
  );
}
