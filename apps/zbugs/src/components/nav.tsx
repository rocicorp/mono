import {useState} from 'react';
import logoURL from '../assets/images/logo.svg';
import {clearJwt, getJwt} from '../jwt.js';
import {Link} from './link.js';
import classNames from 'classnames';
import {FPSMeter} from '@schickling/fps-meter';

export function Nav() {
  const [jwt, setJwt] = useState(() => {
    return getJwt();
  });
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
            href="/"
            className={active =>
              classNames('nav-item', {
                'nav-active': active,
              })
            }
          >
            All
          </Link>
          <Link
            href="/?open=true"
            className={active =>
              classNames('nav-item', {
                'nav-active': active,
              })
            }
          >
            Open
          </Link>
          <Link
            href="/?open=false"
            className={active =>
              classNames('nav-item', {
                'nav-active': active,
              })
            }
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
