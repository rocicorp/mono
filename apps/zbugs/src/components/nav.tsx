import {useState} from 'react';
import logoURL from '../assets/images/logo.svg';
import {clearJwt, getJwt} from '../jwt.js';
import {Link} from './link.js';
import classNames from 'classnames';

export function Nav() {
  const [jwt, setJwt] = useState(() => {
    return getJwt();
  });
  return (
    <div className="flex flex-col gap-8">
      <Link href="/">
        <img src={logoURL} style={{marginRight: '1px'}} />
      </Link>
      {/* could not figure out how to add this color to tailwind.config.js */}
      <button className="primary-cta">New Issue</button>

      <div className="section-issues">
        <div className="font-bold">Issues</div>
        <div className="pt-2 flex flex-col gap-2">
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
      <div className="pt-2 flex flex-col gap-2">
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
            Logout {(jwt as any).name}
          </span>
        )}
      </div>
    </div>
  );
}
