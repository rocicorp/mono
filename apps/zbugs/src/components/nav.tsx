import logoURL from '../assets/images/logo.svg';
import {Link} from './link.js';
import classNames from 'classnames';

export function Nav() {
  return (
    <div className="flex flex-col gap-8">
      <img src={logoURL} style={{marginRight: '1px'}} />
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
    </div>
  );
}
