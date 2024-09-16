import logoURL from '../assets/images/logo.svg';
import {Link} from './link.js';
import classNames from 'classnames';

export function Nav() {
  return (
    <div className="flex flex-col gap-6">
      <img src={logoURL} style={{marginRight: '1px'}} />
      {/* could not figure out how to add this color to tailwind.config.js */}
      <button className="bg-[#FF5C00]">New Issue</button>

      <div>
        <div className="font-bold">Issues</div>
        <div className="pt-2 flex flex-col gap-2">
          <Link
            href="/?open=true"
            className={active =>
              classNames('text-white', 'px-2', 'py-1', 'rounded', {
                'bg-gray-900': active,
              })
            }
          >
            Open
          </Link>
          <Link
            href="/?open=false"
            className={active =>
              classNames('text-white', 'px-2', 'py-1', 'rounded', {
                'bg-gray-900': active,
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
