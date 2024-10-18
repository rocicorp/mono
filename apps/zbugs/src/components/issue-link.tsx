import {links} from '../routes.js';
import {Link, type Props as LinkProps} from './link.js';

export default function IssueLink({
  issue,
  title,
  children,
  className,
}: {
  issue: {id: string; shortID?: number | undefined};
} & Omit<LinkProps, 'href'>) {
  return (
    <Link href={links.issue(issue)} title={title} className={className}>
      {children}
    </Link>
  );
}
