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
    <Link
      href={`/issue/${issue.shortID ?? issue.id}`}
      title={title}
      className={className}
    >
      {children}
    </Link>
  );
}
