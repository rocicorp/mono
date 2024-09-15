import {navigate} from 'wouter/use-browser-location';

/**
 * The Link from wouter uses onClick and there's no way to change it.
 * @param param0
 * @returns
 */
export function Link({
  children,
  href,
  className,
}: {
  children: string;
  href: string;
  className?: string;
}) {
  const isPrimary = (e: React.MouseEvent) => {
    if (e.ctrlKey || e.metaKey || e.altKey || e.shiftKey || e.button !== 0) {
      return false;
    }
    return true;
  };
  const onMouseDown = (e: React.MouseEvent) => {
    if (isPrimary(e)) {
      navigate(href);
    }
  };
  const onClick = (e: React.MouseEvent) => {
    if (isPrimary(e) && !e.defaultPrevented) {
      e.preventDefault();
    }
  };

  return (
    <a
      href={href}
      onMouseDown={onMouseDown}
      onClick={onClick}
      className={className}
    >
      {children}
    </a>
  );
}
