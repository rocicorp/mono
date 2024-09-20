import {ReactNode} from 'react';
import {navigate, useLocationProperty} from 'wouter/use-browser-location';

/**
 * The Link from wouter uses onClick and there's no way to change it.
 * We like mousedown here at Rocicorp.
 */
export function Link({
  children,
  href,
  className,
}: {
  children: ReactNode;
  href: string;
  className?: string | ((active: boolean) => string);
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

  const currentURL = useLocationProperty(() => location.href);
  const cn =
    typeof className === 'function'
      ? className(currentURL === new URL(href, currentURL).toString())
      : className;

  return (
    <a href={href} onMouseDown={onMouseDown} onClick={onClick} className={cn}>
      {children}
    </a>
  );
}
