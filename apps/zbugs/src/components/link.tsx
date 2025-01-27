import {memo, type ReactNode} from 'react';
import type {ZbugsHistoryState} from '../routes.ts';

export type Props = {
  children: ReactNode;
  href: string;
  className?: string | undefined;
  title?: string | undefined;
  state?: ZbugsHistoryState | undefined;
  eventName?: string | undefined;
};

/**
 * The Link from wouter uses onClick and there's no way to change it.
 * We like mousedown here at Rocicorp.
 */
export const Link = memo(
  ({children, href, className, title, state, eventName}: Props) => {
    return (
      <a
        href={href}
        title={title}
        data-zbugs-history-state={JSON.stringify(state)}
        data-zbugs-event-name={eventName}
        className={className}
      >
        {children}
      </a>
    );
  },
);
