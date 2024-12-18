import {memo, type ReactNode} from 'react';
import {navigate} from 'wouter/use-browser-location';
import {isPrimaryMouseButton} from '../is-primary-mouse-button.js';
import type {ZbugsHistoryState} from '../routes.js';
import {umami} from '../umami.js';

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
    const onMouseDown = (e: React.MouseEvent) => {
      if (isPrimaryMouseButton(e)) {
        navigate(href, {state});
        if (eventName) {
          umami.track(eventName);
        }
      }
    };
    const onClick = (e: React.MouseEvent) => {
      if (isPrimaryMouseButton(e) && !e.defaultPrevented) {
        e.preventDefault();
      }
    };

    const onKeyDown = (e: React.KeyboardEvent) => {
      // In html links are not activated by space key, but we want to it to be
      // more consistent with buttons, especially since it is hard to determine
      // what is a link vs a button in our UI.
      if (e.key === 'Enter' || e.key === ' ') {
        navigate(href, {state});
        e.preventDefault();
      }
    };

    return (
      <a
        href={href}
        title={title}
        onMouseDown={onMouseDown}
        onClick={onClick}
        onKeyDown={onKeyDown}
        className={className}
      >
        {children}
      </a>
    );
  },
);
