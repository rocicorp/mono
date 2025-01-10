import {useEffect} from 'react';
import {navigate} from 'wouter/use-browser-location';
import {isPrimaryMouseButton} from '../is-primary-mouse-button.js';
import {umami} from '../umami.js';

// Implements softnav for all links.
// We do it this way rather than in the Link component so that links inside
// markdown/rehyped content are also picked up automatically.
export function useSoftNav() {
  useEffect(() => {
    const getElm = (e: Event) => {
      const elm = e.target as HTMLAnchorElement | null;
      if (!elm || elm.tagName !== 'A') {
        return null;
      }
      const url = new URL(elm.href, window.location.href);
      if (url.origin !== window.location.origin) {
        return null;
      }
      return elm;
    };

    const onMouseDown = (e: MouseEvent) => {
      const elm = getElm(e);
      if (elm && isPrimaryMouseButton(e)) {
        navigate(elm.href, {state: elm.dataset.zbugsHistoryState});
        if (elm.dataset.zbugsEventName) {
          umami.track(elm.dataset.zbugsEventName);
        }
      }
    };

    const onClick = (e: MouseEvent) => {
      const elm = getElm(e);
      if (elm && isPrimaryMouseButton(e) && !e.defaultPrevented) {
        e.preventDefault();
      }
    };

    const onKeyDown = (e: KeyboardEvent) => {
      // In html links are not activated by space key, but we want to it to be
      // more consistent with buttons, especially since it is hard to determine
      // what is a link vs a button in our UI.
      const elm = getElm(e);
      if (elm && (e.key === 'Enter' || e.key === ' ')) {
        navigate(elm.href, {state: elm.dataset.zbugsHistoryState});
        e.preventDefault();
      }
    };

    window.addEventListener('mousedown', onMouseDown);
    window.addEventListener('click', onClick);
    window.addEventListener('keydown', onKeyDown);
    return () => {
      window.removeEventListener('mousedown', onMouseDown);
      window.removeEventListener('click', onClick);
      window.removeEventListener('keydown', onKeyDown);
    };
  }, []);
}
