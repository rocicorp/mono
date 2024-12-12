import {useEffect} from 'react';

export function useKeypress(
  key: string,
  callback: () => void,
  event: 'keypress' | 'keyup' | 'keydown' = 'keypress',
  allowOnInputElements = false,
) {
  useEffect(() => {
    function handleKeypress(e: KeyboardEvent) {
      if (
        e.key === key &&
        (allowOnInputElements || shouldAllow(e.target as HTMLElement))
      ) {
        e.preventDefault();
        callback();
      }
    }

    window.addEventListener(event, handleKeypress);
    return () => {
      window.removeEventListener(event, handleKeypress);
    };
  }, [key, callback, event, allowOnInputElements]);
}

function shouldAllow(el: HTMLElement): boolean {
  const tagName = el.tagName;
  switch (tagName) {
    case 'TEXTAREA':
    case 'SELECT':
    case 'INPUT':
      // To be fair. We could probably allow on type=radio,button and many more. Add more as required.
      return false;
  }
  return !el.isContentEditable;
}
