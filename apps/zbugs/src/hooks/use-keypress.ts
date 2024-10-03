import {useEffect} from 'react';

export function useKeypress(
  key: string,
  callback: () => void,
  event: 'keypress' | 'keyup' | 'keydown' = 'keypress',
) {
  useEffect(() => {
    function handleKeypress(event: KeyboardEvent) {
      if (event.key === key) {
        callback();
      }
    }

    window.addEventListener(event, handleKeypress);
    return () => {
      window.removeEventListener(event, handleKeypress);
    };
  }, [key, callback]);
}
