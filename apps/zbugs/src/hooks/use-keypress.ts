import {useEffect} from 'react';

export function useKeypress(key: string, callback: () => void) {
  useEffect(() => {
    function handleKeypress(event: KeyboardEvent) {
      if (event.key === key) {
        callback();
      }
    }

    window.addEventListener('keypress', handleKeypress);
    return () => {
      window.removeEventListener('keypress', handleKeypress);
    };
  }, [key, callback]);
}
