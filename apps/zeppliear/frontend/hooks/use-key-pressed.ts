import {useEffect} from 'react';

export function useKeyPressed(key: string, onKeyPress: () => void) {
  useEffect(() => {
    const downHandler = (ev: KeyboardEvent) => {
      if (ev.key === key) {
        onKeyPress();
      }
    };
    window.addEventListener('keydown', downHandler);

    return () => {
      window.removeEventListener('keydown', downHandler);
    };
  }, [key, onKeyPress]);
}
