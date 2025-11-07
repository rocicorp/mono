import {useEffect, useState} from 'react';

export function useIsScrolling() {
  const [isScrolling, setIsScrolling] = useState(false);

  useEffect(() => {
    let timeout: ReturnType<typeof setTimeout> | null = null;

    const onScroll = () => {
      setIsScrolling(true);

      if (timeout) {
        clearTimeout(timeout);
      }

      timeout = setTimeout(() => {
        setIsScrolling(false);
      }, 1000);
    };

    window.addEventListener('scroll', onScroll);

    return () => {
      if (timeout) {
        clearTimeout(timeout);
      }
      window.removeEventListener('scroll', onScroll);
    };
  }, []);

  return isScrolling;
}
