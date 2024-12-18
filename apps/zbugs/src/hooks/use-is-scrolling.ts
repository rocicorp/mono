import {useState, useEffect} from 'react';

const useIsScrolling = () => {
  const [isScrolling, setIsScrolling] = useState(false);

  useEffect(() => {
    let timeout: NodeJS.Timeout | null = null;

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
};

export default useIsScrolling;
