import {useState} from 'react';
import useIsomorphicLayoutEffect from './use-isomorphic-layout-effect';

export function useDocumentSize() {
  const [docSize, setDocSize] = useState<{
    width: number;
    height: number;
  } | null>(null);
  useIsomorphicLayoutEffect(() => {
    setDocSize(getDocSize());

    const handleWindowResize = () => {
      setDocSize(getDocSize());
    };
    window.addEventListener('resize', handleWindowResize);

    return () => {
      window.removeEventListener('resize', handleWindowResize);
    };
  }, []);
  return docSize;
}

function getDocSize() {
  return {
    width: document.body.scrollWidth,
    height: document.body.scrollHeight,
  };
}
