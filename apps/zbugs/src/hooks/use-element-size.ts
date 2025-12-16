import {useLayoutEffect, useState} from 'react';

export function useElementSize(elm: React.RefObject<HTMLElement | null>) {
  const [size, setSize] = useState<{width: number; height: number} | null>(
    null,
  );

  useLayoutEffect(() => {
    if (!elm.current) {
      return;
    }

    const observer = new ResizeObserver(entries => {
      const entry = entries[0];
      if (entry) {
        setSize({
          width: entry.contentRect.width,
          height: entry.contentRect.height,
        });
      }
    });

    observer.observe(elm.current);

    return () => {
      observer.disconnect();
    };
  }, [elm]);

  return size;
}
