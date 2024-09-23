import {useLayoutEffect, useState} from 'react';

export function useElementSize(elm: HTMLElement | null) {
  const [size, setSize] = useState<{width: number; height: number} | null>(
    null,
  );

  useLayoutEffect(() => {
    if (!elm) {
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

    observer.observe(elm);

    return () => {
      observer.disconnect();
    };
  }, [elm]);

  return size;
}
