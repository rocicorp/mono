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
      const box = entries[0]?.borderBoxSize[0];
      if (box) {
        setSize({width: box.inlineSize, height: box.blockSize});
      }
    });

    observer.observe(elm.current, {box: 'border-box'});

    return () => {
      observer.disconnect();
    };
  }, [elm]);

  return size;
}
