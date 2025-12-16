import {type RefObject, useCallback, useEffect} from 'react';

export const useClickOutside = (
  ref: RefObject<Node | null> | RefObject<Node | null>[],
  callback: (event: MouseEvent | TouchEvent) => void,
): void => {
  const handleClick = useCallback(
    (event: MouseEvent | TouchEvent) => {
      const target = event.target as Node | null;
      if (!target || !target.isConnected) {
        // target got removed from document?
        return;
      }
      const isOutside = Array.isArray(ref)
        ? ref
            .map(r => r.current)
            .filter(c => c !== null)
            .every(c => !c.contains(target))
        : !ref.current?.contains(target);

      if (isOutside) {
        callback(event);
      }
    },
    [callback, ref],
  );
  useEffect(() => {
    document.addEventListener('mousedown', handleClick);
    document.addEventListener('touchstart', handleClick);

    return () => {
      document.removeEventListener('mousedown', handleClick);
      document.removeEventListener('touchstart', handleClick);
    };
  }, [handleClick]);
};
