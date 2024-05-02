import {RefObject, useCallback, useEffect} from 'react';

export const useClickOutside = (
  ref: RefObject<Element>,
  callback: (event: MouseEvent) => void,
  outerRef?: RefObject<Element>,
): void => {
  const handleClick = useCallback(
    (event: MouseEvent) => {
      if (
        outerRef &&
        outerRef.current &&
        !outerRef.current.contains(event.target as Node)
      )
        return;
      if (ref.current && !ref.current.contains(event.target as Node)) {
        callback(event);
      }
    },
    [callback, ref, outerRef],
  );
  useEffect(() => {
    document.addEventListener('mousedown', handleClick);

    return () => {
      document.removeEventListener('mousedown', handleClick);
    };
  });
};
