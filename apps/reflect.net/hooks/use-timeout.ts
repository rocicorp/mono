import {useCallback, useEffect, useRef} from 'react';

/**
 * A form of timeout function suitable for using in an event handler, like in
 * response to a button click. As compared to `useRenderTimeout` below which
 * is declarative an intended for use at render-time.
 */
export function useEventTimeout() {
  const timerID = useRef<number | null>(null);

  const setTimeout = (callback: () => void, delay: number) => {
    cleanup();
    timerID.current = window.setTimeout(callback, delay);
  };

  const cleanup = () => {
    if (timerID.current) {
      clearTimeout(timerID.current);
      timerID.current = null;
    }
  };

  useEffect(() => {
    return cleanup;
  }, []);

  return [useCallback(setTimeout, []), useCallback(cleanup, [])] as const;
}

/**
 * a form of timeout function suitable for use at render-time, like to animate
 * a component in.
 */
export default function useRenderTimeout(
  callback: () => void,
  delay: number | null,
) {
  const timeoutRef = useRef<number | null>(null);
  const savedCallback = useRef<() => void>(callback);
  useEffect(() => {
    savedCallback.current = callback;
  }, [callback]);
  useEffect(() => {
    const tick = () => savedCallback.current();
    if (typeof delay === 'number') {
      timeoutRef.current = window.setTimeout(tick, delay);
      return () => {
        if (timeoutRef.current !== null) {
          window.clearTimeout(timeoutRef.current);
        }
      };
    }
    return undefined;
  }, [delay]);
  return timeoutRef;
}
