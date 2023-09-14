import {useCallback, useRef, useState} from 'react';

type UseLongPressOptions = {
  shouldPreventDefault?: boolean;
  delay?: number;
};

export function useLongPress(
  onLongPress: (event: React.SyntheticEvent) => void,
  onClick: (event: React.SyntheticEvent) => void,
  options?: UseLongPressOptions | undefined,
) {
  const {shouldPreventDefault = true, delay = 8} = options || {};
  const [longPressTriggered, setLongPressTriggered] = useState(false);
  const timeout = useRef<NodeJS.Timeout | null>(null);
  const target = useRef<EventTarget | null>(null);

  const start = useCallback(
    (event: React.SyntheticEvent) => {
      if (shouldPreventDefault && event.target) {
        (event.target as HTMLElement).addEventListener(
          'touchend',
          preventDefault,
          {
            passive: false,
          },
        );
        target.current = event.target;
      }

      const recursiveTimeout = () => {
        setLongPressTriggered(true); // Reset long press state
        onLongPress(event);
        timeout.current = setTimeout(recursiveTimeout, delay);
      };
      timeout.current = setTimeout(recursiveTimeout, delay);
    },
    [onLongPress, delay, shouldPreventDefault],
  );

  const clear = useCallback(
    (event: React.SyntheticEvent, shouldTriggerClick = true) => {
      if (timeout.current) {
        clearTimeout(timeout.current);
      }
      if (shouldTriggerClick && !longPressTriggered) {
        onClick(event);
      }
      setLongPressTriggered(false);
    },
    [onClick, longPressTriggered],
  );

  return {
    onMouseDown: (e: React.MouseEvent) => start(e),
    onTouchStart: (e: React.TouchEvent) => start(e),
    onMouseUp: (e: React.MouseEvent) => clear(e),
    onMouseLeave: (e: React.MouseEvent) => clear(e, false),
    onTouchEnd: (e: React.TouchEvent) => clear(e),
  };
}

const isTouchEvent = (event: Event): event is TouchEvent => 'touches' in event;

const preventDefault = (event: Event) => {
  if (!isTouchEvent(event)) return;
  // only prevent default when touching with one finger
  if (event.touches.length < 2 && event.preventDefault) {
    event.preventDefault();
  }
};
