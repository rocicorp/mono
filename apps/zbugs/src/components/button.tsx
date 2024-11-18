import {
  forwardRef,
  useCallback,
  type CSSProperties,
  type ForwardedRef,
  type ReactNode,
} from 'react';
import {umami} from '../umami.js';

export interface Props {
  onAction?: (() => void) | undefined;
  eventName?: string | undefined;
  children?: ReactNode | undefined;
  className?: string | undefined;
  disabled?: boolean | undefined;
  style?: CSSProperties | undefined;
  title?: string | undefined;
  autoFocus?: boolean | undefined;
}

export const Button = forwardRef(
  (props: Props, ref: ForwardedRef<HTMLButtonElement>) => {
    const {onAction, eventName, ...rest} = props;

    const handleMouseDown = useCallback(
      (e: React.MouseEvent) => {
        onAction?.();
        if (eventName) {
          umami.track(eventName);
        }
        // Prevent default to avoid the button taking focus on click, which
        // will steal focus from anything focused in response to onAction.
        e.preventDefault();
      },
      [onAction, eventName],
    );

    const actionProps = onAction
      ? {
          onMouseDown: handleMouseDown,
          onKeyUp: (e: React.KeyboardEvent<Element>) => {
            if (e.key === ' ') {
              onAction();
              if (eventName) {
                umami.track(eventName);
              }
            }
          },
          onKeyPress: (e: React.KeyboardEvent<Element>) => {
            if (e.key === 'Enter') {
              onAction();
              if (eventName) {
                umami.track(eventName);
              }
            }
          },
        }
      : {};

    return (
      <button {...actionProps} {...rest} ref={ref}>
        {props.children}
      </button>
    );
  },
);
