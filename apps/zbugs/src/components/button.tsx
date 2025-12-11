import {
  forwardRef,
  memo,
  type CSSProperties,
  type ForwardedRef,
  type ReactNode,
} from 'react';
import {useIsOffline} from '../hooks/use-is-offline.ts';
import {isPrimaryMouseButton} from '../is-primary-mouse-button.ts';
import {umami} from '../umami.ts';

export interface ButtonProps {
  onAction?: (() => void) | undefined;
  eventName?: string | undefined;
  children?: ReactNode | undefined;
  className?: string | undefined;
  disabled?: boolean | undefined;
  enabledOffline?: boolean | undefined;
  style?: CSSProperties | undefined;
  title?: string | undefined;
  autoFocus?: boolean | undefined;
  tabIndex?: number | undefined;
}

export const Button = memo(
  forwardRef((props: ButtonProps, ref: ForwardedRef<HTMLButtonElement>) => {
    const {onAction, eventName, children, disabled, enabledOffline, ...rest} =
      props;

    const isOffline = useIsOffline();

    const handleMouseDown = (e: React.MouseEvent) => {
      if (isPrimaryMouseButton(e.nativeEvent)) {
        onAction?.();
        if (eventName) {
          umami.track(eventName);
        }

        // TODO: This is really not the right thing to do. We should only use
        // preventDefault in the callers if they move focus.... However, this is
        // because we are using onmousedown which is non-standard and it is easy
        // to forget to deal with the focus case

        // Prevent default to avoid the button taking focus on click, which
        // will steal focus from anything focused in response to onAction.
        e.preventDefault();
      }
    };

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

    const isDisabled = enabledOffline ? disabled : isOffline || disabled;

    return (
      <button
        {...actionProps}
        {...rest}
        ref={ref}
        aria-disabled={isDisabled}
        disabled={isDisabled}
      >
        {children}
      </button>
    );
  }),
);
