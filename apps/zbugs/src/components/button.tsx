import {useCallback, type CSSProperties, type ReactNode} from 'react';

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

export function Button(props: Props) {
  const {onAction, eventName, ...rest} = props;

  const handleMouseDown = useCallback(
    (e: React.MouseEvent) => {
      onAction?.();
      e.preventDefault(); // Prevents button from taking focus on click
    },
    [onAction],
  );

  const actionProps = onAction
    ? {
        onMouseDown: handleMouseDown,
        onKeyUp: (e: React.KeyboardEvent<Element>) => {
          if (e.key === ' ') {
            onAction();
          }
        },
        onKeyPress: (e: React.KeyboardEvent<Element>) => {
          if (e.key === 'Enter') {
            onAction();
          }
        },
      }
    : {};

  return (
    <button
      {...actionProps}
      {...rest}
      {...(eventName ? {'data-umami-event': eventName} : {})}
    />
  );
}
