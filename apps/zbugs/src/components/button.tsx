import {useCallback, type CSSProperties, type ReactNode} from 'react';

interface Props {
  children?: ReactNode | undefined;
  className?: string | undefined;
  disabled?: boolean | undefined;
  onAction?: (() => void) | undefined;
  style?: CSSProperties | undefined;
  title?: string | undefined;
  // add more as needed
}

export function Button(props: Props) {
  const {onAction, ...rest} = props;

  const handleMouseDown = useCallback(
    (e: React.MouseEvent) => {
      onAction?.();
      // Prevent default to avoid the button taking focus on click, which
      // wil steal focus from anything focused in response to onAction.
      e.preventDefault();
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

  return <button {...actionProps} {...rest} />;
}
