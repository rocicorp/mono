import type {CSSProperties, ReactNode} from 'react';

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
  // debugger;
  const actionProps = onAction
    ? {
        onMouseDown: onAction,
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

  return <button {...rest} {...actionProps} />;
}
