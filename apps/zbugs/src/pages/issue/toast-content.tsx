import {memo, type ReactNode, useEffect, useState} from 'react';
import {ToastContainer as Container, toast} from 'react-toastify';
import {useDocumentHasFocus} from '../../hooks/use-document-has-focus.ts';
import {emojiToastShowDuration} from './issue-page.tsx';

export function ToastContent({
  children,
  toastID,
}: {
  children: ReactNode;
  toastID: string;
}) {
  const docFocused = useDocumentHasFocus();
  const [hover, setHover] = useState(false);

  useEffect(() => {
    if (docFocused && !hover) {
      const id = setTimeout(() => {
        toast.dismiss(toastID);
      }, emojiToastShowDuration);
      return () => clearTimeout(id);
    }
    return () => void 0;
  }, [docFocused, hover, toastID]);

  return (
    <div
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
    >
      {children}
    </div>
  );
}

export const ToastContainer = memo(
  ({position}: {position: 'top' | 'bottom'}) => (
    <Container
      hideProgressBar={true}
      theme="dark"
      containerId={position}
      newestOnTop={position === 'bottom'}
      closeButton={false}
      position={`${position}-center`}
      closeOnClick={true}
      limit={3}
      // Auto close is broken. So we will manage it ourselves.
      autoClose={false}
    />
  ),
);
