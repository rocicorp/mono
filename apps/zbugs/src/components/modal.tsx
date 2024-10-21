import {Transition} from '@headlessui/react';
import classnames from 'classnames';
import React, {
  useCallback,
  useRef,
  type MouseEvent,
  type RefObject,
} from 'react';
import ReactDOM from 'react-dom';
import CloseIcon from '../assets/icons/close.svg?react';
import {useKeypress} from '../hooks/use-keypress.js';
import useLockBodyScroll from '../hooks/use-lock-body-scroll.js';

interface Props {
  title?: string;
  isOpen: boolean;
  center: boolean;
  className?: string;
  onDismiss: () => void;
  children?: React.ReactNode;
  size: keyof typeof sizeClasses;
  isDirty?: (() => boolean) | undefined;
}
const sizeClasses = {
  large: 'max-w-2xl w-1/2',
  normal: 'max-w-md w-1/3',
};

export default function Modal({
  title,
  isOpen,
  center,
  size,
  className,
  onDismiss,
  children,
  isDirty,
}: Props) {
  const ref = useRef<HTMLDivElement>(null) as RefObject<HTMLDivElement>;
  const outerRef = useRef(null);

  const wrapperClasses = classnames(
    'fixed flex flex-col items-center inset-0 z-50 modal-background',
    {
      'justify-center': center,
    },
  );
  const modalClasses = classnames(
    'flex flex-col items-center overflow-hidden transform modal shadow-large-modal rounded-lg',
    {
      'mt-20 mb-2 ': !center,
    },
    sizeClasses[size],
    className,
  );

  const close = useCallback(() => {
    if (
      isDirty?.() &&
      !confirm('You have unsaved changes. Are you sure you want to close?')
    ) {
      return;
    }
    onDismiss();
  }, [isDirty, onDismiss]);

  const handleMouseDown = useCallback(
    (event: MouseEvent) => {
      if (ref.current && !ref.current.contains(event.target as Element)) {
        event.stopPropagation();
        event.preventDefault();
        close();
      }
    },
    [close],
  );

  useKeypress('Escape', () => onDismiss?.(), 'keydown');

  useLockBodyScroll();

  const modal = (
    <div ref={outerRef} onMouseDown={handleMouseDown}>
      <Transition show={isOpen}>
        <div className={wrapperClasses}>
          <div ref={ref} className={modalClasses}>
            {title && (
              <div className="flex items-center justify-between w-full pl-4">
                <div className="text-sm font-semibold text-white">{title}</div>
                <div className="p-4" onMouseDown={close}>
                  <CloseIcon className="w-4 text-gray-500 hover:text-gray-700" />
                </div>
              </div>
            )}
            {children}
          </div>
        </div>
      </Transition>
    </div>
  );

  return ReactDOM.createPortal(
    modal,
    document.getElementById('root-modal') as Element,
  );
}

Modal.defaultProps = {
  size: 'normal',
  center: true,
};
