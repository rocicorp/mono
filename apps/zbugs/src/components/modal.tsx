import {Transition} from '@headlessui/react';
import classnames from 'classnames';
import React, {
  useCallback,
  useRef,
  useState,
  type MouseEvent,
  type RefObject,
} from 'react';
import ReactDOM from 'react-dom';
import CloseIcon from '../assets/icons/close.svg?react';
import {useKeypress} from '../hooks/use-keypress.js';
import useLockBodyScroll from '../hooks/use-lock-body-scroll.js';
import {Confirm} from './confirm.js';

interface Props {
  title?: string | undefined;
  isOpen: boolean;
  center: boolean;
  className?: string | undefined;
  onDismiss: () => void;
  children?: React.ReactNode;
  size: keyof typeof sizeClasses;
  isDirty?: (() => boolean) | undefined;
}
const sizeClasses = {
  large: 'max-w-2xl w-1/2',
  normal: 'max-w-md w-1/3',
};

export function Modal({
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

  const [confirmDirtyVisible, setConfirmDirtyVisible] = useState(false);

  const close = useCallback(() => {
    if (isDirty?.()) {
      setConfirmDirtyVisible(true);
    } else {
      onDismiss();
    }
  }, [isDirty, onDismiss]);

  const handleMouseDown = useCallback(
    (event: MouseEvent) => {
      // Stop propagation to other modals.
      event.stopPropagation();
      if (ref.current && !ref.current.contains(event.target as Element)) {
        event.preventDefault();
        close();
      }
    },
    [close],
  );

  useKeypress('Escape', close, 'keydown', true);

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
            <div className="flex flex-col w-full py-4 overflow-hidden modal-container">
              {children}
            </div>
          </div>
        </div>
      </Transition>
      {
        // This needs x && y because we cant recursively render the Confirm
        // modal when we're already rendering the Confirm modal.
        confirmDirtyVisible && (
          <Confirm
            text="You have unsaved changes. Are you sure you want to close?"
            isOpen={confirmDirtyVisible}
            okButtonLabel="Close"
            onClose={b => {
              setConfirmDirtyVisible(false);
              if (b) {
                onDismiss();
              }
            }}
          />
        )
      }
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

export function ModalBody({children}: {children: React.ReactNode}) {
  return (
    <div className="flex flex-col flex-1 pb-3.5 overflow-y-auto">
      {children}
    </div>
  );
}

export function ModalText({children}: {children: React.ReactNode}) {
  return (
    <ModalBody>
      <div className="flex items-center w-full mt-1.5 px-4">{children}</div>
    </ModalBody>
  );
}

export function ModalActions({children}: {children: React.ReactNode}) {
  return (
    <div className="flex items-center flex-shrink-0 px-4 pt-3 gap-4">
      {children}
    </div>
  );
}
