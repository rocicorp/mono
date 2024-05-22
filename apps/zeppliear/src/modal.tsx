import classnames from 'classnames';
import React, {RefObject, useCallback, useRef} from 'react';
import ReactDOM from 'react-dom';
import useLockBodyScroll from './hooks/use-lock-body-scroll.js';

interface Props {
  title?: string;
  isOpen: boolean;
  center: boolean;
  className?: string;
  /* function called when modal is closed */
  onDismiss?: (() => void) | undefined;
  children?: React.ReactNode;
  size: 'small' | 'normal' | 'large' | 'wide';
}
const sizeClasses = {
  wide: 'w-185',
  large: 'w-175',
  normal: 'w-140',
  small: 'w-115',
};

function Modal({isOpen, center, size, className, onDismiss, children}: Props) {
  const ref = useRef<HTMLDivElement>(null) as RefObject<HTMLDivElement>;
  const outerRef = useRef(null);

  const wrapperClasses = classnames(
    'fixed flex flex-col items-center inset-0 z-100',
    {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'justify-center': center,
    },
  );
  const modalClasses = classnames(
    'flex flex-col items-center transform bg-gray-750 modal shadow-large-modal rounded-lg',
    {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'mt-5 lg:mt-20 mb-2 ': !center,
    },
    sizeClasses[size],
    className,
  );
  const handleClick = useCallback(
    (e: React.MouseEvent<HTMLDivElement>) => {
      if (!onDismiss) return;
      if (ref.current && !ref.current.contains(e.target as Node)) {
        onDismiss();
      }
    },
    [onDismiss],
  );

  useLockBodyScroll();

  const modal = (
    <div ref={outerRef} onMouseDown={handleClick}>
      <div className={wrapperClasses} style={{display: isOpen ? '' : 'none'}}>
        <div ref={ref} className={modalClasses}>
          {children}
        </div>
      </div>
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

export default Modal;
