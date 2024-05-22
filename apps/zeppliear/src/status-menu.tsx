import React, {memo, MouseEvent, RefObject, useRef, useState} from 'react';
import {usePopper} from 'react-popper';
import CancelIcon from './assets/icons/cancel.svg?react';
import BacklogIcon from './assets/icons/circle-dot.svg?react';
import TodoIcon from './assets/icons/circle.svg?react';
import DoneIcon from './assets/icons/done.svg?react';
import InProgressIcon from './assets/icons/half-circle.svg?react';
import {useClickOutside} from './hooks/use-click-outside.js';
import {Status} from './issue.js';
import StatusIcon from './status-icon.jsx';

interface Props {
  labelVisible?: boolean;
  onSelect: (status: Status) => void;
  status: Status;
}

export const statuses = [
  [BacklogIcon, Status.Backlog, 'Backlog'],
  [TodoIcon, Status.Todo, 'Todo'],
  [InProgressIcon, Status.InProgress, 'In Progress'],
  [DoneIcon, Status.Done, 'Done'],
  [CancelIcon, Status.Canceled, 'Canceled'],
] as const;

const getStatusString = (status: Status) => {
  switch (status) {
    case Status.Backlog:
      return 'Backlog';
    case Status.Todo:
      return 'Todo';
    case Status.InProgress:
      return 'In Progress';
    case Status.Done:
      return 'Done';
    case Status.Canceled:
      return 'Canceled';
    default:
      return 'Backlog';
  }
};

function StatusMenu({labelVisible = false, onSelect, status}: Props) {
  const [buttonRef, setButtonRef] = useState<HTMLButtonElement | null>(null);
  const [statusDropDownVisible, setStatusDropDownVisible] = useState(false);

  const ref = useRef<HTMLDivElement>() as RefObject<HTMLDivElement>;

  const handleDropdownClick = (e: MouseEvent) => {
    e.stopPropagation();
    setStatusDropDownVisible(!statusDropDownVisible);
  };

  useClickOutside(ref, () => {
    if (statusDropDownVisible) {
      setStatusDropDownVisible(false);
    }
  });

  const options = statuses.map(
    (
      [
        // eslint-disable-next-line @typescript-eslint/naming-convention
        Icon,
        status,
        label,
      ],
      idx,
    ) => (
      <div
        key={idx}
        className="flex items-center h-8 px-3 text-gray focus:outline-none hover:text-gray-800 hover:bg-gray-300"
        onClick={(e: MouseEvent) => {
          onSelect(status);
          setStatusDropDownVisible(false);
          e.stopPropagation();
        }}
      >
        <Icon className="mr-3" />
        <span>{label}</span>
      </div>
    ),
  );

  return (
    <div ref={ref}>
      <button
        className="inline-flex items-center h-6 px-2 border-none rounded focus:outline-none hover:bg-gray-850"
        ref={setButtonRef}
        onClick={handleDropdownClick}
      >
        <StatusIcon status={status} />
        {labelVisible && (
          <div className="ml-2 whitespace-nowrap">
            {getStatusString(status)}
          </div>
        )}
      </button>
      {statusDropDownVisible && (
        <Popper buttonRef={buttonRef}>{options}</Popper>
      )}
    </div>
  );
}

function Popper({
  buttonRef,
  children,
}: {
  buttonRef: HTMLButtonElement | null;
  children: React.ReactNode;
}) {
  const [popperRef, setPopperRef] = useState<HTMLDivElement | null>(null);

  const {styles, attributes} = usePopper(buttonRef, popperRef, {
    placement: 'bottom-start',
  });

  return (
    <div
      ref={setPopperRef}
      style={{
        ...styles.popper,
      }}
      {...attributes.popper}
      className="cursor-default bg-white rounded shadow-modal z-100 w-34"
    >
      <div style={styles.offset}>{children}</div>
    </div>
  );
}

export default memo(StatusMenu);
