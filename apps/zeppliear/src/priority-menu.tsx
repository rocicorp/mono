import React, {memo, MouseEvent, RefObject, useRef, useState} from 'react';
import {usePopper} from 'react-popper';
import NoPriorityIcon from './assets/icons/dots.svg?react';
import UrgentPriorityIcon from './assets/icons/rounded-claim.svg?react';
import MediumPriorityIcon from './assets/icons/signal-medium.svg?react';
import HighPriorityIcon from './assets/icons/signal-strong.svg?react';
import LowPriorityIcon from './assets/icons/signal-weak.svg?react';
import {useClickOutside} from './hooks/use-click-outside';
import {Priority} from './issue';
import PriorityIcon from './priority-icon';

interface Props {
  labelVisible?: boolean;
  onSelect: (priority: Priority) => void;
  priority: Priority;
}

export const statusOpts = [
  [NoPriorityIcon, 'No priority', Priority.None],
  [UrgentPriorityIcon, 'Urgent', Priority.Urgent],
  [HighPriorityIcon, 'High', Priority.High],
  [MediumPriorityIcon, 'Medium', Priority.Medium],
  [LowPriorityIcon, 'Low', Priority.Low],
] as const;

function PriorityMenu({
  labelVisible,
  onSelect,
  priority = Priority.None,
}: Props) {
  const [buttonRef, setButtonRef] = useState<HTMLButtonElement | null>(null);
  const [priorityDropDownVisible, setPriorityDropDownVisible] = useState(false);
  const ref = useRef<HTMLDivElement>() as RefObject<HTMLDivElement>;

  const handleDropdownClick = (e: MouseEvent) => {
    e.stopPropagation();
    setPriorityDropDownVisible(!priorityDropDownVisible);
  };

  const getPriorityString = (priority: Priority) => {
    switch (priority) {
      case Priority.None:
        return 'None';
      case Priority.High:
        return 'High';
      case Priority.Medium:
        return 'Medium';
      case Priority.Low:
        return 'Low';
      case Priority.Urgent:
        return 'Urgent';
      default:
        return 'Priority';
    }
  };

  useClickOutside(ref, () => {
    if (priorityDropDownVisible) {
      setPriorityDropDownVisible(false);
    }
  });
  const options = statusOpts.map(([Icon, label, priority], idx) => (
    <div
      key={idx}
      className="flex items-center h-8 px-3 text-gray focus:outline-none hover:text-gray-800 hover:bg-gray-300"
      onClick={(e: MouseEvent) => {
        onSelect(priority);
        setPriorityDropDownVisible(false);
        e.stopPropagation();
      }}
    >
      <Icon className="mr-3" />
      <span>{label}</span>
    </div>
  ));

  return (
    <div ref={ref}>
      <button
        className="inline-flex items-center h-6 px-2 border-none rounded focus:outline-none hover:bg-gray-850"
        ref={setButtonRef}
        onClick={handleDropdownClick}
      >
        <PriorityIcon priority={priority} />
        {labelVisible && (
          <div className="ml-2 whitespace-nowrap">
            {getPriorityString(priority)}
          </div>
        )}
      </button>
      {priorityDropDownVisible && (
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

export default memo(PriorityMenu);
