import React, { memo, MouseEvent, RefObject, useRef, useState } from "react";
import { usePopper } from "react-popper";
import PriorityIcon from "./priority-icon";
import HighPriorityIcon from "./assets/icons/signal-strong.svg";
import LowPriorityIcon from "./assets/icons/signal-weak.svg";
import MediumPriorityIcon from "./assets/icons/signal-medium.svg";
import NoPriorityIcon from "./assets/icons/dots.svg";
import UrgentPriorityIcon from "./assets/icons/rounded-claim.svg";
import { Priority, PriorityEnum } from "./issue";
import { useClickOutside } from "./hooks/useClickOutside";

interface Props {
  labelVisible?: boolean;
  onSelect: (priority: Priority) => void;
  priority: Priority;
}

export const statusOpts = [
  [NoPriorityIcon, "No priority", Priority.NONE],
  [UrgentPriorityIcon, "Urgent", Priority.URGENT],
  [HighPriorityIcon, "High", Priority.HIGH],
  [MediumPriorityIcon, "Medium", Priority.MEDIUM],
  [LowPriorityIcon, "Low", Priority.LOW],
];

const PriorityMenu = ({
  labelVisible,
  onSelect,
  priority = Priority.NONE,
}: Props) => {
  const [buttonRef, setButtonRef] = useState<HTMLButtonElement | null>(null);
  const [priorityDropDownVisible, setPriorityDropDownVisible] = useState(false);
  const ref = useRef<HTMLDivElement>() as RefObject<HTMLDivElement>;

  const handleDropdownClick = (e: MouseEvent) => {
    e.stopPropagation();
    setPriorityDropDownVisible(!priorityDropDownVisible);
  };

  const getPriorityString = (priority: PriorityEnum) => {
    switch (priority) {
      case Priority.NONE:
        return "None";
      case Priority.HIGH:
        return "High";
      case Priority.MEDIUM:
        return "Medium";
      case Priority.LOW:
        return "Low";
      case Priority.URGENT:
        return "Urgent";
      default:
        return "Priority";
    }
  };

  useClickOutside(ref, () => {
    if (priorityDropDownVisible) {
      setPriorityDropDownVisible(false);
    }
  });

  const options = statusOpts.map(([Icon, label, priority], idx) => {
    return (
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
    );
  });

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
};

const Popper = ({
  buttonRef,
  children,
}: {
  buttonRef: HTMLButtonElement | null;
  children: React.ReactNode;
}) => {
  const [popperRef, setPopperRef] = useState<HTMLDivElement | null>(null);

  const { styles, attributes } = usePopper(buttonRef, popperRef, {
    placement: "bottom-start",
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
};

export default memo(PriorityMenu);
