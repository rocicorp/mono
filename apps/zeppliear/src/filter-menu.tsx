import {RefObject, useRef, useState} from 'react';
import {usePopper} from 'react-popper';
import TodoIcon from './assets/icons/circle.svg?react';
import LabelIcon from './assets/icons/label.svg?react';
import SignalStrongIcon from './assets/icons/signal-strong.svg?react';
import {useClickOutside} from './hooks/use-click-outside.js';
import {Filter, Priority, Status} from './issue.js';
import {LabelMenu} from './label-menu.jsx';
import {statusOpts} from './priority-menu.jsx';
import {statuses} from './status-menu.jsx';

interface Props {
  onSelectStatus: (filter: Status) => void;
  onSelectPriority: (filter: Priority) => void;
  onSelectLabel: (filter: string) => void;
}

function FilterMenu({onSelectStatus, onSelectPriority, onSelectLabel}: Props) {
  const [filterRef, setFilterRef] = useState<HTMLButtonElement | null>(null);
  const [popperRef, setPopperRef] = useState<HTMLDivElement | null>(null);
  const [filter, setFilter] = useState<Filter | null>(null);
  const [filterDropDownVisible, setFilterDropDownVisible] = useState(false);

  const {styles, attributes, update} = usePopper(filterRef, popperRef, {
    placement: 'bottom-start',
  });

  const ref = useRef<HTMLDivElement>() as RefObject<HTMLDivElement>;

  const handleDropdownClick = async () => {
    update && (await update());
    setFilter(null);
    setFilterDropDownVisible(!filterDropDownVisible);
  };

  useClickOutside(ref, () => {
    if (filterDropDownVisible) {
      setFilter(null);
      setFilterDropDownVisible(false);
    }
  });

  return (
    <div ref={ref}>
      <button
        className="px-1 py-0.5 ml-3 border border-gray-600 border-dashed rounded text-white hover:text-gray-50 focus:outline-none"
        ref={setFilterRef}
        onMouseDown={handleDropdownClick}
      >
        + Filter
      </button>
      <div
        ref={setPopperRef}
        style={{
          ...styles.popper,
          display: filterDropDownVisible ? '' : 'none',
        }}
        {...attributes.popper}
        className="cursor-default bg-white rounded shadow-modal z-100 w-34"
      >
        <div style={styles.offset}>
          <Options
            filter={filter}
            onSelectPriority={onSelectPriority}
            onSelectStatus={onSelectStatus}
            onSelectLabel={onSelectLabel}
            setFilter={setFilter}
            setFilterDropDownVisible={setFilterDropDownVisible}
          />
        </div>
      </div>
    </div>
  );
}

const filterBys = [
  [SignalStrongIcon, Filter.Priority, 'Priority'],
  [TodoIcon, Filter.Status, 'Status'],
  [LabelIcon, Filter.Label, 'Label'],
] as const;

function Options({
  filter,
  onSelectStatus,
  onSelectPriority,
  onSelectLabel,
  setFilter,
  setFilterDropDownVisible,
}: {
  filter: Filter | null;
  setFilter: (filter: Filter | null) => void;
  setFilterDropDownVisible: (visible: boolean) => void;
} & Props) {
  switch (filter) {
    case Filter.Priority:
      return (
        <>
          {statusOpts.map(
            (
              [
                // eslint-disable-next-line @typescript-eslint/naming-convention
                Icon,
                label,
                priority,
              ],
              idx,
            ) => (
              <div
                key={idx}
                className="flex items-center h-8 px-3 text-gray focus:outline-none hover:text-gray-800 hover:bg-gray-300"
                onMouseDown={() => {
                  onSelectPriority(priority as Priority);
                  setFilter(null);
                  setFilterDropDownVisible(false);
                }}
              >
                <Icon className="mr-4" />
                {label}
              </div>
            ),
          )}
        </>
      );

    case Filter.Status:
      return (
        <>
          {statuses.map(
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
                onMouseDown={() => {
                  onSelectStatus(status as Status);
                  setFilter(null);
                  setFilterDropDownVisible(false);
                }}
              >
                <Icon className="mr-4" />
                {label}
              </div>
            ),
          )}
        </>
      );
    case Filter.Label: {
      return (
        <LabelMenu
          onSelectLabel={label => {
            setFilter(null);
            setFilterDropDownVisible(false);
            onSelectLabel(label);
          }}
        />
      );
    }
    default:
      return (
        <>
          {filterBys.map(
            (
              [
                // eslint-disable-next-line @typescript-eslint/naming-convention
                Icon,
                filter,
                label,
              ],
              idx,
            ) => (
              <div
                key={idx}
                className="flex items-center h-8 px-3 text-gray focus:outline-none hover:text-gray-800 hover:bg-gray-300"
                onMouseDown={e => {
                  e.stopPropagation();
                  e.preventDefault();
                  setFilter(filter as Filter);
                }}
              >
                <Icon className="mr-4" />
                {label}
              </div>
            ),
          )}
        </>
      );
  }
}

export default FilterMenu;
