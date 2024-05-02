import React, {RefObject, useRef, useState} from 'react';
import {usePopper} from 'react-popper';
import {Order} from './issue';
import {useClickOutside} from './hooks/useClickOutside';
import SortOutlinedIcon from '@mui/icons-material/SortOutlined';
import classNames from 'classnames';

interface Props {
  onSelect: (orderBy: Order) => void;
  order: Order;
}

function SortOrderMenu({onSelect, order}: Props) {
  const [orderRef, setOrderRef] = useState<HTMLButtonElement | null>(null);
  const [popperRef, setPopperRef] = useState<HTMLDivElement | null>(null);
  const [orderByDropDownVisible, setOrderByDropDownVisible] = useState(false);

  const {styles, attributes, update} = usePopper(orderRef, popperRef, {
    placement: 'bottom-end',
  });

  const ref = useRef<HTMLDivElement>() as RefObject<HTMLDivElement>;

  const handleDropdownClick = async () => {
    update && (await update());
    setOrderByDropDownVisible(!orderByDropDownVisible);
  };

  useClickOutside(ref, () => {
    if (orderByDropDownVisible) {
      setOrderByDropDownVisible(false);
    }
  });

  const orderedBys: Array<[Order, string]> = [
    [Order.Created, 'Created Date'],
    [Order.Modified, 'Last Modified'],
    [Order.Status, 'Status'],
    [Order.Priority, 'Priority'],
  ];

  const displayOrder = new Map(orderedBys);

  const options = orderedBys.map(([orderOption, label], idx) => (
    <div
      key={idx}
      className={classNames(
        'flex items-center h-8 px-3 text-gray focus:outline-none hover:text-gray-800 hover:bg-gray-300',
        // eslint-disable-next-line @typescript-eslint/naming-convention
        {'font-semibold text-black': order === orderOption},
      )}
      onMouseDown={() => {
        onSelect(orderOption as Order);
        setOrderByDropDownVisible(false);
      }}
    >
      {label}
    </div>
  ));

  return (
    <div className="flex flex-row items-center" ref={ref}>
      <div className="text-white hover:text-gray-50 p-1">
        {displayOrder.get(order)}
      </div>
      <button
        className="items-center justify-center w-6 h-6 border-none rounded focus:outline-none hover:bg-gray-300"
        ref={setOrderRef}
        onMouseDown={handleDropdownClick}
      >
        <SortOutlinedIcon />
      </button>
      <div
        ref={setPopperRef}
        style={{
          ...styles.popper,
          display: orderByDropDownVisible ? '' : 'none',
        }}
        {...attributes.popper}
        className="cursor-default bg-white rounded shadow-modal z-100 w-34"
      >
        <div style={styles.offset}>{options}</div>
      </div>
    </div>
  );
}

export default SortOrderMenu;
