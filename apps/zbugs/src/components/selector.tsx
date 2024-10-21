import classNames from 'classnames';
import {useEffect, useState} from 'react';
import {Button} from './button.js';

type Item<T> = {
  text: string;
  value: T;
  icon?: string | undefined;
};

export function Selector<T>({
  items,
  selectedValue,
  onChange,
  defaultItem,
}: {
  items?: Item<T>[] | undefined;
  selectedValue?: T | undefined;
  onChange?: ((selectedValue: T) => void) | undefined;
  defaultItem?: Omit<Item<T>, 'value'> | undefined;
}) {
  const [isOpen, setIsOpen] = useState(false);

  const toggleDropdown = () => {
    setIsOpen(!isOpen);
  };

  useEffect(() => {
    if (isOpen) {
      const handleMouseDown = (e: MouseEvent) => {
        const target = e.target as HTMLElement;
        if (!target.closest('.selector')) {
          setIsOpen(false);
        }
      };

      document.addEventListener('mousedown', handleMouseDown);
      return () => {
        document.removeEventListener('mousedown', handleMouseDown);
      };
    }
    return undefined;
  }, [isOpen]);

  const handleSelect = (value: T) => {
    onChange?.(value);
    setIsOpen(false);
  };

  const selected = items?.find(item => item.value === selectedValue);
  const closedItem = selected ?? defaultItem;
  const openItems = (selected ? [selected] : []).concat(
    items?.filter(item => item !== selected) ?? [],
  );

  return (
    <div className="selector">
      <Button
        onAction={toggleDropdown}
        className={classNames('sidebar-button', 'button-dropdown', 'item')}
      >
        {closedItem ? (
          <>
            <img
              src={closedItem.icon}
              className="item-avatar"
              alt={closedItem.text}
            />
            {closedItem.text}
          </>
        ) : (
          ''
        )}
      </Button>
      {isOpen && (
        <div className="dropdown">
          {openItems.map((item, index) => (
            <Button
              key={index}
              className="item"
              onAction={() => handleSelect(item.value)}
            >
              <img src={item.icon} className="item-avatar" alt={item.text} />
              {item.text}
            </Button>
          ))}
        </div>
      )}
    </div>
  );
}

export default Selector;
