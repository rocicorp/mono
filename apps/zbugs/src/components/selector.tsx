import {useEffect, useState} from 'react';
import classNames from 'classnames';

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
      <button
        onMouseDown={toggleDropdown}
        className={classNames('sidebar-button', 'button-dropdown', 'item', {
          icon: closedItem?.icon,
        })}
        style={{
          backgroundImage: closedItem?.icon && `url("${closedItem?.icon}")`,
        }}
      >
        {closedItem?.text ?? ''}
      </button>
      {isOpen && (
        <div className="dropdown">
          {openItems.map((item, index) => (
            <button
              key={index}
              className={classNames('item', {
                icon: item.icon,
              })}
              style={{
                backgroundImage: item.icon && `url("${item.icon}")`,
              }}
              onMouseDown={() => handleSelect(item.value)}
            >
              {item.text}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

export default Selector;
