import classNames from 'classnames';
import {useCallback, useEffect, useRef, useState} from 'react';

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
  disabled,
}: {
  items?: Item<T>[] | undefined;
  selectedValue?: T | undefined;
  onChange?: ((selectedValue: T) => void) | undefined;
  defaultItem?: Omit<Item<T>, 'value'> | undefined;
  disabled?: boolean | undefined;
}) {
  const [isOpen, setIsOpen] = useState(false);

  // We keep track of the time of the last open event to prevent the dropdown
  // from closing immediately after clicking on the selected option.
  const openTimeRef = useRef(0);

  const setMenuOpen = (b: boolean) => {
    if (b) {
      openTimeRef.current = Date.now();
    }
    setIsOpen(b);
  };

  const toggleDropdown = () => {
    setMenuOpen(!isOpen);
  };

  useEffect(() => {
    if (isOpen) {
      const handleMouseDown = (e: MouseEvent) => {
        const target = e.target as HTMLElement;
        if (!dropdownRef.current?.contains(target)) {
          setIsOpen(false);
        }
      };

      document.addEventListener('mousedown', handleMouseDown, {capture: true});
      return () => {
        document.removeEventListener('mousedown', handleMouseDown, {
          capture: true,
        });
      };
    }
    return undefined;
  }, [isOpen]);

  const handleSelect = useCallback(
    (value: T) => {
      onChange?.(value);
      setIsOpen(false);
    },
    [onChange],
  );

  const onMouseUp = useCallback(
    (value: T) => {
      // if we press down and up on the selectedoption we want to trigger the action
      // after a short pause. This is to prevent the dropdown from closing immediately
      // after clicking on the selected option.
      const now = Date.now();
      if (!(now - openTimeRef.current < 500)) {
        handleSelect(value);
      }
    },
    [openTimeRef, handleSelect],
  );

  const selected = items?.find(item => item.value === selectedValue);
  const closedItem = selected ?? defaultItem;
  const openItems = (selected ? [selected] : []).concat(
    items?.filter(item => item !== selected) ?? [],
  );

  const [selectedIndex, setSelectedIndex] = useState(0);

  const dropdownRef = useRef<HTMLDivElement>(null);

  const selectItem = (index: number) => {
    setSelectedIndex(index);
    dropdownRef.current?.children[index]?.scrollIntoView({
      behavior: 'smooth',
      block: 'nearest',
    });
  };

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      switch (e.key) {
        case 'Escape':
          setIsOpen(false);
          break;
        case ' ':
          if (!isOpen) {
            setIsOpen(true);
          } else {
            handleSelect(openItems[selectedIndex]?.value);
          }
          break;
        case 'Enter':
          if (isOpen) {
            handleSelect(openItems[selectedIndex]?.value);
          }
          break;
        case 'ArrowUp':
          if (!isOpen) {
            setIsOpen(true);
          } else {
            selectItem(Math.max(selectedIndex - 1, 0));
          }
          break;
        case 'ArrowDown':
          if (!isOpen) {
            setIsOpen(true);
          } else {
            selectItem(Math.min(selectedIndex + 1, openItems.length - 1));
          }
          break;
      }
    },
    [isOpen, handleSelect, openItems, selectedIndex],
  );

  return (
    <div className="selector">
      <button
        disabled={disabled}
        onMouseDown={toggleDropdown}
        onKeyDown={handleKeyDown}
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
      </button>
      {isOpen && (
        <div className="dropdown" ref={dropdownRef}>
          {openItems.map((item, index) => (
            <div
              onMouseEnter={() => setSelectedIndex(index)}
              onMouseLeave={() => setSelectedIndex(-1)}
              key={index}
              className={classNames('item', {
                selected: index === selectedIndex,
              })}
              onMouseDown={() => handleSelect(item.value)}
              onMouseUp={() => onMouseUp(item.value)}
            >
              <img src={item.icon} className="item-avatar" alt={item.text} />
              {item.text}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default Selector;
