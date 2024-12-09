import classNames from 'classnames';
import {
  type KeyboardEvent,
  memo,
  type RefObject,
  useEffect,
  useRef,
  useState,
} from 'react';
import DropdownArrow from '../assets/icons/dropdown-arrow.svg?react';
import {umami} from '../umami.js';
import styles from './combobox.module.css';
import {fuzzySearch} from './fuzzySearch.js';

type Item<T> = {
  text: string;
  value: T;
  icon?: string | undefined;
};

interface Props<T> {
  items?: readonly Item<T>[] | undefined;
  selectedValue?: T | undefined;
  onChange: (selectedValue: T) => void;
  defaultItem?: Omit<Item<T>, 'value'> | undefined;
  disabled?: boolean | undefined;
  className?: string | undefined;
  editable?: boolean | undefined;
}

function Combobox<T>({
  items = [],
  selectedValue: value,
  onChange,
  defaultItem,
  className,
  disabled,
  editable = true,
}: Props<T>) {
  const [isOpen, setIsOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedIndex, setSelectedIndex] = useState(-1);
  const inputRef = useRef<HTMLElement>(null);
  const listboxRef = useRef<HTMLUListElement>(null);
  const openTimeRef = useRef(0);

  const filteredOptions =
    searchQuery === '' ? items : fuzzySearch(searchQuery, items, o => o.text);

  const selectedItem = items.find(option => option.value === value);

  const setMenuOpen = (b: boolean) => {
    if (b) {
      openTimeRef.current = Date.now();
      umami.track('Combobox opened'); // Track open action
    } else {
      umami.track('Combobox closed'); // Track close action
    }
    setIsOpen(b);
  };

  const toggleDropdown = () => {
    console.log('toggleDropdown', isOpen);
    setMenuOpen(!isOpen);
  };

  useEffect(() => {
    if (!isOpen) {
      setSearchQuery('');
      setSelectedIndex(0);
    }
  }, [isOpen]);

  useEffect(() => {
    const handleMouseDown = (event: MouseEvent) => {
      if (
        !inputRef.current?.contains(event.target as Element) &&
        !listboxRef.current?.contains(event.target as Element)
      ) {
        setMenuOpen(false);
      }
    };

    document.addEventListener('mousedown', handleMouseDown);
    return () => document.removeEventListener('mousedown', handleMouseDown);
  }, []);

  const selectIndex = (index: number) => {
    setSelectedIndex(index);
    listboxRef.current?.children[index]?.scrollIntoView({
      behavior: 'smooth',
      block: 'nearest',
    });
  };

  const handleSelect = (value: T) => {
    onChange?.(value);
    setIsOpen(false);
    const selectedText =
      items?.find(item => item.value === value)?.text || 'Unknown';
    umami.track('Selector selection made', {selection: selectedText}); // Track selection with data
  };

  const onMouseUp = (value: T) => {
    const now = Date.now();
    if (!(now - openTimeRef.current < 500)) {
      handleSelect(value);
    }
  };

  const handleKeyDown = (event: KeyboardEvent) => {
    switch (event.key) {
      case 'ArrowDown':
        event.preventDefault();
        if (!isOpen) {
          setMenuOpen(true);
        } else {
          selectIndex(Math.min(selectedIndex + 1, filteredOptions.length - 1));
        }
        break;
      case 'ArrowUp':
        event.preventDefault();
        selectIndex(Math.max(selectedIndex - 1, 0));
        break;
      case 'Enter':
        event.preventDefault();
        if (isOpen && filteredOptions[selectedIndex]) {
          handleSelect(filteredOptions[selectedIndex]?.value);
        }
        break;
      case 'Escape':
        event.preventDefault();
        setMenuOpen(false);
        break;
    }
  };

  const iconItem =
    editable && isOpen ? defaultItem : selectedItem ?? defaultItem;

  return (
    <div
      className={classNames(
        styles.container,
        className,
        disabled ? styles.disabled : undefined,
      )}
    >
      <div className={styles.inputWrapper}>
        {iconItem && (
          <img
            src={iconItem.icon}
            className={classNames(styles.selectedIcon, 'icon')}
          />
        )}
        {editable ? (
          <input
            ref={inputRef as RefObject<HTMLInputElement>}
            disabled={disabled}
            type="text"
            className={classNames(styles.input, {
              [styles.withIcon]: selectedItem && !isOpen,
            })}
            value={isOpen ? searchQuery : selectedItem?.text || ''}
            onChange={e => {
              setSearchQuery(e.target.value);
              setMenuOpen(true);
            }}
            onFocus={() => setMenuOpen(true)}
            onBlur={() => setMenuOpen(false)}
            onMouseDown={() => setMenuOpen(true)}
            onKeyDown={handleKeyDown}
            placeholder={defaultItem?.text}
            role="combobox"
            aria-expanded={isOpen}
            aria-controls="options-listbox"
            aria-activedescendant={
              isOpen
                ? `option-${filteredOptions[selectedIndex]?.value}`
                : undefined
            }
          />
        ) : (
          <button
            ref={inputRef as RefObject<HTMLButtonElement>}
            className={styles.input}
            role="combobox"
            aria-expanded={isOpen}
            aria-controls="options-listbox"
            aria-activedescendant={
              isOpen
                ? `option-${filteredOptions[selectedIndex]?.value}`
                : undefined
            }
            // tabIndex={0}
            disabled={disabled}
            onMouseDown={toggleDropdown}
            onKeyDown={handleKeyDown}
            onBlur={() => setMenuOpen(false)}
          >
            {selectedItem?.text || ''}
          </button>
        )}
        <span className={styles.toggleButton}>
          <DropdownArrow className={styles.toggleIcon} />
        </span>
      </div>

      {isOpen && (
        <ul
          ref={listboxRef}
          className={classNames(styles.optionsList, {
            [styles.editable]: editable,
          })}
          role="listbox"
          id="options-listbox"
          tabIndex={-1}
          onMouseDown={e => {
            // to prevent stealing focus
            e.preventDefault();
          }}
        >
          {filteredOptions.length === 0 ? (
            <li className={styles.noResults}>No results found</li>
          ) : (
            filteredOptions.map((item, index) => (
              <li
                onMouseEnter={() => setSelectedIndex(index)}
                onMouseLeave={() => setSelectedIndex(-1)}
                key={index}
                id={`option-${item.value}`}
                role="option"
                aria-selected={value === item.value}
                className={`${styles.option} ${
                  selectedIndex === index ? styles.highlighted : ''
                }`}
                onMouseDown={() => handleSelect(item.value)}
                onMouseUp={() => onMouseUp(item.value)}
              >
                <img
                  src={item.icon}
                  className={classNames(styles.optionIcon, 'icon')}
                />
                {item.text}
              </li>
            ))
          )}
        </ul>
      )}
    </div>
  );
}

// This odd dance is because Combobox is a generic component.
const ComboboxMemo = memo(Combobox) as typeof Combobox;
export {ComboboxMemo as Combobox};
