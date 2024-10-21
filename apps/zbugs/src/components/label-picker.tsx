import {useQuery} from '@rocicorp/zero/react';
import classNames from 'classnames';
import {useCallback, useEffect, useRef, useState} from 'react';
import {useClickOutside} from '../hooks/use-click-outside.js';
import {useZero} from '../hooks/use-zero.js';
import {Button} from './button.js';
import style from './label-picker.module.css';

export default function LabelPicker({
  selected,
  onDisassociateLabel,
  onAssociateLabel,
  onCreateNewLabel,
}: {
  selected: Set<string>;
  onDisassociateLabel: (id: string) => void;
  onAssociateLabel: (id: string) => void;
  onCreateNewLabel: (name: string) => void;
}) {
  const [isOpen, setIsOpen] = useState(false);
  const z = useZero();
  const labels = useQuery(z.query.label.orderBy('name', 'asc'));
  const ref = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useClickOutside(
    ref,
    useCallback(() => setIsOpen(false), []),
  );

  useEffect(() => {
    if (isOpen) {
      setTimeout(() => {
        inputRef.current?.focus();
      }, 0);
    }
  }, [isOpen]);

  return (
    <div className={style.root} ref={ref}>
      <Button
        title="Add label"
        className={style.addLabel}
        onAction={() => setIsOpen(!isOpen)}
      >
        + Label
      </Button>
      {isOpen && (
        <LabelPopover
          onAssociateLabel={onAssociateLabel}
          onDisassociateLabel={onDisassociateLabel}
          onCreateNewLabel={onCreateNewLabel}
          labels={labels}
          selected={selected}
          inputRef={inputRef}
        />
      )}
    </div>
  );
}

function LabelPopover({
  labels,
  selected,
  onDisassociateLabel,
  onAssociateLabel,
  onCreateNewLabel,
  inputRef,
}: {
  selected: Set<string>;
  onDisassociateLabel: (id: string) => void;
  onAssociateLabel: (id: string) => void;
  onCreateNewLabel: (name: string) => void;
  labels: readonly {id: string; name: string}[];
  inputRef: React.RefObject<HTMLInputElement>;
}) {
  const [input, setInput] = useState('');
  const filteredLabels = labels.filter(label =>
    label.name.toLowerCase().includes(input.toLowerCase()),
  );

  const handleCreateNewLabel = () => {
    if (
      input &&
      !filteredLabels.find(
        label => label.name.toLowerCase() === input.toLowerCase(),
      )
    ) {
      onCreateNewLabel(input);
      setInput('');
    }
  };

  const selectedLabels: React.ReactNode[] = [];
  const unselectedLabels: React.ReactNode[] = [];

  for (const label of filteredLabels) {
    if (selected.has(label.id)) {
      selectedLabels.push(
        <li
          key={label.id}
          onMouseDown={() => onDisassociateLabel(label.id)}
          className={classNames(style.selected, style.label, 'pill', 'label')}
        >
          {label.name}
        </li>,
      );
    } else {
      unselectedLabels.push(
        <li
          onMouseDown={() => onAssociateLabel(label.id)}
          key={label.id}
          className={classNames(style.label, 'pill', 'label')}
        >
          {label.name}
        </li>,
      );
    }
  }

  return (
    <div className={style.popoverWrapper}>
      <div className={style.popover}>
        <input
          type="text"
          placeholder="Filter or add label..."
          className={style.labelFilter}
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={e => {
            if (e.key === 'Enter') {
              handleCreateNewLabel();
            }
          }}
          ref={inputRef}
          autoFocus
        />

        <ul>
          {selectedLabels}
          {unselectedLabels}

          {/* Option to create a new tag if none match */}
          {input && !filteredLabels.length && (
            <li
              onMouseDown={handleCreateNewLabel}
              className={classNames(style.label, 'pill', style.newLabel)}
            >
              Create "{input}"
            </li>
          )}
        </ul>
      </div>
    </div>
  );
}
