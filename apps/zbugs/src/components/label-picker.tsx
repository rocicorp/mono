import {useQuery} from '@rocicorp/zero/react';
import classNames from 'classnames';
import {useCallback, useRef, useState} from 'react';
import {queries} from '../../shared/queries.ts';
import {useClickOutside} from '../hooks/use-click-outside.ts';
import {Button} from './button.tsx';
import style from './label-picker.module.css';

const focusInput = (input: HTMLInputElement | null) => {
  if (input) {
    input.focus();
  }
};

export function LabelPicker({
  projectName,
  selected,
  onDisassociateLabel,
  onAssociateLabel,
  onCreateNewLabel,
}: {
  projectName: string;
  selected: Set<string>;
  onDisassociateLabel: (id: string) => void;
  onAssociateLabel: (id: string) => void;
  onCreateNewLabel: (name: string) => void;
}) {
  const [isOpen, setIsOpen] = useState(false);
  const [labels] = useQuery(
    queries.labelsOrderByName({projectName, orderBy: 'asc'}),
  );
  const ref = useRef<HTMLDivElement>(null);

  useClickOutside(
    ref,
    useCallback(() => setIsOpen(false), []),
  );

  return (
    <div className={style.root} ref={ref}>
      <Button
        title="Add label"
        eventName="Add issue label toggle"
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
          inputRef={focusInput}
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
  inputRef: React.Ref<HTMLInputElement>;
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
