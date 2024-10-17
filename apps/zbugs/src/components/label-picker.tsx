import {useCallback, useRef, useState} from 'react';
import Plus from '../assets/icons/plus.svg?react';
import style from './label-picker.module.css';
import {useClickOutside} from '../hooks/use-click-outside.js';
import {useQuery} from '@rocicorp/zero/react';
import {useZero} from '../hooks/use-zero.js';
import classNames from 'classnames';

export default function LabelPicker({
  selected,
  onDisassociateLabel,
  onAssociateLabel,
  onCreateNewLabel, // Add this prop to handle new label creation
}: {
  selected: Set<string>;
  onDisassociateLabel: (id: string) => void;
  onAssociateLabel: (id: string) => void;
  onCreateNewLabel: (name: string) => void; // Callback for creating new labels
}) {
  const [isOpen, setIsOpen] = useState(false);
  const z = useZero();
  const labels = useQuery(z.query.label.orderBy('name', 'asc'));
  const ref = useRef<HTMLDivElement>(null);

  useClickOutside(
    ref,
    useCallback(() => setIsOpen(false), []),
  );

  return (
    <div className={style.root} ref={ref}>
      <button title="Add label" onMouseDown={() => setIsOpen(!isOpen)}>
        <Plus
          style={{
            width: '1em',
            height: '1em',
            display: 'inline',
          }}
        />
      </button>
      {isOpen ? (
        <LabelPopover
          onAssociateLabel={onAssociateLabel}
          onDisassociateLabel={onDisassociateLabel}
          onCreateNewLabel={onCreateNewLabel} // Pass the new callback
          labels={labels}
          selected={selected}
        />
      ) : null}
    </div>
  );
}

function LabelPopover({
  labels,
  selected,
  onDisassociateLabel,
  onAssociateLabel,
  onCreateNewLabel, // Handle new label creation here
}: {
  selected: Set<string>;
  onDisassociateLabel: (id: string) => void;
  onAssociateLabel: (id: string) => void;
  onCreateNewLabel: (name: string) => void;
  labels: readonly {id: string; name: string}[];
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
      onCreateNewLabel(input); // Call the function to create a new label
      setInput(''); // Clear the input field after creating
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
    <div className={style.popover}>
      {/* Input field for filtering and creating new tags */}
      <input
        type="text"
        placeholder="Filter or add label..."
        className={style.labelFilter}
        value={input}
        onChange={e => setInput(e.target.value)}
        onKeyDown={e => {
          if (e.key === 'Enter') {
            handleCreateNewLabel(); // Create new label on Enter
          }
        }}
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
  );
}
