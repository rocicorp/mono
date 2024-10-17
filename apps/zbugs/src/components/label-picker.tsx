import {useCallback, useRef, useState} from 'react';
import Plus from '../assets/icons/plus.svg?react';
import style from './label-picker.module.css';
import {useClickOutside} from '../hooks/use-click-outside.js';
import {useQuery} from '@rocicorp/zero/react';
import {useZero} from '../hooks/use-zero.js';
import classNames from 'classnames';

/**
 *
 */
export default function LabelPicker({
  selected,
  onDisassociateLabel,
  onAssociateLabel,
}: {
  selected: Set<string>;
  onDisassociateLabel: (id: string) => void;
  onAssociateLabel: (id: string) => void;
}) {
  const [isOpen, setIsOpen] = useState(false);
  const z = useZero();
  const [labels] = useQuery(z.query.label.orderBy('name', 'asc'));
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
}: {
  selected: Set<string>;
  onDisassociateLabel: (id: string) => void;
  onAssociateLabel: (id: string) => void;
  labels: readonly {id: string; name: string}[];
}) {
  const selectedLabels: React.ReactNode[] = [];
  const unselectedLabels: React.ReactNode[] = [];
  for (const label of labels) {
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
    <ul className={style.popover}>
      {selectedLabels}
      {unselectedLabels}
    </ul>
  );
}
