import {useQuery} from '@rocicorp/zero/react';
import classNames from 'classnames';
import {memo, useMemo, useState} from 'react';
import {days} from '../../../../packages/shared/src/time.ts';
import labelIcon from '../assets/icons/label.svg';
import {useZero} from '../hooks/use-zero.ts';
import {Button} from './button.tsx';
import {Combobox} from './combobox.tsx';
import {UserPicker} from './user-picker.tsx';

export type Selection =
  | {creator: string}
  | {assignee: string}
  | {label: string};

type Props = {
  onSelect?: ((selection: Selection) => void) | undefined;
};

export const Filter = memo(function Filter({onSelect}: Props) {
  const z = useZero();
  const [isOpen, setIsOpen] = useState(false);

  const [unsortedLabels] = useQuery(z.query.label, {ttl: days(1)});
  // TODO: Support case-insensitive sorting in ZQL.
  const labels = useMemo(
    () => unsortedLabels.toSorted((a, b) => a.name.localeCompare(b.name)),
    [unsortedLabels],
  );

  const handleSelect = (selection: Selection) => {
    setIsOpen(!isOpen);
    onSelect?.(selection);
  };

  return (
    <div className="add-filter-container">
      <Button
        className={classNames('add-filter', {active: isOpen})}
        eventName="Add filter toggle"
        onAction={() => setIsOpen(!isOpen)}
        style={{
          zIndex: isOpen ? 1 : 0,
        }}
      >
        <span className="plus">+</span> Filter
      </Button>

      {isOpen && (
        <>
          <div
            style={{
              position: 'fixed',
              top: '0',
              left: '0',
              width: '100%',
              height: '100%',
              backgroundColor: 'rgba(0, 0, 0, 0)',
            }}
            onMouseDown={() => setIsOpen(false)}
          ></div>
          <div className="add-filter-modal">
            <div className="filter-modal-item">
              <p className="filter-modal-label">Creator</p>
              <UserPicker
                onSelect={u => u && handleSelect({creator: u.login})}
                placeholder="Select"
                allowNone={false}
                filter="creators"
              />
            </div>
            <div className="filter-modal-item">
              <p className="filter-modal-label">Assignee</p>
              <UserPicker
                onSelect={u => u && handleSelect({assignee: u.login})}
                placeholder="Select"
                allowNone={false}
                filter="crew"
              />
            </div>
            <div className="filter-modal-item">
              <p className="filter-modal-label">Label</p>
              <Combobox
                onChange={l => handleSelect({label: l.name})}
                items={labels.map(c => ({
                  text: c.name,
                  value: c,
                  icon: labelIcon,
                }))}
                defaultItem={{
                  text: 'Select',
                  icon: labelIcon,
                }}
              />
            </div>
          </div>
        </>
      )}
    </div>
  );
});
