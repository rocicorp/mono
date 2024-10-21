import {useQuery} from '@rocicorp/zero/react';
import classNames from 'classnames';
import {useState} from 'react';
import labelIcon from '../assets/icons/label.svg';
import {useZero} from '../hooks/use-zero.js';
import {Button} from './button.js';
import Selector from './selector.js';
import UserPicker from './user-picker.js';

export type Selection =
  | {creator: string}
  | {assignee: string}
  | {label: string};

type Props = {
  onSelect?: ((selection: Selection) => void) | undefined;
};

export default function Filter({onSelect}: Props) {
  const z = useZero();
  const [isOpen, setIsOpen] = useState(false);

  const labels = useQuery(z.query.label);
  // TODO: Support case-insensitive sorting in ZQL.
  labels.sort((a, b) => a.name.localeCompare(b.name));

  const handleSelect = (selection: Selection) => {
    setIsOpen(!isOpen);
    onSelect?.(selection);
  };

  return (
    <div className="add-filter-container">
      <Button
        className={classNames('add-filter', {active: isOpen})}
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
              <UserPicker onSelect={u => handleSelect({creator: u.login})} />
            </div>
            <div className="filter-modal-item">
              <p className="filter-modal-label">Assignee</p>
              <UserPicker onSelect={u => handleSelect({assignee: u.login})} />
            </div>
            <div className="filter-modal-item">
              <p className="filter-modal-label">Label</p>
              <Selector
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
}
