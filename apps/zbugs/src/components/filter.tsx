import {useQuery} from '@rocicorp/zero/react';
import classNames from 'classnames';
import {memo, useEffect, useMemo, useState} from 'react';
import {toSorted} from '../../../../packages/shared/src/to-sorted.ts';
import {queries} from '../../shared/queries.ts';
import labelIcon from '../assets/icons/label.svg';
import {Button} from './button.tsx';
import {Combobox} from './combobox.tsx';
import {UserPicker} from './user-picker.tsx';

export type Selection =
  | {creator: string}
  | {assignee: string}
  | {label: string};

type Props = {
  projectName: string;
  onSelect?: ((selection: Selection) => void) | undefined;
};

export const Filter = memo(function Filter({projectName, onSelect}: Props) {
  const [isOpen, setIsOpen] = useState(false);

  const [unsortedLabels] = useQuery(queries.labels({projectName}));
  // TODO: Support case-insensitive sorting in ZQL.
  const labels = useMemo(
    () => toSorted(unsortedLabels, (a, b) => a.name.localeCompare(b.name)),
    [unsortedLabels],
  );

  const handleSelect = (selection: Selection) => {
    setIsOpen(!isOpen);
    onSelect?.(selection);
  };

  // Prevent page scroll when modal is open on mobile
  // This fixes the issue where the keyboard causes content to scroll under the header
  useEffect(() => {
    if (!isOpen) return;

    const scrollY = window.scrollY;
    const handleScroll = () => {
      window.scrollTo(0, scrollY);
    };

    // Lock scroll position
    window.addEventListener('scroll', handleScroll, {passive: false});

    return () => {
      window.removeEventListener('scroll', handleScroll);
    };
  }, [isOpen]);

  return (
    <div className="add-filter-container">
      <Button
        enabledOffline
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
                enabledOffline
                projectName={projectName}
                onSelect={u => u && handleSelect({creator: u.login})}
                placeholder="Select"
                allowNone={false}
                filter="creators"
              />
            </div>
            <div className="filter-modal-item">
              <p className="filter-modal-label">Assignee</p>
              <UserPicker
                enabledOffline
                projectName={projectName}
                onSelect={u => u && handleSelect({assignee: u.login})}
                placeholder="Select"
                allowNone={false}
                filter="assignees"
              />
            </div>
            <div className="filter-modal-item">
              <p className="filter-modal-label">Label</p>
              <Combobox
                enabledOffline
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
