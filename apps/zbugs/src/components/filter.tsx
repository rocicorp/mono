import type {SchemaToRow} from '@rocicorp/zero';
import {useQuery} from '@rocicorp/zero/react';
import classNames from 'classnames';
import {useEffect, useState} from 'react';
import avatarIcon from '../assets/icons/avatar-default.svg';
import labelIcon from '../assets/icons/label.svg';
import type {Schema} from '../domain/schema.js';
import {useZero} from '../hooks/use-zero.js';
import Selector from './selector.js';

export type Selection = {creator: string} | {label: string};

type Props = {
  onSelect?: ((selection: Selection) => void) | undefined;
};

export default function Filter({onSelect}: Props) {
  const z = useZero();
  const [isOpen, setIsOpen] = useState(false);

  const labels = useQuery(z.query.label.orderBy('name', 'asc'));
  const creators = useQuery(z.query.user.orderBy('name', 'asc'));

  // TODO: should zql sort do this?
  labels.sort((a, b) => a.name.localeCompare(b.name));
  creators.sort((a, b) => a.name.localeCompare(b.name));

  // Preload the avatar icons so they show up instantly when opening the
  // dropdown.
  const [avatars, setAvatars] = useState<Record<string, string>>({});
  useEffect(() => {
    let canceled = false;
    async function preload() {
      const avatars = await Promise.all(creators.map(c => preloadAvatar(c)));
      if (canceled) {
        return;
      }
      setAvatars(Object.fromEntries(avatars));
    }
    void preload();
    return () => {
      canceled = true;
    };
  }, [creators]);

  const handleSelect = (selection: Selection) => {
    setIsOpen(!isOpen);
    onSelect?.(selection);
  };

  return (
    <div className="add-filter-container">
      <button
        className={classNames('add-filter', {active: isOpen})}
        onMouseDown={() => setIsOpen(!isOpen)}
        style={{
          zIndex: isOpen ? 1 : 0,
        }}
      >
        <span className="plus">+</span> Filter
      </button>

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
              <Selector
                onChange={c => handleSelect({creator: c.login})}
                items={creators.map(c => ({
                  text: c.name,
                  value: c,
                  icon: avatars[c.id],
                }))}
                defaultItem={{
                  text: 'Select',
                  icon: avatarIcon,
                }}
              />
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

// TODO: export a nicer named version of SchemaToRow.
function preloadAvatar(user: SchemaToRow<Schema['tables']['user']>) {
  return new Promise<[string, string]>((res, rej) => {
    fetch(user.avatar)
      .then(response => response.blob())
      .then(blob => {
        const reader = new FileReader();
        reader.onloadend = () => {
          res([user.id, reader.result as string]);
        };
        reader.readAsDataURL(blob);
      })
      .catch(err => {
        rej('Error fetching the image: ' + err);
      });
  });
}
