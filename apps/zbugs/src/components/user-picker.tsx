import {type TableSchemaToRow} from '@rocicorp/zero';
import {useQuery} from '@rocicorp/zero/react';
import {useEffect, useState} from 'react';
import avatarIcon from '../assets/icons/avatar-default.svg';
import {type Schema} from '../../schema.js';
import {useZero} from '../hooks/use-zero.js';
import Selector from './selector.js';

type Props = {
  onSelect?: ((user: User | undefined) => void) | undefined;
  selected?: {login?: string | undefined} | undefined;
  disabled?: boolean | undefined;
  unselectedLabel?: string | undefined;
};

type User = TableSchemaToRow<Schema['tables']['user']>;

export default function UserPicker({
  onSelect,
  selected,
  disabled,
  unselectedLabel,
}: Props) {
  const z = useZero();

  const users = useQuery(z.query.user);
  // TODO: Support case-insensitive sorting in ZQL.
  users.sort((a, b) => a.name.localeCompare(b.name));

  // Preload the avatar icons so they show up instantly when opening the
  // dropdown.
  const [avatars, setAvatars] = useState<Record<string, string>>({});
  useEffect(() => {
    let canceled = false;
    async function preload() {
      const avatars = await Promise.all(users.map(c => preloadAvatar(c)));
      if (canceled) {
        return;
      }
      setAvatars(Object.fromEntries(avatars));
    }
    void preload();
    return () => {
      canceled = true;
    };
  }, [users]);

  const handleSelect = (user: User | undefined) => {
    onSelect?.(user);
  };

  const selectedUser = selected && users.find(u => u.login === selected.login);

  const defaultItem = {
    text: unselectedLabel ?? 'Select',
    icon: avatarIcon,
    value: undefined,
  };

  return (
    <Selector
      disabled={disabled}
      onChange={c => handleSelect(c)}
      items={[
        defaultItem,
        ...users.map(u => ({
          text: u.login,
          value: u,
          icon: avatars[u.id],
        })),
      ]}
      selectedValue={selectedUser ?? undefined}
    />
  );
}

function preloadAvatar(user: User) {
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
