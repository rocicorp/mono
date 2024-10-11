import {useEffect, useState} from 'react';
import Selector from './selector.js';
import {useZero} from '../hooks/use-zero.js';
import {useQuery} from '@rocicorp/zero/react';
import {type SchemaToRow} from '@rocicorp/zero';
import {type Schema} from '../domain/schema.js';
import avatarIcon from '../assets/icons/avatar-default.svg';

type Props = {
  onSelect?: ((user: User) => void) | undefined;
  selected?: {login?: string | undefined} | undefined;
};

type User = SchemaToRow<Schema['tables']['user']>;

export default function UserPicker({onSelect, selected}: Props) {
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

  const handleSelect = (user: User) => {
    onSelect?.(user);
  };

  const selectedUser = selected && users.find(u => u.login === selected.login);

  return (
    <Selector
      onChange={c => handleSelect(c)}
      items={users.map(u => ({
        text: u.login,
        value: u,
        icon: avatars[u.id],
      }))}
      defaultItem={
        selectedUser
          ? {text: selectedUser.login, icon: avatars[selectedUser.id]}
          : {
              text: 'Select',
              icon: avatarIcon,
            }
      }
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
