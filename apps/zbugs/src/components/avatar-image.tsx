import {memo, type ImgHTMLAttributes} from 'react';
import type {UserRow} from '../../schema.ts';
import {avatarURLWithSize} from '../avatar-url-with-size.ts';

interface AvatarImageProps extends ImgHTMLAttributes<HTMLImageElement> {
  user: UserRow;
}

export const AvatarImage = memo((props: AvatarImageProps) => {
  const {user, ...rest} = props;
  return (
    <img
      src={avatarURLWithSize(user.avatar)}
      alt={user.name ?? undefined}
      {...rest}
    />
  );
});
