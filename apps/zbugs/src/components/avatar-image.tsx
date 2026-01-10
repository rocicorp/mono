import type {ZbugsRow} from '../../shared/zero-hooks.ts';
import {memo, type ImgHTMLAttributes} from 'react';
import {avatarURLWithSize} from '../avatar-url-with-size.ts';

interface AvatarImageProps extends ImgHTMLAttributes<HTMLImageElement> {
  user: ZbugsRow['user'];
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
