import {memo} from 'react';
import {useCanEdit} from '../hooks/use-can-edit.js';

interface Props {
  children: React.ReactNode;
  ownerID: string;
}

export const CanEdit = memo(({children, ownerID}: Props) => {
  const canEdit = useCanEdit(ownerID);
  if (canEdit) {
    return children;
  }
  return null;
});
