import {useCanEdit} from '../hooks/use-can-edit.js';

interface Props {
  children: React.ReactNode;
  ownerID: string;
}

export function CanEdit({children, ownerID}: Props) {
  const canEdit = useCanEdit(ownerID);
  if (canEdit) {
    return children;
  }
  return null;
}
