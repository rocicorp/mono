import {useQuery} from '@rocicorp/zero/react';
import {useLogin} from './use-login.js';
import {useZero} from './use-zero.js';

export function useCanEdit(ownerUserID: string): boolean {
  const login = useLogin();
  const z = useZero();
  const currentUserID = login.loginState?.decoded.sub;
  const isCrew = useQuery(
    z.query.user
      .where('id', currentUserID || '')
      .where('role', 'crew')
      .one(),
  );
  return (
    import.meta.env.VITE_PUBLIC_SANDBOX ||
    isCrew ||
    ownerUserID === currentUserID
  );
}
