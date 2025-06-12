import {useQuery} from '@rocicorp/zero/react';
import {useLogin} from './use-login.tsx';
import {queries} from '../../shared/schema.ts';

export function useCanEdit(ownerUserID: string | undefined): boolean {
  const login = useLogin();
  const currentUserID = login.loginState?.decoded.sub;
  const [isCrew] = useQuery(
    queries.user
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
