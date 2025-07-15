import {useQuery} from '@rocicorp/zero/react';
import {useLogin} from './use-login.tsx';
import {queries} from '../../shared/queries.ts';

export function useCanEdit(ownerUserID: string | undefined): boolean {
  const login = useLogin();
  const currentUserID = login.loginState?.decoded.sub;
  const [isCrew] = useQuery(
    queries
      .user(login.loginState?.decoded, currentUserID || '')
      .where('role', 'crew'),
  );
  return (
    import.meta.env.VITE_PUBLIC_SANDBOX ||
    isCrew ||
    ownerUserID === currentUserID
  );
}
