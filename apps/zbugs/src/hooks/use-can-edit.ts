import {useQuery} from '@rocicorp/zero/react';
import {useLogin} from './use-login.tsx';
import {queries} from '../../shared/queries.ts';

export function useCanEdit(ownerUserID: string | undefined): boolean {
  const login = useLogin();
  const currentUserID = login.loginState?.decoded.sub;
  const [isCrew] = useQuery(
    queries.user(currentUserID || '').where('role', 'crew'),
  );
  // eslint-disable-next-line @typescript-eslint/no-unsafe-return
  return (
    import.meta.env.VITE_PUBLIC_SANDBOX ||
    isCrew ||
    ownerUserID === currentUserID
  );
}
