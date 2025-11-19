import {useQuery} from '@rocicorp/zero/react';
import {useLogin} from './use-login.tsx';
import {useZero} from './use-zero.ts';

export function useCanEdit(ownerUserID: string | undefined): boolean {
  const login = useLogin();
  const z = useZero();
  const currentUserID = login.loginState?.decoded.sub;
  const [isCrew] = useQuery(
    z.query.user(currentUserID || '').where('role', 'crew'),
  );
  return (
    import.meta.env.VITE_PUBLIC_SANDBOX ||
    isCrew ||
    ownerUserID === currentUserID
  );
}
