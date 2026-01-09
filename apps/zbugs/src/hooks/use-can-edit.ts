import {useQuery} from '../../shared/zero-hooks.ts';
import {queries} from '../../shared/queries.ts';
import {useLogin} from './use-login.tsx';

export function useCanEdit(ownerUserID: string | undefined): boolean {
  const login = useLogin();
  const currentUserID = login.loginState?.decoded.sub;
  const [isCrew] = useQuery(currentUserID && queries.crewUser(currentUserID));
  return (
    import.meta.env.VITE_PUBLIC_SANDBOX ||
    isCrew ||
    ownerUserID === currentUserID
  );
}
