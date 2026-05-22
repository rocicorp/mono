import {useMemo, type ReactNode} from 'react';
import {mutators} from '../shared/mutators.ts';
import {ZeroProvider} from '../shared/zero-hooks.ts';
import {useLogin} from './hooks/use-login.tsx';

export function ZeroInit({children}: {children: ReactNode}) {
  const login = useLogin();

  const options = useMemo(
    () =>
      ({
        cacheURL: import.meta.env.VITE_PUBLIC_SERVER,
        userID: login.loginState?.decoded?.sub,
        mutators,
        logLevel: 'info',
        // changing the auth token will cause ZeroProvider to call connection.connect
        auth: login.loginState?.encoded,
        mutateURL: `${window.location.origin}/api/mutate`,
        queryURL: `${window.location.origin}/api/query`,
        context: login.loginState?.decoded,
      }) as const,
    [login.loginState],
  );

  return <ZeroProvider {...options}>{children}</ZeroProvider>;
}
