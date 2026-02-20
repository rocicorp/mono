import type {ZeroOptions} from '@rocicorp/zero';
import {ZeroProvider} from '@rocicorp/zero/react';
import {useMemo, type ReactNode} from 'react';
import {mutators} from '../shared/mutators.ts';
import {schema} from '../shared/schema.ts';
import {useLogin} from './hooks/use-login.tsx';

export function ZeroInit({children}: {children: ReactNode}) {
  const login = useLogin();

  const options = useMemo(
    () =>
      ({
        schema,
        cacheURL: import.meta.env.VITE_PUBLIC_SERVER,
        userID: login.loginState?.decoded?.sub ?? 'anon',
        mutators,
        logLevel: 'info',
        // changing the auth token will cause ZeroProvider to call connection.connect
        auth: login.loginState?.encoded,
        mutateURL: `${window.location.origin}/api/mutate`,
        queryURL: `${window.location.origin}/api/query`,
        context: login.loginState?.decoded,
      }) as const satisfies ZeroOptions,
    [login.loginState],
  );

  return <ZeroProvider {...options}>{children}</ZeroProvider>;
}
