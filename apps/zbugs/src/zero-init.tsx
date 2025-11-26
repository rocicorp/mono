import {ZeroProvider} from '@rocicorp/zero/react';
import {type ReactNode} from 'react';
import {mutators} from '../shared/mutators.ts';
import {schema} from '../shared/schema.ts';
import {useLogin} from './hooks/use-login.tsx';

export function ZeroInit({children}: {children: ReactNode}) {
  const login = useLogin();

  return (
    <ZeroProvider
      {...{
        schema,
        cacheURL: import.meta.env.VITE_PUBLIC_SERVER,
        userID: login.loginState?.decoded?.sub ?? 'anon',
        mutators,
        logLevel: 'info' as const,
        // changing the auth token will cause ZeroProvider to call connection.connect
        auth: login.loginState?.encoded,
        mutateURL: `${window.location.origin}/api/mutate`,
        queryURL: `${window.location.origin}/api/query`,
        context: login.loginState?.decoded,
      }}
    >
      {children}
    </ZeroProvider>
  );
}
