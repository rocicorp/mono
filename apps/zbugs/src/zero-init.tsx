import {ZeroProvider} from '@rocicorp/zero/react';
import {useLogin} from './hooks/use-login.tsx';
import {createMutators} from '../shared/mutators.ts';
import {useMemo, type ReactNode} from 'react';
import {schema} from '../shared/schema.ts';

export function ZeroInit({children}: {children: ReactNode}) {
  const login = useLogin();

  const props = useMemo(() => {
    return {
      schema,
      server: import.meta.env.VITE_PUBLIC_SERVER,
      userID: login.loginState?.decoded?.sub ?? 'anon',
      mutators: createMutators(login.loginState?.decoded),
      logLevel: 'info' as const,
      auth: (error?: 'invalid-token') => {
        if (error === 'invalid-token') {
          login.logout();
          return undefined;
        }
        return login.loginState?.encoded;
      },
    };
  }, [login]);

  return <ZeroProvider {...props}>{children}</ZeroProvider>;
}
