import {ZeroProvider} from '@rocicorp/zero/react';
import {useLogin} from './hooks/use-login.tsx';
import {createMutators, type Mutators} from '../shared/mutators.ts';
import {useMemo, type ReactNode} from 'react';
import {schema, type Schema} from '../shared/schema.ts';
import type {ZeroOptions} from '@rocicorp/zero';

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
    } as const satisfies ZeroOptions<Schema, Mutators>;
  }, [login]);

  return <ZeroProvider {...props}>{children}</ZeroProvider>;
}
