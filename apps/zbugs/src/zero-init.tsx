import {ZeroProvider} from '@rocicorp/zero/react';
import {useLogin} from './hooks/use-login.tsx';
import {useMemo, type ReactNode} from 'react';
import {schema, type Schema} from '../shared/schema.ts';
import {type NamedMutator} from '@rocicorp/zero';
import * as mutators from '../shared/mutators.ts';

export function ZeroInit({children}: {children: ReactNode}) {
  const login = useLogin();

  const props = useMemo(() => {
    return {
      schema,
      server: import.meta.env.VITE_PUBLIC_SERVER,
      userID: login.loginState?.decoded?.sub ?? 'anon',
      // TODO: ideally the `mutators` options becomes an async function that looks up the
      // mutator definition in order to support code splitting for large applications.
      mutators: Object.values(mutators).map(v =>
        v(login.loginState?.decoded),
      ) as NamedMutator<Schema>[],
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
