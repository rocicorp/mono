import type {CustomMutatorDefs, Schema, ZeroOptions} from '@rocicorp/zero';
import {ZeroProvider} from '@rocicorp/zero/react';
import {useMemo, type ReactNode} from 'react';
import {createMutators} from '../shared/mutators.ts';
import {schema} from '../shared/schema.ts';
import {useLogin} from './hooks/use-login.tsx';

export function ZeroInit({children}: {children: ReactNode}) {
  const login = useLogin();

  const props = useMemo(
    () =>
      ({
        schema,
        server: import.meta.env.VITE_PUBLIC_SERVER,
        userID: login.loginState?.decoded?.sub ?? 'anon',
        mutators: createMutators(login.loginState?.decoded),
        logLevel: 'info' as const,
        // changing the auth token will cause ZeroProvider to call connection.connect
        auth: login.loginState?.encoded,
        mutateURL: `${window.location.origin}/api/mutate`,
        getQueriesURL: `${window.location.origin}/api/get-queries`,
      }) satisfies ZeroOptions<Schema, CustomMutatorDefs>,
    [login],
  );

  return <ZeroProvider {...props}>{children}</ZeroProvider>;
}
