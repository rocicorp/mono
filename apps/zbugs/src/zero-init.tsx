import {ZeroProvider} from '@rocicorp/zero/react';
import {type ReactNode} from 'react';
import {mutators} from '../shared/mutators.ts';
import {queries} from '../shared/queries.ts';
import {schema} from '../shared/schema.ts';
import {useLogin} from './hooks/use-login.tsx';

export function ZeroInit({children}: {children: ReactNode}) {
  const login = useLogin();

  return (
    <ZeroProvider
      {...{
        schema,
        mutators,
        queries,

        auth: login.loginState?.encoded,
        context: login.loginState?.decoded,
        userID: login.loginState?.decoded?.sub ?? 'anon',

        server: import.meta.env.VITE_PUBLIC_SERVER,
        mutateURL: `${window.location.origin}/api/mutate`,
        getQueriesURL: `${window.location.origin}/api/get-queries`,
      }}
    >
      {children}
    </ZeroProvider>
  );
}
