import {type ReactNode} from 'react';
import {mutators} from '../shared/mutators.ts';
import {schema} from '../shared/schema.ts';
import {useLogin} from './hooks/use-login.tsx';
import {ZeroProvider} from '@rocicorp/zero/react';

export function ZeroInit({children}: {children: ReactNode}) {
  const login = useLogin();

  return (
    <ZeroProvider
      schema={schema}
      server={import.meta.env.VITE_PUBLIC_SERVER}
      userID={login.loginState?.decoded?.sub ?? 'anon'}
      mutators={mutators}
      logLevel="info"
      auth={login.loginState?.encoded}
      mutateURL={`${window.location.origin}/api/mutate`}
      getQueriesURL={`${window.location.origin}/api/get-queries`}
      context={login.loginState?.decoded}
    >
      {children}
    </ZeroProvider>
  );
}
