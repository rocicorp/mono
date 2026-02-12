import { ZeroProvider } from '@rocicorp/zero/solid';
import type { JSX } from 'solid-js';
import { useLogin } from './login-provider.tsx';
import { mutators, schema } from './zero.ts';

export function ZeroInit(props: {children: JSX.Element}) {
  const cacheURL = import.meta.env.VITE_PUBLIC_SERVER;
  const login = useLogin();
  const loginState = login.loginState();

  console.log('[ZeroInit] cacheURL:', cacheURL);
  console.log('[ZeroInit] schema tables:', Object.keys(schema.tables));
  console.log('[ZeroInit] loginState:', loginState);

  return (
    <ZeroProvider
      schema={schema}
      mutators={mutators}
      cacheURL={cacheURL}
      userID={loginState?.decoded?.sub ?? 'anon'}
      logLevel="debug"
      mutateURL="http://localhost:5173/api/mutate"
      queryURL="http://localhost:5173/api/query"
      auth={loginState?.encoded}
      context={loginState?.decoded}
    >
      {props.children}
    </ZeroProvider>
  );
}
