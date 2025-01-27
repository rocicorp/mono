import {useCallback, useSyncExternalStore} from 'react';
import {loginContext} from '../hooks/use-login.tsx';
import {clearJwt} from '../jwt.ts';
import {authRef} from '../zero-setup.ts';

export function LoginProvider({children}: {children: React.ReactNode}) {
  const loginState = useSyncExternalStore(
    authRef.onChange,
    useCallback(() => authRef.value, []),
  );

  return (
    <loginContext.Provider
      value={{
        logout: () => {
          clearJwt();
          authRef.value = undefined;
        },
        loginState,
      }}
    >
      {children}
    </loginContext.Provider>
  );
}
