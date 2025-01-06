import {useCallback} from 'react';
import {useSyncExternalStore} from 'use-sync-external-store/shim';
import {loginContext} from '../hooks/use-login.js';
import {clearJwt} from '../jwt.js';
import {authRef} from '../zero-setup.js';

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
