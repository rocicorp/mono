import {
  createContext,
  useCallback,
  useContext,
  useSyncExternalStore,
} from 'react';
import {clearJwt} from '../jwt.js';
import {type LoginState, authRef} from '../zero-setup.js';

export type LoginContext = {
  logout: () => void;
  loginState: LoginState | undefined;
};

const loginContext = createContext<LoginContext | undefined>(undefined);

// eslint-disable-next-line react-refresh/only-export-components
export function useLogin() {
  const state = useContext(loginContext);
  if (state === undefined) {
    throw new Error('useLogin must be used within a LoginProvider');
  }
  return state;
}

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
