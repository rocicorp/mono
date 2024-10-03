import {createContext, useContext, useState} from 'react';
import {clearJwt, getJwt, getRawJwt} from '../jwt.js';

export type LoginContext = {
  logout: () => void;
  loginState: LoginState | undefined;
};

export type LoginState = {
  token: string;
  userID: string;
  login: string;
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
  const jwt = getJwt();
  const encodedJwt = getRawJwt();
  const [loginState, setLoginState] = useState<LoginState | undefined>(
    jwt && encodedJwt && jwt.sub
      ? {
          token: encodedJwt,
          userID: jwt.sub,
          login: jwt.name as string,
        }
      : undefined,
  );

  return (
    <loginContext.Provider
      value={{
        logout: () => {
          clearJwt();
          setLoginState(undefined);
        },
        loginState,
      }}
    >
      {children}
    </loginContext.Provider>
  );
}
