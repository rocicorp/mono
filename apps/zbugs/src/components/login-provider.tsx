import {loginContext} from '../hooks/use-login.tsx';
import {clearJwt, getJwt, getRawJwt} from '../jwt.ts';
import {useState} from 'react';
import {jwtDataSchema, type JWTData} from '../../shared/auth.ts';

export type LoginState = {
  encoded: string;
  decoded: JWTData;
};

const encoded = getRawJwt();
const decoded = getJwt();

export function LoginProvider({children}: {children: React.ReactNode}) {
  const [loginState, setLoginState] = useState<LoginState | undefined>(
    encoded && decoded
      ? {
          encoded,
          decoded: decoded && jwtDataSchema.parse(decoded),
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
