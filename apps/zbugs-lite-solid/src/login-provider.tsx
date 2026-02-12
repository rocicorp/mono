import {
  createContext,
  createSignal,
  useContext,
  type Accessor,
  type JSX,
} from 'solid-js';
import {jwtDataSchema, type JWTData} from '../../zbugs/shared/auth.ts';
import {clearJwt, getJwt, getRawJwt} from './jwt.ts';

export type LoginState = {
  encoded: string;
  decoded: JWTData;
};

export type LoginContext = {
  logout: () => void;
  loginState: Accessor<LoginState | undefined>;
};

const LoginContextInstance = createContext<LoginContext>();

export function LoginProvider(props: {children: JSX.Element}) {
  const encoded = getRawJwt();
  const decoded = getJwt();

  const [loginState, setLoginState] = createSignal<LoginState | undefined>(
    encoded && decoded
      ? {
          encoded,
          decoded: jwtDataSchema.parse(decoded),
        }
      : undefined,
  );

  const logout = () => {
    clearJwt();
    setLoginState(undefined);
  };

  return (
    <LoginContextInstance.Provider value={{logout, loginState}}>
      {props.children}
    </LoginContextInstance.Provider>
  );
}

export function useLogin(): LoginContext {
  const context = useContext(LoginContextInstance);
  if (!context) {
    throw new Error('useLogin must be used within a LoginProvider');
  }
  return context;
}
