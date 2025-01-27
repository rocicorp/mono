import {forwardRef, useCallback, useState, type ForwardedRef} from 'react';
import {useLogin} from '../hooks/use-login.tsx';
import {Button, type ButtonProps} from './button.tsx';
import {NotLoggedInModal} from './not-logged-in-modal.tsx';

interface Props extends ButtonProps {
  loginMessage: string;
}

export const ButtonWithLoginCheck = forwardRef(
  (props: Props, ref: ForwardedRef<HTMLButtonElement>) => {
    const login = useLogin();
    const [open, setOpen] = useState(false);
    const {loginMessage, ...buttonProps} = props;
    const onAction = useCallback(() => {
      if (login.loginState === undefined) {
        setOpen(true);
      } else {
        props.onAction?.();
      }
    }, [login.loginState, props]);
    return (
      <>
        <Button {...buttonProps} onAction={onAction} ref={ref} />
        {login.loginState === undefined ? (
          <NotLoggedInModal
            text={loginMessage}
            isOpen={open}
            onDismiss={() => setOpen(false)}
          />
        ) : null}
      </>
    );
  },
);
