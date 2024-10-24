import {useCallback, useState} from 'react';
import {useLogin} from '../hooks/use-login.js';
import {Button, type Props as ButtonProps} from './button.jsx';
import {NotLoggedInModal} from './not-logged-in-modal.js';

interface Props extends ButtonProps {
  loginMessage: string;
}

export function ButtonWithLoginCheck(props: Props) {
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
      <Button {...buttonProps} onAction={onAction} />
      {login.loginState === undefined ? (
        <NotLoggedInModal
          text={loginMessage}
          isOpen={open}
          onDismiss={() => setOpen(false)}
        />
      ) : null}
    </>
  );
}
