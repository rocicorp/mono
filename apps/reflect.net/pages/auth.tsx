import {StyledFirebaseAuth} from '@/components/Firebase/StyledFirebaseAuth';
import {auth, uiConfig} from '@/firebase-config/firebase-auth-ui-config';
import styles from '@/styles/Auth.module.css';
import Image from 'next/image';
import logoAnimated from '@/public/auth/reflect-animated.svg';

export default function Auth() {
  return (
    <div className={styles.authContainer}>
      <Image
        src={logoAnimated}
        loading="lazy"
        alt="Reflect"
        className={styles.logoAnimated}
      />
      <h3>Sign in to Reflect:</h3>
      <div className={styles.signinOptions}>
        <StyledFirebaseAuth uiConfig={uiConfig} firebaseAuth={auth} />
      </div>
    </div>
  );
}
