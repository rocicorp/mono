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
      <StyledFirebaseAuth uiConfig={uiConfig} firebaseAuth={auth} />
    </div>
  );
}
