import styles from '@/styles/Auth.module.css';
import Image from 'next/image';
import logoAnimated from '@/public/auth/reflect-animated.svg';

export default function ReflectAuthError() {
  return (
    <div className={styles.authContainer}>
      <Image
        src={logoAnimated}
        loading="lazy"
        alt="Reflect"
        className={styles.logoAnimated}
      />
      <h3>Sign in unsuccessful</h3>
      <div className={styles.authError}>
        <span>Something went wrong with authentication</span>
      </div>
    </div>
  );
}
