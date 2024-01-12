import styles from '@/styles/Auth.module.css';
import Image from 'next/image';
import logoAnimated from '@/public/auth/reflect-animated.svg';

export default function ReflectAuthWelcome() {
  return (
    <div className={styles.authContainer}>
      <Image
        src={logoAnimated}
        loading="lazy"
        alt="Reflect"
        className={styles.logoAnimated}
      />
      <h3>Success</h3>
      <div className={styles.authSuccess}>
        <span>Reflect OAuth consent granted.</span>
      </div>
      <p className={styles.returnToCLI}>
        Please return to the CLI to continue.
      </p>
    </div>
  );
}
