import Link from 'next/link';
import styles from './GetStarted.module.css';

export function GetStarted() {
  return (
    <div className={styles.getStarted}>
      <p>Build your first multiplayer app in under a minute:</p>
      <div className={styles.ctaWrap}>
        <Link
          href="https://hello.reflect.net/scaffold"
          className={styles.buttonPrimary}
        >
          Hello, Reflect
        </Link>
      </div>
    </div>
  );
}
