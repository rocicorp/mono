import Link from 'next/link';
import styles from './GetStarted.module.css';
import {event} from 'nextjs-google-analytics';

export function GetStarted() {
  const trackClick = () => {
    event('Hello Reflect Clicked', {
      action: 'click',
      category: 'CTA Buttons',
      label: 'Hello Reflect Link',
    });
  };

  return (
    <div className={styles.getStarted}>
      <p>Build your first multiplayer app in under a minute:</p>
      <div className={styles.ctaWrap}>
        <Link
          href="https://hello.reflect.net/start/scaffold"
          className={styles.buttonPrimary}
          onClick={trackClick}
        >
          Hello, Reflect
        </Link>
      </div>
    </div>
  );
}
