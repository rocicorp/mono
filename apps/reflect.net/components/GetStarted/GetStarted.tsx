import Link from 'next/link';
import Image from 'next/image';
import styles from './GetStarted.module.css';
import stopwatchGif from '../../public/img/scaffolding-stopwatch-quickedit.gif'

export function GetStarted() {
  return (
    <div className={styles.getStarted}>
      <p>
        Reflect is officially in public beta. Give it a try now and build your first multiplayer app in under a minute:
      </p>
      <div className={styles.imageContainer}>
        <Link
          href='https://hello.reflect.net/scaffold'>
            <Image
              src={stopwatchGif}
              alt={`Run Reflect in 41 seconds`}
              className={styles.innerImage}
          />
        </Link>
        
      </div>
      <div className={styles.ctaWrap}>
        <Link
          href='https://hello.reflect.net/scaffold'
          className={styles.buttonPrimary}>
            Hello, Reflect
        </Link>
      </div>
      <p>
        Questions?
      </p>
      <p className={styles.getStartedNote}>
        You can contact us by{' '}
        <Link href="mailto:hi@reflect.net">email</Link>, on{' '}
        <Link href="https://twitter.com/rocicorp">Twitter</Link>, or on{' '}
        <Link href="https://discord.replicache.dev/">Discord</Link>.
      </p>
    </div>
  );
}
