import Link from 'next/link';
import {BetaSignup} from './BetaSignup';
import styles from './GetStarted.module.css';

export function GetStarted() {
  return (
    <div className={styles.getStarted}>
      <p>
        Reflect is being built by <a href="https://rocicorp.dev/">the team</a>{' '}
        behind <a href="https://replicache.dev/">Replicache</a>, and we&apos;re
        working toward a public beta later this year.
      </p>
      <p>
        Want to be first to try it? Let us know what you&apos;re building and
        how you&apos;d use Reflect. We&apos;ll get you access as soon as we can.
      </p>
      <BetaSignup />
      <p className={styles.getStartedNote}>
        You can also contact us by{' '}
        <Link href="mailto:hi@reflect.net">email</Link>, on{' '}
        <Link href="https://twitter.com/rocicorp">Twitter</Link>, or on{' '}
        <Link href="https://discord.replicache.dev/">Discord</Link>.
      </p>
    </div>
  );
}
