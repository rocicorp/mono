// components/Benefits/Benefits.tsx

import Image from 'next/image';
import styles from './Benefits.module.css';

// static images
import realtimeCollab from '@/public/benefits/realtimeCollab.svg';
import persistence from '@/public/benefits/persistence.svg';
import conflictResolution from '@/public/benefits/conflict-resolution.svg';
import offline from '@/public/benefits/offline.svg';
{/* import itworks from '@/public/benefits/itworks-white.svg'; */}
import textedit from '@/public/benefits/text-editing.svg';
import onprem from '@/public/benefits/onprem.svg';

const Benefits = () => (
  <div className={styles.benefitsGrid}>
    {/* 120 FPS */}
    <div className={styles.benefitBlock}>
      <div className={styles.benefitIconContainer}>
        <Image
          src={realtimeCollab}
          loading="lazy"
          alt="120 FPS cursor"
          className={styles.benefitIcon}
        />
      </div>
      <div className={styles.benefitInfoContainer}>
        <h3 className={styles.benefitTitle}>Absurdly Smooth Motion</h3>
        <p className={styles.benefitDescription}>
          Throw away your interpolation code. Reflect syncs changes at 120 FPS
          (hardware permitting). Built-in batching and buffering provide buttery
          smooth, precision playback automatically &#8212; across town or across
          the globe.
        </p>
      </div>
    </div>

    {/* Servers */}
    <div className={styles.benefitBlock}>
      <div className={styles.benefitIconContainer}>
        <Image
          src={conflictResolution}
          loading="lazy"
          alt="Servers: Pretty Great"
          className={styles.benefitIcon}
        />
      </div>
      <div className={styles.benefitInfoContainer}>
        <h3 className={styles.benefitTitle}>
          Transactional Conflict Resolution
        </h3>
        <p className={styles.benefitDescription}>
          CRDTs converge, but to what?? Validation and custom business logic
          aren&apos;t possible.
        </p>
        <p className={styles.benefitDescription}>
          Reflect uses a more powerful technique known as{' '}
          <a href="https://en.wikipedia.org/wiki/Client-side_prediction">
            Server Reconciliation
          </a>
          . <strong>Your mutation code runs server-side</strong> and is
          authoritative. Clients are guaranteed to converge with server changes.
        </p>
        <p className={styles.benefitDescription}>
          Mutators can enforce arbitrary business logic, fine-grained
          authorization, server-side integrations, and more.
        </p>
      </div>
    </div>

    {/* First-Class Text */}
    <div className={styles.benefitBlock}>
      <div className={styles.benefitIconContainer}>
        <Image
          src={textedit}
          loading="lazy"
          alt="Persistence"
          className={styles.benefitIcon}
        />
      </div>
      <div className={styles.benefitInfoContainer}>
        <h3 className={styles.benefitTitle}>First-Class Text</h3>
        <p className={styles.benefitDescription}>
          Your may have heard that multiplayer text is hard. No worries. Reflect
          has built-in, high-quality text support via the industry standard yjs
          library.
        </p>
      </div>
    </div>

    {/* Automatic Persistence */}
    <div className={styles.benefitBlock}>
      <div className={styles.benefitIconContainer}>
        <Image
          src={persistence}
          loading="lazy"
          alt="Persistence"
          className={styles.benefitIcon}
        />
      </div>
      <div className={styles.benefitInfoContainer}>
        <h3 className={styles.benefitTitle}>Automatic Persistence</h3>
        <p className={styles.benefitDescription}>
          There&apos;s no separate, slower persistence API to juggle. Write
          changes as they happen (yes, every mouse movement) and they are stored
          continuously and automatically, up to 50MB/room.
        </p>
      </div>
    </div>

    {/* Optional Offline */}
    <div className={styles.benefitBlock}>
      <div className={styles.benefitIconContainer}>
        <Image
          src={offline}
          loading="lazy"
          alt="Offline"
          className={styles.benefitIcon}
        />
      </div>
      <div className={styles.benefitInfoContainer}>
        <h3 className={styles.benefitTitle}>Local-First</h3>
        <p className={styles.benefitDescription}>
          Set <code className="inline">clientPersistence: true</code>
          &nbsp; and data is also stored on the client, providing instant
          (“local-first”) startup, navigation, and offline support.
        </p>
      </div>
    </div>

    {/* On-Prem */}
    <div className={styles.benefitBlock}>
      <div className={styles.benefitIconContainer}>
        <Image
          src={onprem}
          loading="lazy"
          alt="On-Prem"
          className={styles.benefitIcon}
        />
      </div>
      <div className={styles.benefitInfoContainer}>
        <h3 className={styles.benefitTitle}>On-&ldquo;Prem&rdquo; Available</h3>
        <p className={styles.benefitDescription}>
          Use Reflect as a traditional SaaS, or deploy it to your own Cloudflare
          account.
        </p>
        <p className={styles.benefitDescription}>
          We&apos;ll run, monitor, and update it. You maintain control,
          ownership, and business continuity with a perpetual source license.
        </p>
      </div>
    </div>

    {/*
    <div className={styles.benefitBlockFull}>
      <div className={styles.benefitIconContainer}>
        <Image
          src={itworks}
          loading="lazy"
          alt="Your have better things to do"
          className={styles.benefitIcon}
        />
      </div>
      <div className={styles.benefitInfoContainer}>
        <h3 className={styles.benefitTitle}>
          Because You Have Better Things to Do
        </h3>
        <p className={styles.benefitDescription}>
          Building solid multiplayer infrastructure is a huge project with many
          difficult tradeoffs. Migrations? Monitoring? Persistence? Scaling?
          Blech.
        </p>
        <p className={styles.benefitDescription}>
          {' '}
          Reflect provides a multiplayer foundation that just works, so you can
          forget about sync and focus on your product.
        </p>
      </div>
    </div>
    */}
  </div>
);

export default Benefits;
