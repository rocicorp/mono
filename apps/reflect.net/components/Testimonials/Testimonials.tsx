import guillermo from '@/public/quote-images/guillermo.jpeg';
import noam from '@/public/quote-images/noam.jpg';
import Image from 'next/image';
import styles from './Testimonials.module.css';

export function Testimonials() {
  return (
    <div className={styles.testimonialsGrid}>
      <div className={styles.testimonialBlockFull}>
        <div className={styles.testimonialHeader}>
          <object
            data="/brands/vercel.svg"
            height="48"
            className={styles.brandLogo}
            aria-label="Vercel logo"
          />
          <a href="https://vercel.com" className={styles.testimonialURL}>
            vercel.com
          </a>
        </div>
        <div className={styles.testimonialQuote}>
          &ldquo;The realtime Next.js conf experience is powered by Reflect …
          proven today to the tune of 190,000 messages per second{' '}
          <span style={{fontStyle: 'normal'}}>🤯</span>.&rdquo;
        </div>
        <div className={styles.testimonialAuthorBlock}>
          <Image
            src={guillermo}
            alt="Guillermo Rauch"
            className={styles.authorImage}
            width={42}
            height={42}
          />
          <div className={styles.authorInfo}>
            <div className={styles.authorName}>Guillermo Rauch</div>
            <div className={styles.authorTitle}>CEO</div>
          </div>
        </div>
      </div>

      <div className={styles.testimonialBlockFull}>
        <div className={styles.testimonialHeader}>
          <object
            data="/brands/monday.svg"
            height="48"
            className={styles.brandLogo}
            aria-label="Monday logo"
          />
          <a href="https://monday.com" className={styles.testimonialURL}>
            monday.com
          </a>
        </div>
        <div className={styles.testimonialQuote}>
          &ldquo;Reflect saved us years of engineering work on a collaborative
          diagraming tool. The developer experience feels like saving state
          locally, but it automagically syncs and resolves conflicts.&rdquo;
        </div>
        <div className={styles.testimonialAuthorBlock}>
          <Image
            src={noam}
            alt="Noam Ackerman"
            className={styles.authorImage}
            width={42}
            height={42}
          />
          <div className={styles.authorInfo}>
            <div className={styles.authorName}>Noam Ackerman</div>
            <div className={styles.authorTitle}>Head of Canvas by Monday</div>
          </div>
        </div>
      </div>
    </div>
  );
}
